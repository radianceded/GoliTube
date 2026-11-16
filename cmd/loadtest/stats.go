package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/radianceded/GoliTube/internal/metrics"
)

const (
	messageIDPrefix = "LT"
	messageIDLength = 18 // LT + 16 位十六进制序号
)

type outbound struct {
	seq  uint64
	data []byte
}

// parseMessageID 从广播内容开头读取压测序号。服务端不改写 content，
// 只按 UTF-8 去掉首尾空白，所以发送时把 ID 放在最前面。
func parseMessageID(content string) (uint64, bool) {
	if len(content) < messageIDLength || content[0] != 'L' || content[1] != 'T' {
		return 0, false
	}
	seq, err := strconv.ParseUint(content[2:messageIDLength], 16, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// latencyRing 保存每条消息的发送时间和房间，按全局序号取模覆盖。
// 覆盖周期为 1<<20 条消息；在途消息的延迟远小于这个窗口。
type latencyRing struct {
	mask   uint64
	seqs   []atomic.Uint64
	stamps []atomic.Int64
	rooms  []atomic.Int32
}

func newLatencyRing(bits uint) *latencyRing {
	size := 1 << bits
	return &latencyRing{
		mask:   uint64(size - 1),
		seqs:   make([]atomic.Uint64, size),
		stamps: make([]atomic.Int64, size),
		rooms:  make([]atomic.Int32, size),
	}
}

func (r *latencyRing) Mark(seq uint64, room int) {
	index := seq & r.mask
	r.seqs[index].Store(seq)
	r.rooms[index].Store(int32(room))
	r.stamps[index].Store(0)
}

func (r *latencyRing) Stamp(seq uint64) {
	index := seq & r.mask
	if r.seqs[index].Load() == seq {
		r.stamps[index].Store(time.Now().UnixNano())
	}
}

func (r *latencyRing) Lookup(seq uint64) (int, time.Time, bool) {
	index := seq & r.mask
	if r.seqs[index].Load() != seq {
		return 0, time.Time{}, false
	}
	stamp := r.stamps[index].Load()
	if stamp == 0 {
		return 0, time.Time{}, false
	}
	return int(r.rooms[index].Load()), time.Unix(0, stamp), true
}

// seenWindow 检测同一连接在最近 16384 条消息内的重复投递。
// 窗口按序号对齐，允许一定程度的跨发送者乱序；窗口之外的迟到消息按重复计。
const seenWindowBits = 16384

type seenWindow struct {
	mu    sync.Mutex
	base  uint64
	bits  [seenWindowBits / 64]uint64
	ready bool
}

func (w *seenWindow) Reset() {
	w.mu.Lock()
	w.ready = false
	w.base = 0
	w.bits = [seenWindowBits / 64]uint64{}
	w.mu.Unlock()
}

func (w *seenWindow) Mark(seq uint64) (duplicate bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.ready {
		w.base = seq - seq%seenWindowBits
		w.ready = true
	}
	if seq < w.base {
		return true
	}
	if seq >= w.base+seenWindowBits {
		// 跨过整窗：清空旧位图后从新窗口开始。
		w.base = seq - seq%seenWindowBits
		w.bits = [seenWindowBits / 64]uint64{}
	}
	offset := seq - w.base
	word, bit := offset/64, offset%64
	mask := uint64(1) << bit
	if w.bits[word]&mask != 0 {
		return true
	}
	w.bits[word] |= mask
	return false
}

// Stats 汇总压测客户端看到的发送、投递、重复、缺失和错误。
type Stats struct {
	ring *latencyRing
	lat  *metrics.Histogram

	nextSeq    atomic.Uint64
	epochSeq   atomic.Uint64
	sent       atomic.Int64
	expected   atomic.Int64
	received   atomic.Int64
	duplicates atomic.Int64
	invalid    atomic.Int64

	sendDropped     atomic.Int64
	writeErrors     atomic.Int64
	readErrors      atomic.Int64
	disconnects     atomic.Int64
	connectFailures atomic.Int64
	connectedTotal  atomic.Int64
	rateLimited     atomic.Int64
	unauthenticated atomic.Int64
	serverErrors    atomic.Int64

	shuttingDown  atomic.Bool
	sendingPaused atomic.Bool
	phase         atomic.Value
	start         time.Time
	measureStart  atomic.Int64
	alive         atomic.Int64

	rooms []*Room
	conns []*Conn

	disconnectMu      sync.Mutex
	disconnectReasons map[string]int
}

func newStats(rooms []*Room, conns []*Conn) *Stats {
	stats := &Stats{
		ring:              newLatencyRing(20),
		lat:               metrics.NewHistogram(),
		rooms:             rooms,
		conns:             conns,
		start:             time.Now(),
		disconnectReasons: make(map[string]int),
	}
	stats.phase.Store("connect")
	return stats
}

func (s *Stats) SetPhase(phase string) { s.phase.Store(phase) }

func (s *Stats) Phase() string {
	if value, ok := s.phase.Load().(string); ok {
		return value
	}
	return ""
}

func (s *Stats) BeginMeasurement() {
	// 先暂停发送并排空发送队列：否则切换瞬间仍在途的消息会在旧统计里记一次发送，
	// 到达时又因为序号不晚于新起点而被忽略，表现为每条连接恒定少一条。
	s.sendingPaused.Store(true)
	drainDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(drainDeadline) && s.sendersBusy() {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	s.epochSeq.Store(s.nextSeq.Load())
	for _, room := range s.rooms {
		room.Sent.Store(0)
		room.Expected.Store(0)
		room.Received.Store(0)
		room.Duplicates.Store(0)
	}
	for _, conn := range s.conns {
		conn.Seen.Reset()
	}
	s.sent.Store(0)
	s.expected.Store(0)
	s.received.Store(0)
	s.duplicates.Store(0)
	s.invalid.Store(0)
	s.sendDropped.Store(0)
	s.writeErrors.Store(0)
	s.readErrors.Store(0)
	s.rateLimited.Store(0)
	s.unauthenticated.Store(0)
	s.serverErrors.Store(0)
	s.lat.Reset()
	s.measureStart.Store(time.Now().UnixNano())
	s.sendingPaused.Store(false)
}

// sendersBusy 表示仍有发送连接的消息留在本地队列，尚未写进 WebSocket。
func (s *Stats) sendersBusy() bool {
	for _, conn := range s.conns {
		if conn.Sender && len(conn.SendCh) > 0 {
			return true
		}
	}
	return false
}

func (s *Stats) handleMessage(conn *Conn, data []byte) {
	var message struct {
		Type    string `json:"type"`
		Code    string `json:"code"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(data, &message); err != nil {
		s.invalid.Add(1)
		return
	}
	if message.Type == "error" {
		switch message.Code {
		case "rate_limited":
			s.rateLimited.Add(1)
		case "unauthenticated":
			s.unauthenticated.Add(1)
		default:
			s.serverErrors.Add(1)
		}
		return
	}
	if message.Type != "danmaku" {
		s.invalid.Add(1)
		return
	}
	seq, ok := parseMessageID(message.Content)
	if !ok {
		s.invalid.Add(1)
		return
	}
	// 预热阶段发出的消息可能晚于统计起点到达，直接忽略。
	if seq <= s.epochSeq.Load() {
		return
	}
	if conn.Seen.Mark(seq) {
		s.duplicates.Add(1)
		conn.Room.Duplicates.Add(1)
		return
	}
	s.received.Add(1)
	conn.Room.Received.Add(1)
	if roomIndex, sentAt, ok := s.ring.Lookup(seq); ok && roomIndex == conn.Room.Index {
		s.lat.Record(time.Since(sentAt))
	}
}

func (s *Stats) noteDisconnect(err error) {
	s.readErrors.Add(1)
	if s.shuttingDown.Load() {
		return
	}
	s.disconnects.Add(1)
	reason := err.Error()
	if len(reason) > 120 {
		reason = reason[:120]
	}
	s.disconnectMu.Lock()
	if _, exists := s.disconnectReasons[reason]; exists || len(s.disconnectReasons) < 20 {
		s.disconnectReasons[reason]++
	}
	s.disconnectMu.Unlock()
}

func (s *Stats) ConnectFailed(err error) {
	s.connectFailures.Add(1)
	reason := err.Error()
	if len(reason) > 120 {
		reason = reason[:120]
	}
	s.disconnectMu.Lock()
	if _, exists := s.disconnectReasons[reason]; exists || len(s.disconnectReasons) < 20 {
		s.disconnectReasons[reason]++
	}
	s.disconnectMu.Unlock()
}

type Summary struct {
	Time                   string               `json:"time"`
	Phase                  string               `json:"phase"`
	ElapsedSeconds         float64              `json:"elapsed_seconds"`
	Connections            int64                `json:"connections"`
	ConnectionsEstablished int64                `json:"connections_established"`
	ConnectFailures        int64                `json:"connect_failures"`
	Sent                   int64                `json:"sent"`
	Expected               int64                `json:"expected_deliveries"`
	Received               int64                `json:"received"`
	Missing                int64                `json:"missing"`
	MissingRate            float64              `json:"missing_rate"`
	Duplicates             int64                `json:"duplicates"`
	SendPerSecond          float64              `json:"send_per_second"`
	ReceivePerSecond       float64              `json:"receive_per_second"`
	WindowSendPerSecond    float64              `json:"window_send_per_second"`
	WindowReceivePerSecond float64              `json:"window_receive_per_second"`
	Amplification          float64              `json:"delivery_amplification"`
	Latency                metrics.LatencyStats `json:"latency"`
	SendDropped            int64                `json:"send_queue_dropped"`
	WriteErrors            int64                `json:"write_errors"`
	ReadErrors             int64                `json:"read_errors"`
	Disconnects            int64                `json:"disconnects"`
	RateLimited            int64                `json:"rate_limited"`
	Unauthenticated        int64                `json:"unauthenticated"`
	ServerErrors           int64                `json:"server_errors"`
	Invalid                int64                `json:"invalid_messages"`
}

func (s *Stats) elapsed() float64 {
	if started := s.measureStart.Load(); started > 0 {
		return time.Since(time.Unix(0, started)).Seconds()
	}
	return time.Since(s.start).Seconds()
}

func (s *Stats) Snapshot() Summary {
	sent := s.sent.Load()
	expected := s.expected.Load()
	received := s.received.Load()
	missing := expected - received
	if missing < 0 {
		missing = 0
	}
	elapsed := s.elapsed()
	if elapsed <= 0 {
		elapsed = 1
	}
	summary := Summary{
		Time:                   time.Now().Format(time.RFC3339Nano),
		Phase:                  s.Phase(),
		ElapsedSeconds:         elapsed,
		Connections:            s.alive.Load(),
		ConnectionsEstablished: s.connectedTotal.Load(),
		ConnectFailures:        s.connectFailures.Load(),
		Sent:                   sent,
		Expected:               expected,
		Received:               received,
		Missing:                missing,
		Duplicates:             s.duplicates.Load(),
		SendPerSecond:          float64(sent) / elapsed,
		ReceivePerSecond:       float64(received) / elapsed,
		Latency:                s.lat.Snapshot(),
		SendDropped:            s.sendDropped.Load(),
		WriteErrors:            s.writeErrors.Load(),
		ReadErrors:             s.readErrors.Load(),
		Disconnects:            s.disconnects.Load(),
		RateLimited:            s.rateLimited.Load(),
		Unauthenticated:        s.unauthenticated.Load(),
		ServerErrors:           s.serverErrors.Load(),
		Invalid:                s.invalid.Load(),
	}
	if expected > 0 {
		summary.MissingRate = float64(missing) / float64(expected)
	}
	if sent > 0 {
		summary.Amplification = float64(received) / float64(sent)
	}
	return summary
}

type RoomResult struct {
	RoomID      string `json:"room_id"`
	Connections int    `json:"connections"`
	Sent        int64  `json:"sent"`
	Expected    int64  `json:"expected_deliveries"`
	Received    int64  `json:"received"`
	Missing     int64  `json:"missing"`
	Duplicates  int64  `json:"duplicates"`
}

func (s *Stats) RoomResults() []RoomResult {
	results := make([]RoomResult, 0, len(s.rooms))
	for _, room := range s.rooms {
		expected := room.Expected.Load()
		received := room.Received.Load()
		missing := expected - received
		if missing < 0 {
			missing = 0
		}
		results = append(results, RoomResult{
			RoomID:      room.ID,
			Connections: len(room.Conns),
			Sent:        room.Sent.Load(),
			Expected:    expected,
			Received:    received,
			Missing:     missing,
			Duplicates:  room.Duplicates.Load(),
		})
	}
	return results
}

func (s *Stats) DisconnectReasons() map[string]int {
	s.disconnectMu.Lock()
	defer s.disconnectMu.Unlock()
	reasons := make(map[string]int, len(s.disconnectReasons))
	for reason, count := range s.disconnectReasons {
		reasons[reason] = count
	}
	return reasons
}

// ExpectedFromFormula 按“各房间发送 TPS × 房间连接数”独立核算预期投递量，
// 用来核对逐条累计的 expected，而不是只相信同一个计数器。
func (s *Stats) ExpectedFromFormula() int64 {
	var total int64
	for _, room := range s.rooms {
		total += room.Sent.Load() * int64(len(room.Conns))
	}
	return total
}

func (s *Stats) SortRoomsByLoad() {
	sort.SliceStable(s.rooms, func(i, j int) bool {
		return s.rooms[i].Sent.Load() > s.rooms[j].Sent.Load()
	})
}

func (s *Stats) String() string {
	summary := s.Snapshot()
	return fmt.Sprintf("连接=%d 发送=%.1f/s 接收=%.1f/s 缺失=%.4f%% P99=%.1fms",
		summary.Connections, summary.SendPerSecond, summary.ReceivePerSecond, summary.MissingRate*100, summary.Latency.P99MS)
}
