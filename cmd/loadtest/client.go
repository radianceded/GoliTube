package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/danmaku"
)

// Room 是一个测试房间及其连接。发送速率按房间配置，由该房间的发送连接分担。
type Room struct {
	Index      int
	ID         string
	Rate       float64
	Conns      []*Conn
	Senders    []*Conn
	Live       atomic.Int64
	Sent       atomic.Int64
	Expected   atomic.Int64
	Received   atomic.Int64
	Duplicates atomic.Int64
}

type Conn struct {
	Index   int
	Room    *Room
	WS      *websocket.Conn
	Account *Account
	Sender  bool
	SendCh  chan outbound
	Seen    seenWindow
	Closed  atomic.Bool
	stats   *Stats
}

// connect 建立一条 WebSocket 连接。发送连接携带注入的会话 Cookie，
// 接收连接保持游客身份，避免压测把无关的账号查询也算进广播路径。
func connect(ctx context.Context, stats *Stats, cfg *Config, room *Room, index int, account *Account) *Conn {
	header := http.Header{}
	if account != nil {
		header.Set("Cookie", "golitube_session="+account.Token)
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
		ReadBufferSize:   4096,
		WriteBufferSize:  4096,
	}
	target := cfg.WSURL + "?room=" + url.QueryEscape(room.ID)
	ws, resp, err := dialer.DialContext(ctx, target, header)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		stats.ConnectFailed(err)
		return nil
	}
	conn := &Conn{Index: index, Room: room, WS: ws, Account: account, Sender: account != nil, stats: stats}
	if conn.Sender {
		conn.SendCh = make(chan outbound, 512)
	}
	room.Conns = append(room.Conns, conn)
	room.Live.Add(1)
	stats.connectedTotal.Add(1)
	stats.alive.Add(1)
	go conn.readLoop(stats)
	if conn.Sender {
		go conn.writeLoop(stats)
	}
	return conn
}

// readLoop 持续读取，保证服务端 Ping 能被及时 Pong；
// 客户端自己也要重置读期限，否则空闲连接会被本地读超时断开。
func (c *Conn) readLoop(stats *Stats) {
	c.WS.SetReadLimit(64 << 10)
	_ = c.WS.SetReadDeadline(time.Now().Add(90 * time.Second))
	c.WS.SetPingHandler(func(appData string) error {
		_ = c.WS.SetReadDeadline(time.Now().Add(90 * time.Second))
		return c.WS.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})
	c.WS.SetPongHandler(func(string) error {
		return c.WS.SetReadDeadline(time.Now().Add(90 * time.Second))
	})
	for {
		_, data, err := c.WS.ReadMessage()
		if err != nil {
			if c.Closed.CompareAndSwap(false, true) {
				c.Room.Live.Add(-1)
				stats.alive.Add(-1)
				stats.noteDisconnect(err)
			}
			return
		}
		stats.handleMessage(c, data)
	}
}

// writeLoop 每条发送连接一个写协程，记录实际写出时间并串行写帧。
func (c *Conn) writeLoop(stats *Stats) {
	for message := range c.SendCh {
		if c.Closed.Load() {
			return
		}
		stats.ring.Stamp(message.seq)
		_ = c.WS.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := c.WS.WriteMessage(websocket.TextMessage, message.data); err != nil {
			stats.writeErrors.Add(1)
			if c.Closed.CompareAndSwap(false, true) {
				c.Room.Live.Add(-1)
				stats.alive.Add(-1)
				stats.noteDisconnect(err)
			}
			return
		}
		if message.seq > stats.epochSeq.Load() {
			stats.sent.Add(1)
			c.Room.Sent.Add(1)
			live := c.Room.Live.Load()
			stats.expected.Add(live)
			c.Room.Expected.Add(live)
		}
	}
}

func buildPayload(seq uint64, size int) []byte {
	id := fmt.Sprintf("%s%016x", messageIDPrefix, seq)
	overhead := len(`{"content":"","position_ms":1234,"color":"#ffffff"}`)
	contentLength := size - overhead
	if contentLength < len(id) {
		contentLength = len(id)
	}
	if contentLength > danmaku.MaxCharacters {
		contentLength = danmaku.MaxCharacters
	}
	content := id + strings.Repeat("x", contentLength-len(id))
	return []byte(fmt.Sprintf(`{"content":%q,"position_ms":1234,"color":"#ffffff"}`, content))
}

// runScheduler 按房间速率产生消息，轮流交给本房间的发送连接。
// 每条发送连接平均不超过 1.5 条/秒，低于单连接令牌桶和单用户窗口的限制。
func (r *Room) runScheduler(ctx context.Context, stats *Stats, cfg *Config) {
	if r.Rate <= 0 || len(r.Senders) == 0 {
		return
	}
	interval := time.Duration(float64(time.Second) / r.Rate)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	next := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if stats.sendingPaused.Load() {
				continue
			}
			seq := stats.nextSeq.Add(1)
			stats.ring.Mark(seq, r.Index)
			payload := buildPayload(seq, cfg.MsgSize)
			queued := false
			for attempt := 0; attempt < len(r.Senders) && !queued; attempt++ {
				sender := r.Senders[next%len(r.Senders)]
				next++
				if sender.Closed.Load() {
					continue
				}
				select {
				case sender.SendCh <- outbound{seq: seq, data: payload}:
					queued = true
				default:
					// 本条发送连接的本地队列已满：说明压测工具自己跟不上，计入丢弃。
				}
			}
			if !queued {
				stats.sendDropped.Add(1)
			}
		}
	}
}

func (c *Conn) Close() {
	if c.Closed.CompareAndSwap(false, true) {
		c.Room.Live.Add(-1)
		c.stats.alive.Add(-1)
	}
	_ = c.WS.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	_ = c.WS.Close()
}
