// Package metrics 汇总压测需要观察的进程内指标。
//
// 只使用标准库和原子计数，不引入 Prometheus 客户端；采样由外部脚本按固定间隔拉取
// /debug/metrics，避免指标采集本身随连接数增长而变重。
package metrics

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"runtime"
	"sort"
	"sync/atomic"
	"time"
)

var started = time.Now()

// 进程内计数。连接数在建立成功和移除时更新，重启后归零。
var (
	ConnectionsCurrent atomic.Int64
	ConnectionsTotal   atomic.Int64
	Publishes          atomic.Int64
	PublishErrors      atomic.Int64
	Deliveries         atomic.Int64
	SlowEvictions      atomic.Int64
	RateLimited        atomic.Int64
	ReadErrors         atomic.Int64
	WriteErrors        atomic.Int64
)

// PublishLatency 只统计服务端“收到消息 → Kafka 确认 → Redis 发布返回”的耗时，
// 不含 MySQL 落库。客户端端到端延迟由压测工具单独统计。
var PublishLatency = NewHistogram()

func Now() time.Time { return time.Now() }

// 分桶精度：100 毫秒以内按 100 微秒，1 秒以内按 1 毫秒，10 秒以内按 10 毫秒，
// 60 秒以内按 100 毫秒。P99 落在 200 毫秒附近时误差不超过 1 毫秒。
const (
	fastStepUS   = 100
	fastEdgeUS   = 100_000
	midStepUS    = 1_000
	midEdgeUS    = 1_000_000
	slowStepUS   = 10_000
	slowEdgeUS   = 10_000_000
	slowerStepUS = 100_000
	slowerEdgeUS = 60_000_000
	fastBuckets  = 1_000
	midBuckets   = 900
	slowBuckets  = 900
	slowerBucket = 500
	bucketCount  = fastBuckets + midBuckets + slowBuckets + slowerBucket + 1
)

type Histogram struct {
	buckets [bucketCount]atomic.Int64
	count   atomic.Int64
	sumUS   atomic.Int64
	minUS   atomic.Int64
	maxUS   atomic.Int64
}

func NewHistogram() *Histogram {
	h := &Histogram{}
	h.minUS.Store(-1)
	return h
}

func bucketOf(us int64) int {
	switch {
	case us < fastEdgeUS:
		return int(us / fastStepUS)
	case us < midEdgeUS:
		return fastBuckets + int((us-fastEdgeUS)/midStepUS)
	case us < slowEdgeUS:
		return fastBuckets + midBuckets + int((us-midEdgeUS)/slowStepUS)
	case us < slowerEdgeUS:
		return fastBuckets + midBuckets + slowBuckets + int((us-slowEdgeUS)/slowerStepUS)
	default:
		return bucketCount - 1
	}
}

func bucketUpperUS(index int) int64 {
	switch {
	case index < fastBuckets:
		return int64(index+1) * fastStepUS
	case index < fastBuckets+midBuckets:
		return fastEdgeUS + int64(index-fastBuckets+1)*midStepUS
	case index < fastBuckets+midBuckets+slowBuckets:
		return midEdgeUS + int64(index-fastBuckets-midBuckets+1)*slowStepUS
	case index < bucketCount-1:
		return slowEdgeUS + int64(index-fastBuckets-midBuckets-slowBuckets+1)*slowerStepUS
	default:
		return slowerEdgeUS
	}
}

func (h *Histogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	h.buckets[bucketOf(us)].Add(1)
	h.count.Add(1)
	h.sumUS.Add(us)
	for {
		current := h.maxUS.Load()
		if us <= current || h.maxUS.CompareAndSwap(current, us) {
			break
		}
	}
	for {
		current := h.minUS.Load()
		if (current >= 0 && us >= current) || h.minUS.CompareAndSwap(current, us) {
			break
		}
	}
}

// Reset 清空分桶，用于压测预热结束后从零统计。并发写入时最多丢失少量样本。
func (h *Histogram) Reset() {
	for i := range h.buckets {
		h.buckets[i].Store(0)
	}
	h.count.Store(0)
	h.sumUS.Store(0)
	h.maxUS.Store(0)
	h.minUS.Store(-1)
}

type LatencyStats struct {
	Count int64   `json:"count"`
	MinMS float64 `json:"min_ms"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	MaxMS float64 `json:"max_ms"`
	AvgMS float64 `json:"avg_ms"`
}

func (h *Histogram) Snapshot() LatencyStats {
	count := h.count.Load()
	if count == 0 {
		return LatencyStats{}
	}
	// 先复制计数再求分位，采样期间的新写入最多让总数略小于分桶和，不影响分位位置。
	var cumulative int64
	targets := []int64{count * 50 / 100, count * 95 / 100, count * 99 / 100}
	percentiles := make([]float64, 3)
	targetIndex := 0
	for index := 0; index < bucketCount && targetIndex < len(targets); index++ {
		cumulative += h.buckets[index].Load()
		for targetIndex < len(targets) && cumulative >= targets[targetIndex] {
			percentiles[targetIndex] = float64(bucketUpperUS(index)) / 1000
			targetIndex++
		}
	}
	for ; targetIndex < len(targets); targetIndex++ {
		percentiles[targetIndex] = float64(slowerEdgeUS) / 1000
	}
	min := h.minUS.Load()
	if min < 0 {
		min = 0
	}
	stats := LatencyStats{
		Count: count,
		MinMS: float64(min) / 1000,
		P50MS: percentiles[0],
		P95MS: percentiles[1],
		P99MS: percentiles[2],
		MaxMS: float64(h.maxUS.Load()) / 1000,
	}
	if count > 0 {
		stats.AvgMS = float64(h.sumUS.Load()) / float64(count) / 1000
	}
	return stats
}

// Sources 提供需要遍历连接才能得到的指标；未设置时对应字段省略。
type Sources struct {
	Rooms  func() map[string]int
	Queued func() int
}

type RoomStat struct {
	RoomID      string `json:"room_id"`
	Connections int    `json:"connections"`
}

type Snapshot struct {
	Time           time.Time    `json:"time"`
	UptimeSeconds  float64      `json:"uptime_seconds"`
	Connections    Connections  `json:"connections"`
	Messages       Messages     `json:"messages"`
	PublishLatency LatencyStats `json:"publish_latency"`
	Runtime        RuntimeStats `json:"runtime"`
}

type Connections struct {
	Current int        `json:"current"`
	Total   int64      `json:"total"`
	Rooms   int        `json:"rooms"`
	Top     []RoomStat `json:"top_rooms,omitempty"`
}

type Messages struct {
	Publishes     int64 `json:"publishes"`
	PublishErrors int64 `json:"publish_errors"`
	Deliveries    int64 `json:"deliveries"`
	SlowEvictions int64 `json:"slow_conn_evictions"`
	RateLimited   int64 `json:"rate_limited"`
	ReadErrors    int64 `json:"read_errors"`
	WriteErrors   int64 `json:"write_errors"`
	Queued        int   `json:"queued_messages,omitempty"`
}

type RuntimeStats struct {
	Goroutines     int    `json:"goroutines"`
	HeapAllocBytes uint64 `json:"heap_alloc_bytes"`
	HeapInuseBytes uint64 `json:"heap_inuse_bytes"`
	GCCount        uint32 `json:"gc_count"`
	GCPauseTotalMS uint64 `json:"gc_pause_total_ms"`
}

func SnapshotNow(s Sources) Snapshot {
	connections := Connections{Current: int(ConnectionsCurrent.Load()), Total: ConnectionsTotal.Load()}
	if s.Rooms != nil {
		rooms := s.Rooms()
		connections.Rooms = len(rooms)
		top := make([]RoomStat, 0, len(rooms))
		for room, count := range rooms {
			top = append(top, RoomStat{RoomID: room, Connections: count})
		}
		sort.Slice(top, func(i, j int) bool {
			if top[i].Connections == top[j].Connections {
				return top[i].RoomID < top[j].RoomID
			}
			return top[i].Connections > top[j].Connections
		})
		if len(top) > 20 {
			top = top[:20]
		}
		connections.Top = top
	}
	messages := Messages{
		Publishes:     Publishes.Load(),
		PublishErrors: PublishErrors.Load(),
		Deliveries:    Deliveries.Load(),
		SlowEvictions: SlowEvictions.Load(),
		RateLimited:   RateLimited.Load(),
		ReadErrors:    ReadErrors.Load(),
		WriteErrors:   WriteErrors.Load(),
	}
	if s.Queued != nil {
		messages.Queued = s.Queued()
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return Snapshot{
		Time:           time.Now(),
		UptimeSeconds:  time.Since(started).Seconds(),
		Connections:    connections,
		Messages:       messages,
		PublishLatency: PublishLatency.Snapshot(),
		Runtime: RuntimeStats{
			Goroutines:     runtime.NumGoroutine(),
			HeapAllocBytes: ms.HeapAlloc,
			HeapInuseBytes: ms.HeapInuse,
			GCCount:        ms.NumGC,
			GCPauseTotalMS: ms.PauseTotalNs / uint64(time.Millisecond),
		},
	}
}

// Handler 暴露当前快照；由调用方决定是否注册路由。
func Handler(s Sources) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		json.NewEncoder(w).Encode(SnapshotNow(s))
	})
}

// StartReporter 每 interval 输出一行汇总，用于没有独立采集器的短时压测。
func StartReporter(ctx context.Context, interval time.Duration, s Sources) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		previous := SnapshotNow(s)
		previousAt := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				current := SnapshotNow(s)
				seconds := now.Sub(previousAt).Seconds()
				if seconds <= 0 {
					seconds = 1
				}
				publishRate := float64(current.Messages.Publishes-previous.Messages.Publishes) / seconds
				deliveryRate := float64(current.Messages.Deliveries-previous.Messages.Deliveries) / seconds
				log.Printf("运行汇总 连接数=%d 发布速率=%.1f/s 投递速率=%.1f/s 发布P99=%.1fms "+
					"慢连接淘汰=%d 限流=%d goroutine=%d 堆=%.1fMB GC=%d 队列=%d",
					current.Connections.Current, publishRate, deliveryRate, current.PublishLatency.P99MS,
					current.Messages.SlowEvictions, current.Messages.RateLimited,
					current.Runtime.Goroutines, float64(current.Runtime.HeapAllocBytes)/(1<<20),
					current.Runtime.GCCount, current.Messages.Queued)
				previous = current
				previousAt = now
			}
		}
	}()
}
