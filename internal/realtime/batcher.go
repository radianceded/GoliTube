package realtime

import (
	"time"

	"github.com/gorilla/websocket"
)

const (
	DefaultBroadcastBatchSize   = 50
	DefaultBroadcastBatchWindow = 20 * time.Millisecond
	maxBroadcastBatchBytes      = 48 << 10 // 低于压测客户端 64KiB 读限制，留出 JSON 包装余量。
)

type roomBatch struct {
	items [][]byte
	bytes int
	timer *time.Timer
}

// EnableBroadcastBatching 启用房间级下行合包：满 maxItems 条、累计达到 48KiB
// 或首条到达满 window 后刷新一次广播。单条批次仍发原始弹幕帧，多条批次封装为 danmaku_batch。
func (h *Hub) EnableBroadcastBatching(maxItems int, window time.Duration) {
	if maxItems <= 1 || window <= 0 {
		return
	}
	h.batchMu.Lock()
	h.batchMaxItems = maxItems
	h.batchWindow = window
	if h.batches == nil {
		h.batches = make(map[string]*roomBatch)
	}
	h.batchMu.Unlock()
}

// EnqueueBroadcast 在启用合包时将 JSON 文本弹幕按房间缓冲，否则立即广播。
func (h *Hub) EnqueueBroadcast(roomID string, messageType int, message []byte) {
	h.batchMu.Lock()
	enabled := h.batchMaxItems > 1 && h.batchWindow > 0
	if !enabled {
		h.batchMu.Unlock()
		h.Broadcast(roomID, messageType, message)
		return
	}

	// 非 JSON 文本或二进制消息不参与合包：先刷出该房间已有缓冲以保持顺序，再原样广播。
	if messageType != websocket.TextMessage || len(message) == 0 || message[0] != '{' {
		var pending [][]byte
		if b := h.batches[roomID]; b != nil {
			if b.timer != nil {
				b.timer.Stop()
			}
			pending = b.items
			delete(h.batches, roomID)
		}
		h.batchMu.Unlock()
		if len(pending) > 0 {
			h.broadcastBatch(roomID, pending)
		}
		h.Broadcast(roomID, messageType, message)
		return
	}

	var flushNow [][]byte
	b := h.batches[roomID]
	if b != nil && len(b.items) > 0 && b.bytes+len(message) > maxBroadcastBatchBytes {
		if b.timer != nil {
			b.timer.Stop()
		}
		flushNow = b.items
		delete(h.batches, roomID)
		b = nil
	}
	if b == nil {
		b = &roomBatch{items: make([][]byte, 0, min(h.batchMaxItems, 8))}
		h.batches[roomID] = b
		target := b
		b.timer = time.AfterFunc(h.batchWindow, func() {
			h.flushRoomBatch(roomID, target)
		})
	}
	b.items = append(b.items, message)
	b.bytes += len(message)

	var full [][]byte
	if len(b.items) >= h.batchMaxItems {
		if b.timer != nil {
			b.timer.Stop()
		}
		full = b.items
		delete(h.batches, roomID)
	}
	h.batchMu.Unlock()

	if len(flushNow) > 0 {
		h.broadcastBatch(roomID, flushNow)
	}
	if len(full) > 0 {
		h.broadcastBatch(roomID, full)
	}
}

func (h *Hub) flushRoomBatch(roomID string, target *roomBatch) {
	h.batchMu.Lock()
	if h.batches[roomID] != target {
		h.batchMu.Unlock()
		return
	}
	items := target.items
	delete(h.batches, roomID)
	h.batchMu.Unlock()

	if len(items) > 0 {
		h.broadcastBatch(roomID, items)
	}
}

// FlushAllBatches 立即刷出所有房间尚未发出的合包缓冲，用于优雅退出前排空。
func (h *Hub) FlushAllBatches() {
	h.batchMu.Lock()
	if len(h.batches) == 0 {
		h.batchMu.Unlock()
		return
	}
	type pendingRoom struct {
		roomID string
		items  [][]byte
	}
	pending := make([]pendingRoom, 0, len(h.batches))
	for roomID, b := range h.batches {
		if b.timer != nil {
			b.timer.Stop()
		}
		if len(b.items) > 0 {
			pending = append(pending, pendingRoom{roomID: roomID, items: b.items})
		}
		delete(h.batches, roomID)
	}
	h.batchMu.Unlock()

	for _, entry := range pending {
		h.broadcastBatch(entry.roomID, entry.items)
	}
}

func (h *Hub) broadcastBatch(roomID string, items [][]byte) {
	if len(items) == 0 {
		return
	}
	if len(items) == 1 {
		h.broadcastWithCount(roomID, websocket.TextMessage, items[0], 1)
		return
	}
	payload := encodeBatchFrame(items)
	h.broadcastWithCount(roomID, websocket.TextMessage, payload, int64(len(items)))
}

// encodeBatchFrame 直接拼接已序列化的单条弹幕 JSON，避免重复 Unmarshal/Marshal。
func encodeBatchFrame(items [][]byte) []byte {
	const prefix = `{"type":"danmaku_batch","items":[`
	const suffix = `]}`
	total := len(prefix) + len(suffix) + len(items) - 1
	for _, item := range items {
		total += len(item)
	}
	buf := make([]byte, 0, total)
	buf = append(buf, prefix...)
	for i, item := range items {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, item...)
	}
	buf = append(buf, suffix...)
	return buf
}
