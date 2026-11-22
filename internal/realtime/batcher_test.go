package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/radianceded/GoliTube/internal/danmaku"
)

func TestBroadcastBatchingCountAndWindow(t *testing.T) {
	hub := NewHub()
	hub.EnableBroadcastBatching(3, 20*time.Millisecond)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeWebSocket(w, r, PingInterval, PongWait, func(_ context.Context, e danmaku.RoomEvent) error {
			hub.EnqueueBroadcast(e.RoomID, e.Type, e.Data)
			return nil
		}, rawTestEvent)
	}))
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"?room=batch-room", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}

	// 1) 连续入队 3 条 JSON 弹幕，触发满 3 条立即合包刷出为 danmaku_batch。
	for i := range 3 {
		payload := fmt.Sprintf(`{"type":"danmaku","id":"m%d","room_id":"batch-room","content":"hi-%d"}`, i, i)
		hub.EnqueueBroadcast("batch-room", websocket.TextMessage, []byte(payload))
	}
	kind, body, err := conn.ReadMessage()
	if err != nil || kind != websocket.TextMessage {
		t.Fatalf("read batch failed: kind=%d err=%v", kind, err)
	}
	var batch struct {
		Type  string            `json:"type"`
		Items []danmaku.Danmaku `json:"items"`
	}
	if err := json.Unmarshal(body, &batch); err != nil {
		t.Fatalf("unmarshal batch: %v body=%s", err, body)
	}
	if batch.Type != "danmaku_batch" || len(batch.Items) != 3 || batch.Items[0].ID != "m0" || batch.Items[2].ID != "m2" {
		t.Fatalf("unexpected batch: %+v", batch)
	}

	// 2) 只入队 1 条 JSON 弹幕，20ms 到期后保持单条原始 danmaku 格式，不包一层 danmaku_batch。
	singlePayload := `{"type":"danmaku","id":"solo","room_id":"batch-room","content":"solo"}`
	hub.EnqueueBroadcast("batch-room", websocket.TextMessage, []byte(singlePayload))
	kind, body, err = conn.ReadMessage()
	if err != nil || kind != websocket.TextMessage {
		t.Fatalf("read single failed: kind=%d err=%v", kind, err)
	}
	var single danmaku.Danmaku
	if err := json.Unmarshal(body, &single); err != nil {
		t.Fatalf("unmarshal single: %v body=%s", err, body)
	}
	if single.Type != "danmaku" || single.ID != "solo" || single.Content != "solo" {
		t.Fatalf("unexpected single: %+v", single)
	}

	// 3) 入队 2 条后遇到二进制消息，先刷出 2 条批次再原样发二进制，保证顺序不乱。
	hub.EnqueueBroadcast("batch-room", websocket.TextMessage, []byte(`{"type":"danmaku","id":"a","room_id":"batch-room","content":"a"}`))
	hub.EnqueueBroadcast("batch-room", websocket.TextMessage, []byte(`{"type":"danmaku","id":"b","room_id":"batch-room","content":"b"}`))
	hub.EnqueueBroadcast("batch-room", websocket.BinaryMessage, []byte{1, 2, 3})

	_, body, err = conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	batch.Items = nil
	if err := json.Unmarshal(body, &batch); err != nil || batch.Type != "danmaku_batch" || len(batch.Items) != 2 {
		t.Fatalf("expected 2-item batch before binary, got %s err=%v", body, err)
	}
	kind, body, err = conn.ReadMessage()
	if err != nil || kind != websocket.BinaryMessage || len(body) != 3 {
		t.Fatalf("expected binary message, got kind=%d body=%v err=%v", kind, body, err)
	}
}
