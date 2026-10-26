package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func waitForRoomCount(t *testing.T, roomID string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		roomsMu.Lock()
		members, exists := rooms[roomID]
		count := len(members)
		roomsMu.Unlock()
		if count == want && (want != 0 || !exists) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("room %q: connections = %d, exists = %v, want %d", roomID, count, exists, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBroadcast(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(handleWebSocket))
	defer server.Close()
	defer waitForRoomCount(t, "lobby", 0)

	clients := make([]*websocket.Conn, 2)
	for i := range clients {
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = conn
		defer conn.Close()
	}
	waitForRoomCount(t, "lobby", 2)

	// 两个客户端同时发送，验证每个接收者都收到全部消息且顺序一致。
	const perClient = 10
	var writers sync.WaitGroup
	for i, conn := range clients {
		writers.Go(func() {
			if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Error(err)
				return
			}
			for j := range perClient {
				if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("%d:%d", i, j))); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	writers.Wait()

	var firstOrder []string
	for i, conn := range clients {
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		seen := make(map[string]bool)
		for j := range len(clients) * perClient {
			kind, message, err := conn.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			value := string(message)
			var sender, sequence int
			_, parseErr := fmt.Sscanf(value, "%d:%d", &sender, &sequence)
			if kind != websocket.TextMessage || parseErr != nil || sender < 0 || sender >= len(clients) || sequence < 0 || sequence >= perClient || seen[value] {
				t.Fatalf("unexpected message: type=%d, body=%q", kind, value)
			}
			seen[value] = true
			if i == 0 {
				firstOrder = append(firstOrder, value)
			} else if value != firstOrder[j] {
				t.Fatalf("message %d = %q, want %q", j, value, firstOrder[j])
			}
		}
	}

	clients[1].Close()
	waitForRoomCount(t, "lobby", 1)
	if err := clients[0].WriteMessage(websocket.BinaryMessage, []byte("still connected")); err != nil {
		t.Fatal(err)
	}
	kind, message, err := clients[0].ReadMessage()
	if err != nil || kind != websocket.BinaryMessage || string(message) != "still connected" {
		t.Fatalf("after disconnect: type=%d, body=%q, err=%v", kind, message, err)
	}
}

func TestRoomIsolationAndRejoin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(handleWebSocket))
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		waitForRoomCount(t, "red", 0)
		waitForRoomCount(t, "blue", 0)
	})

	dial := func(roomID string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?room="+roomID, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	exchange := func(conn *websocket.Conn, message string) {
		t.Helper()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
			t.Fatal(err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		kind, got, err := conn.ReadMessage()
		if err != nil || kind != websocket.TextMessage || string(got) != message {
			t.Fatalf("message = %q, type = %d, err = %v, want %q", got, kind, err, message)
		}
	}

	red, blue := dial("red"), dial("blue")
	waitForRoomCount(t, "red", 1)
	waitForRoomCount(t, "blue", 1)
	// 交替发送标记；若串房，下一次读取将先读到其他房间的消息。
	exchange(red, "red only")
	exchange(blue, "blue only")
	exchange(red, "red again")
	red.Close()
	waitForRoomCount(t, "red", 0)
	waitForRoomCount(t, "blue", 1)
	red = dial("red")
	waitForRoomCount(t, "red", 1)
	exchange(red, "new red room")
	exchange(blue, "blue still connected")
}

func TestRoomNameLimit(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/ws?room="+strings.Repeat("a", 129), nil)
	response := httptest.NewRecorder()
	handleWebSocket(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}
