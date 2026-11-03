package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHistoryHTTP(t *testing.T) {
	called := false
	handler := handleHistory(func(ctx context.Context, q HistoryQuery) (HistoryPage, error) {
		called = true
		if q.RoomID != "room" || q.StartMS != 0 || q.EndMS != 10000 || q.Limit != 200 {
			t.Fatalf("unexpected query: %+v", q)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("query has no deadline")
		}
		return HistoryPage{}, nil
	})
	response := httptest.NewRecorder()
	handler(response, httptest.NewRequest(http.MethodGet, "/danmaku/history?room=room&start_ms=0&end_ms=10000", nil))
	if response.Code != 200 || !called || response.Body.String() != "{\"items\":[],\"next_cursor\":\"\"}\n" {
		t.Fatalf("status=%d body=%s", response.Code, response.Body)
	}
	for _, query := range []string{
		"start_ms=0&end_ms=10000", "room=room&end_ms=10000", "room=room&start_ms=0",
		"room=room&start_ms=-1&end_ms=1", "room=room&start_ms=1&end_ms=1",
		"room=room&start_ms=0&end_ms=60001", "room=room&start_ms=0.5&end_ms=10000",
		"room=room&start_ms=0&end_ms=9223372036854775808",
		"room=room&start_ms=0&end_ms=10000&limit=0", "room=room&start_ms=0&end_ms=10000&limit=501",
		"room=room&start_ms=0&end_ms=10000&cursor=garbage", "room=%FF&start_ms=0&end_ms=10000",
	} {
		called = false
		response := httptest.NewRecorder()
		handler(response, httptest.NewRequest(http.MethodGet, "/danmaku/history?"+query, nil))
		if response.Code != 400 || called {
			t.Fatalf("query=%s status=%d called=%v", query, response.Code, called)
		}
	}
	response = httptest.NewRecorder()
	handler(response, httptest.NewRequest(http.MethodPost, "/danmaku/history", nil))
	if response.Code != 405 || response.Header().Get("Allow") != "GET" {
		t.Fatal("method restriction failed")
	}
	for _, query := range []func(context.Context, HistoryQuery) (HistoryPage, error){nil, func(context.Context, HistoryQuery) (HistoryPage, error) {
		return HistoryPage{}, errors.New("private database details")
	}} {
		response := httptest.NewRecorder()
		handleHistory(query)(response, httptest.NewRequest(http.MethodGet, "/danmaku/history?room=room&start_ms=0&end_ms=10000", nil))
		var body ProtocolError
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != 503 || body.Code != "history_unavailable" || body.Message != "历史查询暂不可用" {
			t.Fatalf("body=%+v status=%d err=%v", body, response.Code, err)
		}
	}
}

func TestHistoryCursorScope(t *testing.T) {
	payload, err := json.Marshal(historyCursor{RoomID: "room", StartMS: 0, EndMS: 10000, PositionMS: 5000, ID: "id"})
	if err != nil {
		t.Fatal(err)
	}
	token := base64.RawURLEncoding.EncodeToString(payload)
	for _, query := range []string{
		"room=room&start_ms=0&end_ms=10000", "room=other&start_ms=0&end_ms=10000",
		"room=room&start_ms=1&end_ms=10000", "room=room&start_ms=0&end_ms=9999",
	} {
		q, err := parseHistoryQuery(httptest.NewRequest("GET", "/danmaku/history?"+query+"&cursor="+token, nil))
		valid := query == "room=room&start_ms=0&end_ms=10000"
		if valid && (err != nil || q.AfterID != "id" || q.AfterPositionMS != 5000) {
			t.Fatalf("q=%+v err=%v", q, err)
		}
		if !valid && err == nil {
			t.Fatalf("accepted cursor for another scope: %s", query)
		}
	}
}
