package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

type HistoryQuery struct {
	RoomID          string
	StartMS, EndMS  int64
	Limit           int
	AfterPositionMS int64
	AfterID         string
}

type HistoryPage struct {
	Items      []Danmaku `json:"items"`
	NextCursor string    `json:"next_cursor"`
}

type historyCursor struct {
	RoomID     string `json:"room_id"`
	StartMS    int64  `json:"start_ms"`
	EndMS      int64  `json:"end_ms"`
	PositionMS int64  `json:"position_ms"`
	ID         string `json:"id"`
}

func parseHistoryQuery(r *http.Request) (HistoryQuery, error) {
	values := r.URL.Query()
	q := HistoryQuery{RoomID: strings.TrimSpace(values.Get("room")), Limit: 200}
	if q.RoomID == "" || len(q.RoomID) > 128 || !utf8.ValidString(q.RoomID) {
		return q, errors.New("room 必填，且须为不超过 128 字节的 UTF-8 房间名")
	}
	var err error
	q.StartMS, err = strconv.ParseInt(values.Get("start_ms"), 10, 64)
	if err != nil || q.StartMS < 0 {
		return q, errors.New("start_ms 必须为非负整数")
	}
	q.EndMS, err = strconv.ParseInt(values.Get("end_ms"), 10, 64)
	if err != nil || q.EndMS <= q.StartMS || q.EndMS-q.StartMS > 60000 {
		return q, errors.New("end_ms 必须大于 start_ms，且时间段不能超过 60000 毫秒")
	}
	if value := values.Get("limit"); value != "" {
		q.Limit, err = strconv.Atoi(value)
		if err != nil || q.Limit < 1 || q.Limit > 500 {
			return q, errors.New("limit 须为 1～500 的整数")
		}
	}
	if token := values.Get("cursor"); token != "" {
		if len(token) > 2048 {
			return q, errors.New("cursor 无效")
		}
		payload, err := base64.RawURLEncoding.DecodeString(token)
		var cursor historyCursor
		if err != nil || json.Unmarshal(payload, &cursor) != nil || cursor.RoomID != q.RoomID ||
			cursor.StartMS != q.StartMS || cursor.EndMS != q.EndMS || cursor.PositionMS < q.StartMS ||
			cursor.PositionMS >= q.EndMS || cursor.ID == "" || len(cursor.ID) > 128 {
			return q, errors.New("cursor 无效，必须沿用同一房间和时间段")
		}
		q.AfterPositionMS, q.AfterID = cursor.PositionMS, cursor.ID
	}
	return q, nil
}

func handleHistory(query func(context.Context, HistoryQuery) (HistoryPage, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		fail := func(status int, code, message string) {
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(ProtocolError{Type: "error", Code: code, Message: message})
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			fail(http.StatusMethodNotAllowed, "method_not_allowed", "仅支持 GET")
			return
		}
		q, err := parseHistoryQuery(r)
		if err != nil {
			fail(http.StatusBadRequest, "invalid_query", err.Error())
			return
		}
		if query == nil {
			fail(http.StatusServiceUnavailable, "history_unavailable", "历史查询暂不可用")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		page, err := query(ctx, q)
		if err != nil {
			log.Println("历史弹幕查询失败:", err)
			fail(http.StatusServiceUnavailable, "history_unavailable", "历史查询暂不可用")
			return
		}
		if page.Items == nil {
			page.Items = []Danmaku{}
		}
		json.NewEncoder(w).Encode(page)
	}
}

func queryHistory(ctx context.Context, db *sql.DB, q HistoryQuery) (HistoryPage, error) {
	statement := `SELECT id, room_id, content, position_ms, color, created_at_ms
		FROM danmaku_history WHERE room_id = ? AND position_ms >= ? AND position_ms < ?`
	args := []any{q.RoomID, q.StartMS, q.EndMS}
	if q.AfterID != "" {
		statement += " AND (position_ms > ? OR (position_ms = ? AND id > ?))"
		args = append(args, q.AfterPositionMS, q.AfterPositionMS, q.AfterID)
	}
	statement += " ORDER BY position_ms ASC, id ASC LIMIT ?"
	args = append(args, q.Limit+1)
	rows, err := db.QueryContext(ctx, statement, args...)
	if err != nil {
		return HistoryPage{}, err
	}
	defer rows.Close()
	page := HistoryPage{Items: make([]Danmaku, 0, q.Limit)}
	for rows.Next() {
		event := Danmaku{Type: "danmaku"}
		if err := rows.Scan(&event.ID, &event.RoomID, &event.Content, &event.PositionMS, &event.Color, &event.CreatedAtMS); err != nil {
			return HistoryPage{}, err
		}
		page.Items = append(page.Items, event)
	}
	if err := rows.Err(); err != nil {
		return HistoryPage{}, err
	}
	if len(page.Items) > q.Limit {
		page.Items = page.Items[:q.Limit]
		last := page.Items[len(page.Items)-1]
		payload, _ := json.Marshal(historyCursor{RoomID: q.RoomID, StartMS: q.StartMS, EndMS: q.EndMS, PositionMS: last.PositionMS, ID: last.ID})
		page.NextCursor = base64.RawURLEncoding.EncodeToString(payload)
	}
	return page, nil
}

// 旧的原样文本/二进制、字段不完整或 ID 不一致的记录不猜测播放位置，只保留归档。
func historyDanmaku(event RoomEvent) (Danmaku, bool) {
	var stored struct {
		Type        string `json:"type"`
		ID          string `json:"id"`
		RoomID      string `json:"room_id"`
		Content     string `json:"content"`
		PositionMS  *int64 `json:"position_ms"`
		Color       string `json:"color"`
		CreatedAtMS int64  `json:"created_at_ms"`
	}
	if event.Type != websocket.TextMessage || !utf8.Valid(event.Data) || json.Unmarshal(event.Data, &stored) != nil ||
		stored.Type != "danmaku" || stored.ID == "" || len(stored.ID) > 128 || stored.ID != event.ID ||
		stored.RoomID == "" || len(stored.RoomID) > 128 || !utf8.ValidString(stored.RoomID) || stored.RoomID != event.RoomID ||
		stored.PositionMS == nil || *stored.PositionMS < 0 || stored.CreatedAtMS <= 0 ||
		strings.TrimSpace(stored.Content) != stored.Content || utf8.RuneCountInString(stored.Content) < 1 ||
		utf8.RuneCountInString(stored.Content) > maxDanmakuCharacters || !colorPattern.MatchString(stored.Color) {
		return Danmaku{}, false
	}
	for _, character := range stored.Content {
		if unicode.IsControl(character) {
			return Danmaku{}, false
		}
	}
	return Danmaku{Type: stored.Type, ID: stored.ID, RoomID: stored.RoomID, Content: stored.Content,
		PositionMS: *stored.PositionMS, Color: stored.Color, CreatedAtMS: stored.CreatedAtMS}, true
}

func projectHistory(ctx context.Context, tx *sql.Tx, event RoomEvent) (bool, error) {
	danmaku, ok := historyDanmaku(event)
	if !ok {
		return false, nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO danmaku_history (id,room_id,content,position_ms,color,created_at_ms)
		VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE id=id`, danmaku.ID, danmaku.RoomID, danmaku.Content,
		danmaku.PositionMS, danmaku.Color, danmaku.CreatedAtMS)
	return true, err
}

func backfillHistory(ctx context.Context, db *sql.DB) (scanned, eligible int, err error) {
	lastID := ""
	for {
		rows, queryErr := db.QueryContext(ctx, "SELECT id,room_id,message_type,data FROM danmaku WHERE id > ? ORDER BY id LIMIT 200", lastID)
		if queryErr != nil {
			return scanned, eligible, queryErr
		}
		batch := make([]RoomEvent, 0, 200)
		for rows.Next() {
			var event RoomEvent
			if err := rows.Scan(&event.ID, &event.RoomID, &event.Type, &event.Data); err != nil {
				rows.Close()
				return scanned, eligible, err
			}
			batch = append(batch, event)
		}
		readErr := rows.Err()
		rows.Close()
		if readErr != nil {
			return scanned, eligible, readErr
		}
		if len(batch) == 0 {
			return scanned, eligible, nil
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return scanned, eligible, err
		}
		count := 0
		for _, event := range batch {
			ok, err := projectHistory(ctx, tx, event)
			if err != nil {
				tx.Rollback()
				return scanned, eligible, err
			}
			if ok {
				count++
			}
		}
		if err := tx.Commit(); err != nil {
			return scanned, eligible, err
		}
		scanned += len(batch)
		eligible += count
		lastID = batch[len(batch)-1].ID
	}
}

func runHistoryBackfill(ctx context.Context, dsn string) error {
	db, err := openPersistenceDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	scanned, eligible, err := backfillHistory(ctx, db)
	if err != nil {
		return err
	}
	log.Printf("历史弹幕回填完成：扫描 %d 条，识别 %d 条正式弹幕（含已有记录）", scanned, eligible)
	return nil
}
