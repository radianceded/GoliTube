package history

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
	"github.com/radianceded/GoliTube/internal/danmaku"
)

type Query struct {
	RoomID          string
	StartMS, EndMS  int64
	Limit           int
	AfterPositionMS int64
	AfterID         string
}

type Page struct {
	Items      []danmaku.Danmaku `json:"items"`
	NextCursor string            `json:"next_cursor"`
}

type cursor struct {
	RoomID     string `json:"room_id"`
	StartMS    int64  `json:"start_ms"`
	EndMS      int64  `json:"end_ms"`
	PositionMS int64  `json:"position_ms"`
	ID         string `json:"id"`
}

func parseQuery(r *http.Request) (Query, error) {
	values := r.URL.Query()
	q := Query{RoomID: strings.TrimSpace(values.Get("room")), Limit: 200}
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
		var c cursor
		if err != nil || json.Unmarshal(payload, &c) != nil || c.RoomID != q.RoomID ||
			c.StartMS != q.StartMS || c.EndMS != q.EndMS || c.PositionMS < q.StartMS ||
			c.PositionMS >= q.EndMS || c.ID == "" || len(c.ID) > 128 {
			return q, errors.New("cursor 无效，必须沿用同一房间和时间段")
		}
		q.AfterPositionMS, q.AfterID = c.PositionMS, c.ID
	}
	return q, nil
}

func Handler(query func(context.Context, Query) (Page, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		fail := func(status int, code, message string) {
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(danmaku.ProtocolError{Type: "error", Code: code, Message: message})
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			fail(http.StatusMethodNotAllowed, "method_not_allowed", "仅支持 GET")
			return
		}
		q, err := parseQuery(r)
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
			page.Items = []danmaku.Danmaku{}
		}
		json.NewEncoder(w).Encode(page)
	}
}

func Fetch(ctx context.Context, db *sql.DB, q Query) (Page, error) {
	statement := `SELECT h.id, h.room_id, h.content, h.position_ms, h.color, h.created_at_ms, h.user_id, COALESCE(u.username, '')
		FROM danmaku_history h LEFT JOIN users u ON u.id = h.user_id
		WHERE h.room_id = ? AND h.position_ms >= ? AND h.position_ms < ?`
	args := []any{q.RoomID, q.StartMS, q.EndMS}
	if q.AfterID != "" {
		statement += " AND (h.position_ms > ? OR (h.position_ms = ? AND h.id > ?))"
		args = append(args, q.AfterPositionMS, q.AfterPositionMS, q.AfterID)
	}
	statement += " ORDER BY h.position_ms ASC, h.id ASC LIMIT ?"
	args = append(args, q.Limit+1)
	rows, err := db.QueryContext(ctx, statement, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	page := Page{Items: make([]danmaku.Danmaku, 0, q.Limit)}
	for rows.Next() {
		event := danmaku.Danmaku{Type: "danmaku"}
		var userID sql.NullString
		if err := rows.Scan(&event.ID, &event.RoomID, &event.Content, &event.PositionMS, &event.Color, &event.CreatedAtMS, &userID, &event.Username); err != nil {
			return Page{}, err
		}
		event.UserID = userID.String
		page.Items = append(page.Items, event)
	}
	if err := rows.Err(); err != nil {
		return Page{}, err
	}
	if len(page.Items) > q.Limit {
		page.Items = page.Items[:q.Limit]
		last := page.Items[len(page.Items)-1]
		payload, _ := json.Marshal(cursor{RoomID: q.RoomID, StartMS: q.StartMS, EndMS: q.EndMS, PositionMS: last.PositionMS, ID: last.ID})
		page.NextCursor = base64.RawURLEncoding.EncodeToString(payload)
	}
	return page, nil
}

// 旧的原样文本/二进制、字段不完整或 ID 不一致的记录不猜测播放位置，只保留归档。
func toDanmaku(event danmaku.RoomEvent) (danmaku.Danmaku, bool) {
	var stored struct {
		Type        string `json:"type"`
		ID          string `json:"id"`
		RoomID      string `json:"room_id"`
		UserID      string `json:"user_id"`
		Content     string `json:"content"`
		PositionMS  *int64 `json:"position_ms"`
		Color       string `json:"color"`
		CreatedAtMS int64  `json:"created_at_ms"`
	}
	if event.Type != websocket.TextMessage || !utf8.Valid(event.Data) || json.Unmarshal(event.Data, &stored) != nil ||
		stored.Type != "danmaku" || stored.ID == "" || len(stored.ID) > 128 || stored.ID != event.ID ||
		stored.RoomID == "" || len(stored.RoomID) > 128 || !utf8.ValidString(stored.RoomID) || stored.RoomID != event.RoomID ||
		len(stored.UserID) > 128 || !utf8.ValidString(stored.UserID) ||
		stored.PositionMS == nil || *stored.PositionMS < 0 || stored.CreatedAtMS <= 0 ||
		strings.TrimSpace(stored.Content) != stored.Content || utf8.RuneCountInString(stored.Content) < 1 ||
		utf8.RuneCountInString(stored.Content) > danmaku.MaxCharacters || !danmaku.ColorPattern.MatchString(stored.Color) {
		return danmaku.Danmaku{}, false
	}
	for _, character := range stored.Content {
		if unicode.IsControl(character) {
			return danmaku.Danmaku{}, false
		}
	}
	return danmaku.Danmaku{Type: stored.Type, ID: stored.ID, RoomID: stored.RoomID, UserID: stored.UserID, Content: stored.Content,
		PositionMS: *stored.PositionMS, Color: stored.Color, CreatedAtMS: stored.CreatedAtMS}, true
}

func Project(ctx context.Context, tx *sql.Tx, event danmaku.RoomEvent) (bool, error) {
	d, ok := toDanmaku(event)
	if !ok {
		return false, nil
	}
	author := sql.NullString{String: d.UserID, Valid: d.UserID != ""}
	_, err := tx.ExecContext(ctx, `INSERT INTO danmaku_history (id,room_id,content,position_ms,color,created_at_ms,user_id)
		VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE user_id=COALESCE(user_id,?)`, d.ID, d.RoomID, d.Content,
		d.PositionMS, d.Color, d.CreatedAtMS, author, author)
	return true, err
}

func Backfill(ctx context.Context, db *sql.DB) (scanned, eligible int, err error) {
	lastID := ""
	for {
		rows, queryErr := db.QueryContext(ctx, "SELECT id,room_id,message_type,data FROM danmaku WHERE id > ? ORDER BY id LIMIT 200", lastID)
		if queryErr != nil {
			return scanned, eligible, queryErr
		}
		batch := make([]danmaku.RoomEvent, 0, 200)
		for rows.Next() {
			var event danmaku.RoomEvent
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
			ok, err := Project(ctx, tx, event)
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
