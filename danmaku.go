package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

const maxDanmakuCharacters = 255

var colorPattern = regexp.MustCompile(`^#[0-9a-fA-F]{6}$`)

// 客户端只能提供业务输入，不能指定事件 ID、房间或服务端接收时间。
type SendDanmaku struct {
	Content    string `json:"content"`
	PositionMS *int64 `json:"position_ms"`
	Color      string `json:"color"`
}

// 广播和保存使用同一份服务端生成的 JSON，便于实时与历史消息对应。
type Danmaku struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	RoomID      string `json:"room_id"`
	UserID      string `json:"user_id,omitempty"` // 旧归档没有作者；新弹幕只能使用会话中的 ID。
	Content     string `json:"content"`
	PositionMS  int64  `json:"position_ms"`
	Color       string `json:"color"`
	CreatedAtMS int64  `json:"created_at_ms"`
}

type ProtocolError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *ProtocolError) Error() string { return e.Message }

func decodeDanmaku(roomID string, messageType int, data []byte, userID string) (RoomEvent, error) {
	if userID == "" {
		return RoomEvent{}, errors.New("弹幕缺少服务端用户身份")
	}
	if messageType != websocket.TextMessage || !utf8.Valid(data) {
		return RoomEvent{}, errors.New("弹幕必须使用 UTF-8 JSON 文本消息")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var input SendDanmaku
	if err := decoder.Decode(&input); err != nil {
		return RoomEvent{}, errors.New("格式错误，仅接受 content、position_ms 和 color")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return RoomEvent{}, errors.New("一次只能发送一个 JSON 对象")
	}
	input.Content = strings.TrimSpace(input.Content)
	if count := utf8.RuneCountInString(input.Content); count == 0 || count > maxDanmakuCharacters {
		return RoomEvent{}, errors.New("弹幕内容须为 1～255 个 Unicode 字符")
	}
	for _, character := range input.Content {
		if unicode.IsControl(character) {
			return RoomEvent{}, errors.New("弹幕内容不能包含换行或控制字符")
		}
	}
	if input.PositionMS == nil || *input.PositionMS < 0 {
		return RoomEvent{}, errors.New("position_ms 必须提供，且为非负整数毫秒")
	}
	if input.Color == "" {
		input.Color = "#ffffff"
	}
	if !colorPattern.MatchString(input.Color) {
		return RoomEvent{}, errors.New("color 须为 #RRGGBB 格式")
	}
	event := Danmaku{
		Type: "danmaku", ID: rand.Text(), RoomID: roomID, UserID: userID,
		Content: input.Content, PositionMS: *input.PositionMS,
		Color: strings.ToLower(input.Color), CreatedAtMS: time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return RoomEvent{}, err
	}
	return RoomEvent{ID: event.ID, RoomID: roomID, Type: websocket.TextMessage, Data: payload}, nil
}
