package main

import (
	"os"
	"runtime"
	"time"
)

type ConfigView struct {
	WSURL           string  `json:"ws_url"`
	Connections     int     `json:"connections"`
	Rooms           int     `json:"rooms"`
	RatePerRoom     float64 `json:"rate_per_room"`
	MsgSize         int     `json:"msg_size"`
	WarmupSeconds   float64 `json:"warmup_seconds"`
	DurationSeconds float64 `json:"duration_seconds"`
	ConnectRate     float64 `json:"connect_rate"`
	SendersPerRoom  int     `json:"senders_per_room"`
	Accounts        int     `json:"accounts"`
	RoomPrefix      string  `json:"room_prefix"`
}

type EnvironmentView struct {
	GoVersion  string `json:"go_version"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	NumCPU     int    `json:"num_cpu"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	Hostname   string `json:"hostname"`
}

type MeasurementView struct {
	WarmupSeconds   float64 `json:"warmup_seconds"`
	DurationSeconds float64 `json:"duration_seconds"`
	ActualSeconds   float64 `json:"actual_seconds"`
}

type Report struct {
	RunID                string          `json:"run_id"`
	GeneratedAt          string          `json:"generated_at"`
	OutputDir            string          `json:"output_dir"`
	Config               ConfigView      `json:"config"`
	Environment          EnvironmentView `json:"environment"`
	Measurement          MeasurementView `json:"measurement"`
	Totals               Summary         `json:"totals"`
	ExpectedByFormula    int64           `json:"expected_by_formula"`
	FormulaReceivedRatio float64         `json:"formula_received_ratio"`
	Rooms                []RoomResult    `json:"rooms"`
	DisconnectReasons    map[string]int  `json:"disconnect_reasons"`
	Interrupted          bool            `json:"interrupted"`
	Notes                []string        `json:"notes"`
}

func buildReport(cfg *Config, stats *Stats, final Summary, interrupted bool) Report {
	hostname, _ := os.Hostname()
	formulaExpected := stats.ExpectedFromFormula()
	report := Report{
		RunID:       cfg.RunID,
		GeneratedAt: time.Now().Format(time.RFC3339),
		OutputDir:   cfg.OutDir,
		Config: ConfigView{
			WSURL:           cfg.WSURL,
			Connections:     cfg.Connections,
			Rooms:           cfg.Rooms,
			RatePerRoom:     cfg.RatePerRoom,
			MsgSize:         cfg.MsgSize,
			WarmupSeconds:   cfg.Warmup.Seconds(),
			DurationSeconds: cfg.Duration.Seconds(),
			ConnectRate:     cfg.ConnectRate,
			SendersPerRoom:  cfg.sendersPerRoomValue(),
			Accounts:        cfg.accountCount(),
			RoomPrefix:      cfg.RoomPrefix,
		},
		Environment: EnvironmentView{
			GoVersion:  runtime.Version(),
			GOOS:       runtime.GOOS,
			GOARCH:     runtime.GOARCH,
			NumCPU:     runtime.NumCPU(),
			GOMAXPROCS: runtime.GOMAXPROCS(0),
			Hostname:   hostname,
		},
		Measurement: MeasurementView{
			WarmupSeconds:   cfg.Warmup.Seconds(),
			DurationSeconds: cfg.Duration.Seconds(),
			ActualSeconds:   final.ElapsedSeconds,
		},
		Totals:            final,
		ExpectedByFormula: formulaExpected,
		Rooms:             stats.RoomResults(),
		DisconnectReasons: stats.DisconnectReasons(),
		Interrupted:       interrupted,
		Notes: []string{
			"端到端延迟从发送连接写出前到最后一条接收连接读到广播，包含 Kafka 接收确认、Redis 广播和本机网络，不含 MySQL 落库。",
			"missing = expected_deliveries - received；expected 按每条消息写出时的房间存活连接数累计，formula 另按发送数 × 房间连接数独立核对。",
			"duplicates 用每条连接最近 16384 条序号检测，超出窗口的迟到消息按重复计。",
			"账号通过测试库和 Redis 会话直接注入，不经过 /auth/register，避免每 IP 每分钟 30 次的注册限流。",
		},
	}
	if formulaExpected > 0 {
		report.FormulaReceivedRatio = float64(final.Received) / float64(formulaExpected)
	}
	return report
}

func (c *Config) sendersPerRoomValue() int {
	_, senders := c.roomShape()
	return senders
}
