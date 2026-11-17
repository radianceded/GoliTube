package main

import (
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
)

// Config 描述一次压测的负载形状和连接方式。
type Config struct {
	BaseURL         string
	WSURL           string
	Connections     int
	Rooms           int
	RatePerRoom     float64
	MsgSize         int
	Warmup          time.Duration
	Duration        time.Duration
	Drain           time.Duration
	ConnectRate     float64
	SendersPerRoom  int
	Accounts        int
	Password        string
	MySQLDSN        string
	RedisAddr       string
	RoomPrefix      string
	Room            string
	Tag             uint16
	RunID           string
	OutDir          string
	SummaryInterval time.Duration
	StopFile        string
	SeedOnly        bool
	Reseed          bool
	Cleanup         bool
	NoSeed          bool
	NoDedup         bool
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func parseConfig() (*Config, error) {
	cfg := &Config{}
	flag.StringVar(&cfg.BaseURL, "base", envOr("GOLITUBE_LOADTEST_BASE", "http://127.0.0.1:8081"), "HTTP 服务地址，用于推导 WebSocket 地址")
	flag.StringVar(&cfg.WSURL, "ws", "", "WebSocket 地址；留空时由 -base 推导为 ws(s)://host/ws")
	flag.IntVar(&cfg.Connections, "connections", 100, "连接总数")
	flag.IntVar(&cfg.Rooms, "rooms", 1, "房间数；连接平均分配，余数给前面的房间")
	flag.Float64Var(&cfg.RatePerRoom, "rate", 0, "每个房间每秒发送的消息数，0 表示只连接不发送")
	flag.IntVar(&cfg.MsgSize, "msg-size", 128, "发送 JSON 文本的目标字节数，内容上限 255 个字符")
	flag.DurationVar(&cfg.Warmup, "warmup", 30*time.Second, "预热时长，期间同样发送流量但不计入统计")
	flag.DurationVar(&cfg.Duration, "duration", 2*time.Minute, "正式测量时长")
	flag.DurationVar(&cfg.Drain, "drain", 3*time.Second, "停止发送后等待在途广播的时间")
	flag.Float64Var(&cfg.ConnectRate, "connect-rate", 100, "每秒建立连接数，0 表示不限制")
	flag.IntVar(&cfg.SendersPerRoom, "senders", 0, "每个房间的发送连接数，0 表示按速率自动计算")
	flag.IntVar(&cfg.Accounts, "accounts", 0, "注入的独立账号数，0 表示每个发送连接一个账号")
	flag.StringVar(&cfg.Password, "password", "loadtest-pass-2026", "测试账号密码，仅用于直接写入会话")
	flag.StringVar(&cfg.MySQLDSN, "mysql-dsn", envOr("GOLITUBE_LOADTEST_MYSQL_DSN", ""), "测试库 DSN，用于注入账号")
	flag.StringVar(&cfg.RedisAddr, "redis-addr", envOr("GOLITUBE_LOADTEST_REDIS_ADDR", "127.0.0.1:6379"), "Redis 地址，用于注入会话")
	flag.StringVar(&cfg.RoomPrefix, "room-prefix", "lt", "房间名前缀，最终名称包含 run id")
	flag.StringVar(&cfg.Room, "room", "", "所有连接使用同一个固定房间名；设置后忽略 -rooms，用于多实例共享同一房间")
	flag.StringVar(&cfg.RunID, "run-id", time.Now().Format("20060102-150405"), "本次运行的标识，用于房间名、账号名和结果目录")
	flag.StringVar(&cfg.OutDir, "out", "", "结果目录，默认 loadtest-results/<run-id>")
	flag.DurationVar(&cfg.SummaryInterval, "summary-interval", 10*time.Second, "汇总输出间隔")
	flag.StringVar(&cfg.StopFile, "stop-file", "", "该文件出现时停止发送并写出部分结果；用于后台运行时干净地中途结束")
	flag.BoolVar(&cfg.SeedOnly, "seed-only", false, "只注入账号并退出")
	flag.BoolVar(&cfg.Reseed, "reseed", false, "按 -out 中的 accounts.json 重新写入 Redis 会话后退出，用于 Redis 重启后恢复")
	flag.BoolVar(&cfg.Cleanup, "cleanup", false, "删除本次 run id 的测试账号和会话后退出")
	flag.BoolVar(&cfg.NoSeed, "no-seed", false, "不注入账号；仅适用于不发送的只连接场景")
	flag.BoolVar(&cfg.NoDedup, "no-dedup", false, "跳过重复检测；多进程共享房间时各进程序号不连续，重复统计不可用")
	flag.Parse()

	if cfg.OutDir == "" {
		cfg.OutDir = "loadtest-results/" + cfg.RunID
	}
	if cfg.Room != "" {
		cfg.Rooms = 1
	}
	// 不同压测进程可能向同一房间发送，用 run id 的 16 位标签抬高各自的序号起点。
	tagSum := sha256.Sum256([]byte(cfg.RunID))
	cfg.Tag = binary.BigEndian.Uint16(tagSum[:2])
	if cfg.WSURL == "" {
		cfg.WSURL = deriveWSURL(cfg.BaseURL)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func deriveWSURL(base string) string {
	switch {
	case strings.HasPrefix(base, "https://"):
		return "wss://" + strings.TrimPrefix(base, "https://") + "/ws"
	case strings.HasPrefix(base, "http://"):
		return "ws://" + strings.TrimPrefix(base, "http://") + "/ws"
	default:
		return base + "/ws"
	}
}

func (c *Config) validate() error {
	if c.Connections < 1 {
		return fmt.Errorf("-connections 必须大于 0")
	}
	if c.Rooms < 1 {
		return fmt.Errorf("-rooms 必须大于 0")
	}
	if c.Connections < c.Rooms {
		return fmt.Errorf("-connections(%d) 不能小于 -rooms(%d)", c.Connections, c.Rooms)
	}
	if c.Room != "" && (len(c.Room) > 128 || strings.TrimSpace(c.Room) == "") {
		return fmt.Errorf("-room 须为非空且不超过 128 字节的房间名")
	}
	if c.RatePerRoom < 0 {
		return fmt.Errorf("-rate 不能为负数")
	}
	if c.MsgSize < 64 || c.MsgSize > 320 {
		return fmt.Errorf("-msg-size 须在 64～320 字节之间（内容最多 255 个字符）")
	}
	if c.Warmup < 0 || c.Duration <= 0 {
		return fmt.Errorf("预热不能为负，正式时长必须大于 0")
	}
	if c.ConnectRate < 0 {
		return fmt.Errorf("-connect-rate 不能为负数")
	}
	if c.SummaryInterval < time.Second {
		return fmt.Errorf("-summary-interval 不能小于 1 秒")
	}
	if c.RatePerRoom > 5000 {
		return fmt.Errorf("-rate 单房间上限为 5000/s，避免压测工具自身定时器先成为瓶颈")
	}
	if c.RatePerRoom > 0 && !c.NoSeed && c.MySQLDSN == "" {
		return fmt.Errorf("发送流量需要独立账号：请设置 -mysql-dsn 或 GOLITUBE_LOADTEST_MYSQL_DSN")
	}
	if c.SeedOnly && c.MySQLDSN == "" {
		return fmt.Errorf("-seed-only 需要 -mysql-dsn")
	}
	if c.SeedOnly && c.accountCount() == 0 {
		return fmt.Errorf("-seed-only 需要 -accounts 或 -rate 以确定账号数量")
	}
	return nil
}

// roomShape 返回每个房间的连接数和发送连接数。发送连接按每条 ≤1.5 条/秒分配，
// 同时受服务端单连接令牌桶（2/s）和单用户 5 秒 10 条的限制。
func (c *Config) roomShape() (connsPerRoom, sendersPerRoom int) {
	connsPerRoom = c.Connections / c.Rooms
	sendersPerRoom = c.SendersPerRoom
	if c.RatePerRoom <= 0 {
		return connsPerRoom, 0
	}
	if sendersPerRoom <= 0 {
		sendersPerRoom = int(math.Ceil(c.RatePerRoom / 1.5))
	}
	if sendersPerRoom < 1 {
		sendersPerRoom = 1
	}
	if sendersPerRoom > connsPerRoom {
		sendersPerRoom = connsPerRoom
	}
	return connsPerRoom, sendersPerRoom
}

func (c *Config) roomID(index int) string {
	if c.Room != "" {
		return c.Room
	}
	return fmt.Sprintf("%s-%s-r%03d", c.RoomPrefix, c.RunID, index)
}

// accountCount 返回需要注入的账号数，默认每个发送连接一个独立账号。
func (c *Config) accountCount() int {
	if c.Accounts > 0 {
		return c.Accounts
	}
	_, senders := c.roomShape()
	return senders * c.Rooms
}
