package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	cfg, err := parseConfig()
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch {
	case cfg.SeedOnly:
		accounts, err := seedAccounts(ctx, cfg, cfg.accountCount())
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("账号注入完成：%d 个，目录 %s", len(accounts), cfg.OutDir)
		return
	case cfg.Cleanup:
		if err := cleanupAccounts(ctx, cfg); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func sleepCtx(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return true
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type connectGate struct {
	interval time.Duration
	next     time.Time
}

func newConnectGate(rate float64) *connectGate {
	if rate <= 0 {
		return &connectGate{}
	}
	return &connectGate{interval: time.Duration(float64(time.Second) / rate)}
}

func (g *connectGate) Wait(ctx context.Context) bool {
	if g.interval <= 0 {
		return true
	}
	now := time.Now()
	if g.next.IsZero() || g.next.Before(now) {
		g.next = now
	}
	delay := time.Until(g.next)
	g.next = g.next.Add(g.interval)
	if delay <= 0 {
		return true
	}
	return sleepCtx(ctx, delay)
}

func run(ctx context.Context, cfg *Config) error {
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return err
	}
	connsPerRoom, sendersPerRoom := cfg.roomShape()
	log.Printf("开始 run=%s 目标=%s 连接=%d 房间=%d（每房间 %d）发送连接/房间=%d 每房间速率=%.2f/s 消息目标=%dB 预热=%s 正式=%s",
		cfg.RunID, cfg.WSURL, cfg.Connections, cfg.Rooms, connsPerRoom, sendersPerRoom,
		cfg.RatePerRoom, cfg.MsgSize, cfg.Warmup, cfg.Duration)

	rooms := make([]*Room, cfg.Rooms)
	for index := range rooms {
		rooms[index] = &Room{Index: index, ID: cfg.roomID(index), Rate: cfg.RatePerRoom}
	}

	var accounts []Account
	if sendersPerRoom > 0 && !cfg.NoSeed {
		var err error
		accounts, err = seedAccounts(ctx, cfg, sendersPerRoom*len(rooms))
		if err != nil {
			return err
		}
	}

	stats := newStats(rooms, nil)
	gate := newConnectGate(cfg.ConnectRate)
	senderIndex := 0
	connected := 0
	for index, room := range rooms {
		count := cfg.Connections / cfg.Rooms
		if index < cfg.Connections%cfg.Rooms {
			count++
		}
		for slot := 0; slot < count; slot++ {
			var account *Account
			if slot < sendersPerRoom && senderIndex < len(accounts) {
				account = &accounts[senderIndex]
				senderIndex++
			}
			dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			conn := connect(dialCtx, stats, cfg, room, slot, account)
			cancel()
			if conn != nil {
				stats.conns = append(stats.conns, conn)
				if conn.Sender {
					room.Senders = append(room.Senders, conn)
				}
				connected++
				if connected%500 == 0 {
					log.Printf("已建立 %d/%d 条连接，失败 %d", connected, cfg.Connections, stats.connectFailures.Load())
				}
			}
			if !gate.Wait(ctx) {
				log.Printf("建立连接时收到退出信号，已建立 %d 条", connected)
				break
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	log.Printf("连接建立结束：成功 %d，失败 %d，当前存活 %d", connected, stats.connectFailures.Load(), stats.alive.Load())
	if connected == 0 {
		return fmt.Errorf("没有成功建立任何连接，检查服务地址 %s", cfg.WSURL)
	}

	stats.SetPhase("warmup")
	schedulerCtx, stopSchedulers := context.WithCancel(context.Background())
	defer stopSchedulers()
	for _, room := range rooms {
		go room.runScheduler(schedulerCtx, stats, cfg)
	}

	writers, err := openWriters(cfg.OutDir)
	if err != nil {
		return err
	}
	defer writers.Close()

	summaryStop := make(chan struct{})
	summaryDone := make(chan struct{})
	go func() {
		defer close(summaryDone)
		writeSummaries(summaryStop, stats, cfg, writers)
	}()

	interrupted := false
	if cfg.Warmup > 0 {
		if !sleepCtx(ctx, cfg.Warmup) {
			interrupted = true
		}
	}
	if !interrupted {
		stats.BeginMeasurement()
		stats.SetPhase("measure")
		log.Printf("预热结束，开始正式测量 %s", cfg.Duration)
		if !sleepCtx(ctx, cfg.Duration) {
			interrupted = true
		}
	}

	stats.SetPhase("drain")
	stopSchedulers()
	// 中断时只等一小段，避免用户等不到结果。
	drain := cfg.Drain
	if interrupted && drain > 500*time.Millisecond {
		drain = 500 * time.Millisecond
	}
	sleepCtx(context.Background(), drain)

	stats.SetPhase("final")
	final := stats.Snapshot()
	close(summaryStop)
	<-summaryDone
	writers.WriteRow(final)

	stats.shuttingDown.Store(true)
	for _, conn := range stats.conns {
		conn.Close()
	}

	report := buildReport(cfg, stats, final, interrupted)
	if err := writers.WriteReport(report); err != nil {
		return err
	}
	printFinal(report)
	return nil
}

type resultWriters struct {
	dir     string
	csvFile *os.File
	csv     *csv.Writer
	jsonl   *os.File
}

func openWriters(dir string) (*resultWriters, error) {
	csvFile, err := os.Create(filepath.Join(dir, "summary.csv"))
	if err != nil {
		return nil, err
	}
	jsonl, err := os.Create(filepath.Join(dir, "summary.jsonl"))
	if err != nil {
		csvFile.Close()
		return nil, err
	}
	writer := &resultWriters{dir: dir, csvFile: csvFile, csv: csv.NewWriter(csvFile), jsonl: jsonl}
	writer.csv.Write([]string{
		"time", "phase", "elapsed_s", "connections", "established", "connect_failures",
		"sent", "expected", "received", "missing", "missing_rate", "duplicates",
		"send_per_s", "recv_per_s", "window_send_per_s", "window_recv_per_s", "amplification",
		"p50_ms", "p95_ms", "p99_ms", "max_ms",
		"send_dropped", "write_errors", "read_errors", "disconnects",
		"rate_limited", "unauthenticated", "server_errors", "invalid",
	})
	writer.csv.Flush()
	return writer, nil
}

func (w *resultWriters) Close() {
	w.csv.Flush()
	w.csvFile.Close()
	w.jsonl.Close()
}

func (w *resultWriters) WriteRow(summary Summary) {
	payload, _ := json.Marshal(summary)
	w.jsonl.Write(payload)
	w.jsonl.Write([]byte("\n"))
	w.csv.Write([]string{
		summary.Time, summary.Phase, strconv.FormatFloat(summary.ElapsedSeconds, 'f', 3, 64),
		strconv.FormatInt(summary.Connections, 10), strconv.FormatInt(summary.ConnectionsEstablished, 10),
		strconv.FormatInt(summary.ConnectFailures, 10), strconv.FormatInt(summary.Sent, 10),
		strconv.FormatInt(summary.Expected, 10), strconv.FormatInt(summary.Received, 10),
		strconv.FormatInt(summary.Missing, 10), strconv.FormatFloat(summary.MissingRate, 'f', 6, 64),
		strconv.FormatInt(summary.Duplicates, 10), strconv.FormatFloat(summary.SendPerSecond, 'f', 2, 64),
		strconv.FormatFloat(summary.ReceivePerSecond, 'f', 2, 64),
		strconv.FormatFloat(summary.WindowSendPerSecond, 'f', 2, 64),
		strconv.FormatFloat(summary.WindowReceivePerSecond, 'f', 2, 64),
		strconv.FormatFloat(summary.Amplification, 'f', 3, 64),
		strconv.FormatFloat(summary.Latency.P50MS, 'f', 3, 64),
		strconv.FormatFloat(summary.Latency.P95MS, 'f', 3, 64),
		strconv.FormatFloat(summary.Latency.P99MS, 'f', 3, 64),
		strconv.FormatFloat(summary.Latency.MaxMS, 'f', 3, 64),
		strconv.FormatInt(summary.SendDropped, 10), strconv.FormatInt(summary.WriteErrors, 10),
		strconv.FormatInt(summary.ReadErrors, 10), strconv.FormatInt(summary.Disconnects, 10),
		strconv.FormatInt(summary.RateLimited, 10), strconv.FormatInt(summary.Unauthenticated, 10),
		strconv.FormatInt(summary.ServerErrors, 10), strconv.FormatInt(summary.Invalid, 10),
	})
	w.csv.Flush()
}

func (w *resultWriters) WriteReport(report Report) error {
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(w.dir, "final.json"), payload, 0o644)
}

func writeSummaries(stop <-chan struct{}, stats *Stats, cfg *Config, writers *resultWriters) {
	ticker := time.NewTicker(cfg.SummaryInterval)
	defer ticker.Stop()
	previous := stats.Snapshot()
	previousAt := time.Now()
	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			current := stats.Snapshot()
			seconds := now.Sub(previousAt).Seconds()
			if seconds <= 0 {
				seconds = 1
			}
			current.WindowSendPerSecond = float64(current.Sent-previous.Sent) / seconds
			current.WindowReceivePerSecond = float64(current.Received-previous.Received) / seconds
			writers.WriteRow(current)
			log.Printf("[%s] 存活=%d 发送=%.0f/s 接收=%.0f/s 预期=%d 缺失=%.3f%% 重复=%d P50=%.1fms P95=%.1fms P99=%.1fms 断连=%d 限流=%d 发送丢弃=%d",
				current.Phase, current.Connections, current.WindowSendPerSecond, current.WindowReceivePerSecond,
				current.Expected, current.MissingRate*100, current.Duplicates,
				current.Latency.P50MS, current.Latency.P95MS, current.Latency.P99MS,
				current.Disconnects, current.RateLimited, current.SendDropped)
			previous, previousAt = current, now
		}
	}
}

func printFinal(report Report) {
	totals := report.Totals
	log.Printf("最终结果 run=%s 连接=%d 发送=%d（%.1f/s）投递=%d（%.1f/s）预期=%d 缺失=%d（%.4f%%）重复=%d",
		report.RunID, totals.Connections, totals.Sent, totals.SendPerSecond, totals.Received,
		totals.ReceivePerSecond, totals.Expected, totals.Missing, totals.MissingRate*100, totals.Duplicates)
	log.Printf("端到端延迟 P50=%.1fms P95=%.1fms P99=%.1fms 最大=%.1fms 断连=%d 限流拒绝=%d",
		totals.Latency.P50MS, totals.Latency.P95MS, totals.Latency.P99MS, totals.Latency.MaxMS,
		totals.Disconnects, totals.RateLimited)
	log.Printf("结果目录 %s", report.OutputDir)
}
