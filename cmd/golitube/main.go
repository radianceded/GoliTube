package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/radianceded/GoliTube/internal/danmaku"
	"github.com/radianceded/GoliTube/internal/persistence"
	"github.com/radianceded/GoliTube/internal/server"
)

func main() {
	addr := flag.String("addr", "localhost:8081", "HTTP 服务监听地址")
	redisAddr := flag.String("redis-addr", "127.0.0.1:6379", "Redis 服务地址")
	mode := flag.String("mode", "server", "运行模式：server、persist 或 migrate-history")
	brokers := flag.String("kafka-brokers", "", "Kafka 地址，逗号分隔；server 留空时仅实时广播")
	batchSize := flag.Int("persist-batch", 100, "persist 模式每批消息数，1～1000；最多等待 50ms")
	flag.Parse()

	// systemd 停止服务默认发送 SIGTERM，开发时的 Ctrl+C 是 SIGINT。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch *mode {
	case "server":
		if *brokers == "" {
			err = server.Run(ctx, *addr, *redisAddr, nil)
		} else {
			writer := persistence.NewEventWriter(strings.Split(*brokers, ","))
			err = server.Run(ctx, *addr, *redisAddr, func(ctx context.Context, event danmaku.RoomEvent) error {
				return persistence.EnqueueRoomEvent(ctx, writer, event)
			})
			writer.Close()
		}
	case "persist":
		err = persistence.Run(ctx, strings.Split(*brokers, ","), os.Getenv("GOLITUBE_MYSQL_DSN"), *redisAddr, *batchSize)
	case "migrate-history":
		err = persistence.RunBackfill(ctx, os.Getenv("GOLITUBE_MYSQL_DSN"))
	default:
		err = fmt.Errorf("未知运行模式 %q", *mode)
	}
	if err != nil {
		log.Fatal(err)
	}
}
