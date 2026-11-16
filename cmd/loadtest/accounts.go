package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

// Account 是通过测试库和 Redis 会话注入的登录身份。
// 不走 /auth/register，是因为注册和登录共用每个 IP 每分钟 30 次的限制，
// 数百个发送账号无法在压测开始前通过 HTTP 建立。
type Account struct {
	Username string `json:"username"`
	UserID   string `json:"user_id"`
	Token    string `json:"token"`
}

const sessionNamespace = "golitube"

func sessionKey(token string) string { return sessionNamespace + ":session:" + digest(token) }

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func accountPrefix(runID string) string {
	sum := sha256.Sum256([]byte(runID))
	return "lt" + hex.EncodeToString(sum[:3])
}

func usernameFor(prefix string, index int) string {
	return fmt.Sprintf("%s%05d", prefix, index)
}

// userIDFor 由用户名确定性生成，重复执行 seed 不会产生第二个用户。
func userIDFor(username string) string {
	sum := sha256.Sum256([]byte("golitube-loadtest:" + username))
	return "lt" + hex.EncodeToString(sum[:16])
}

func openTestDB(ctx context.Context, cfg *Config) (*sql.DB, error) {
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		return nil, fmt.Errorf("打开测试库失败: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(2 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("连接测试库失败: %w", err)
	}
	var probe int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&probe); err != nil {
		db.Close()
		return nil, fmt.Errorf("测试库缺少 users 表，请先运行 migrate-history: %w", err)
	}
	return db, nil
}

func openRedis(ctx context.Context, cfg *Config) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DialTimeout: 3 * time.Second,
		ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, ContextTimeoutEnabled: true})
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("连接 Redis 失败: %w", err)
	}
	return rdb, nil
}

// seedAccounts 写入用户行和 Redis 会话，并把令牌保存到结果目录，便于清理。
func seedAccounts(ctx context.Context, cfg *Config, count int) ([]Account, error) {
	if count <= 0 {
		return nil, nil
	}
	db, err := openTestDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rdb, err := openRedis(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer rdb.Close()

	// 账号登录不会被压测使用，MinCost 即可；真正登录仍可验证密码。
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(cfg.Password), bcrypt.MinCost)
	if err != nil {
		return nil, err
	}
	prefix := accountPrefix(cfg.RunID)
	accounts := make([]Account, 0, count)
	for index := 0; index < count; index++ {
		username := usernameFor(prefix, index)
		userID := userIDFor(username)
		if _, err := db.ExecContext(ctx, `INSERT INTO users (id,username,password_hash) VALUES (?,?,?)
			ON DUPLICATE KEY UPDATE id=id`, userID, username, passwordHash); err != nil {
			return nil, fmt.Errorf("写入账号 %q 失败: %w", username, err)
		}
		// 已存在的用户名可能由旧数据生成，以库中 ID 为准，保证会话与 users 表一致。
		var storedID string
		if err := db.QueryRowContext(ctx, "SELECT id FROM users WHERE username = ?", username).Scan(&storedID); err != nil {
			return nil, fmt.Errorf("读取账号 %q 失败: %w", username, err)
		}
		token := rand.Text()
		payload, err := json.Marshal(map[string]string{"id": storedID, "username": username})
		if err != nil {
			return nil, err
		}
		if err := rdb.Set(ctx, sessionKey(token), payload, 24*time.Hour).Err(); err != nil {
			return nil, fmt.Errorf("写入会话失败: %w", err)
		}
		accounts = append(accounts, Account{Username: username, UserID: storedID, Token: token})
	}
	if err := writeAccounts(cfg.OutDir, accounts); err != nil {
		return nil, err
	}
	log.Printf("已注入 %d 个独立账号，用户名前缀 %s*", len(accounts), prefix)
	return accounts, nil
}

func writeAccounts(outDir string, accounts []Account) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(accounts, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(outDir, "accounts.json"), payload, 0o600)
}

func readAccounts(outDir string) ([]Account, error) {
	payload, err := os.ReadFile(filepath.Join(outDir, "accounts.json"))
	if err != nil {
		return nil, err
	}
	var accounts []Account
	if err := json.Unmarshal(payload, &accounts); err != nil {
		return nil, err
	}
	return accounts, nil
}

// cleanupAccounts 删除本次运行注入的账号、会话，避免测试库和 Redis 持续增长。
func cleanupAccounts(ctx context.Context, cfg *Config) error {
	if cfg.MySQLDSN == "" {
		return fmt.Errorf("-cleanup 需要 -mysql-dsn")
	}
	accounts, err := readAccounts(cfg.OutDir)
	if err != nil {
		return fmt.Errorf("读取账号文件失败（-out 应指向原运行目录）: %w", err)
	}
	db, err := openTestDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()
	rdb, err := openRedis(ctx, cfg)
	if err != nil {
		return err
	}
	defer rdb.Close()

	keys := make([]string, 0, len(accounts))
	for _, account := range accounts {
		keys = append(keys, sessionKey(account.Token))
	}
	if len(keys) > 0 {
		if err := rdb.Del(ctx, keys...).Err(); err != nil {
			return err
		}
	}
	prefix := accountPrefix(cfg.RunID)
	result, err := db.ExecContext(ctx, "DELETE FROM users WHERE username LIKE ?", prefix+"%")
	if err != nil {
		return err
	}
	deleted, _ := result.RowsAffected()
	log.Printf("已删除 %d 个会话和 %d 个账号", len(keys), deleted)
	return nil
}
