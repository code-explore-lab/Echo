// Package config loads runtime configuration from environment variables,
// falling back to sensible defaults so the server runs with zero config.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds every tunable knob for the MiniDanmaku server.
type Config struct {
	Addr      string
	MongoURI  string
	MongoDB   string
	AuthToken string // empty disables auth (Phase 1)

	// Buffering / backpressure.
	ClientSendBuf int // per-connection outbound buffer (bounded channel)
	StoreQueue    int // bounded persistence queue capacity
	StoreWorkers  int // goroutines consuming the persistence queue

	// Per-connection rate limiting.
	RateLimit float64 // messages per second
	RateBurst int     // burst size

	// Heartbeat / connection liveness.
	PingPeriod time.Duration
	PongWait   time.Duration
	WriteWait  time.Duration
	MaxMsgSize int64

	// Dedup cache for resent client messages.
	DedupSize int
	DedupTTL  time.Duration

	// Drops before a persistently-slow consumer is disconnected.
	MaxDrops int64
}

// Load reads the environment and returns a Config with defaults applied.
func Load() Config {
	return Config{
		Addr:          envStr("ADDR", ":8080"),
		MongoURI:      envStr("MONGO_URI", "mongodb://localhost:27017"),
		MongoDB:       envStr("MONGO_DB", "minidanmaku"),
		AuthToken:     envStr("AUTH_TOKEN", ""),

		ClientSendBuf: envInt("CLIENT_SEND_BUF", 256),
		StoreQueue:    envInt("STORE_QUEUE", 1024),
		StoreWorkers:  envInt("STORE_WORKERS", 4),

		RateLimit: envFloat("RATE_LIMIT", 10),
		RateBurst: envInt("RATE_BURST", 20),

		PingPeriod: envDuration("PING_PERIOD", 30*time.Second),
		PongWait:   envDuration("PONG_WAIT", 60*time.Second),
		WriteWait:  envDuration("WRITE_WAIT", 10*time.Second),
		MaxMsgSize: int64(envInt("MAX_MSG_SIZE", 4096)),

		DedupSize: envInt("DEDUP_SIZE", 10000),
		DedupTTL:  envDuration("DEDUP_TTL", 60*time.Second),

		MaxDrops: int64(envInt("MAX_DROPS", 16)),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
