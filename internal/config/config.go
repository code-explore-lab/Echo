// Package config loads runtime configuration from environment variables,
// falling back to sensible defaults so the server runs with zero config.
//
// The flat set of knobs is grouped into sub-configs by concern: Server (HTTP
// listener + gateway auth), Mongo (persistence backend), Client (per-connection
// behavior), Store (async persistence queue) and Hub (room dedup).
package config

import (
	"os"
	"strconv"
	"time"
)

// Config is the root configuration.
type Config struct {
	Server ServerConfig // HTTP listener + gateway auth
	Mongo  MongoConfig  // persistence backend
	Client ClientConfig // per-connection behavior (buffer, rate, heartbeat)
	Store  StoreConfig  // async persistence queue
	Hub    HubConfig    // room hub dedup / slow-consumer policy
}

// ServerConfig configures the HTTP listener and gateway authentication.
type ServerConfig struct {
	Addr      string
	AuthToken string // empty disables auth (Phase 1)
}

// MongoConfig configures the MongoDB connection.
type MongoConfig struct {
	URI string
	DB  string
}

// ClientConfig configures per-connection behavior: the outbound buffer, the
// rate limiter, the heartbeat liveness windows and the slow-consumer policy.
type ClientConfig struct {
	SendBuf    int // per-connection outbound buffer (bounded channel)
	RateLimit  float64
	RateBurst  int
	PingPeriod time.Duration
	PongWait   time.Duration
	WriteWait  time.Duration
	MaxMsgSize int64
	MaxDrops   int64 // drops before a persistently-slow consumer is disconnected
}

// StoreConfig configures the async write-behind persistence queue.
type StoreConfig struct {
	QueueSize int
	Workers   int
}

// HubConfig configures the per-room dedup cache.
type HubConfig struct {
	DedupSize int
	DedupTTL  time.Duration
}

// Load reads the environment and returns a Config with defaults applied.
func Load() Config {
	return Config{
		Server: ServerConfig{
			Addr:      envStr("ADDR", ":8080"),
			AuthToken: envStr("AUTH_TOKEN", ""),
		},
		Mongo: MongoConfig{
			URI: envStr("MONGO_URI", "mongodb://localhost:27017"),
			DB:  envStr("MONGO_DB", "minidanmaku"),
		},
		Client: ClientConfig{
			SendBuf:    envInt("CLIENT_SEND_BUF", 256),
			RateLimit:  envFloat("RATE_LIMIT", 10),
			RateBurst:  envInt("RATE_BURST", 20),
			PingPeriod: envDuration("PING_PERIOD", 30*time.Second),
			PongWait:   envDuration("PONG_WAIT", 60*time.Second),
			WriteWait:  envDuration("WRITE_WAIT", 10*time.Second),
			MaxMsgSize: int64(envInt("MAX_MSG_SIZE", 4096)),
			MaxDrops:   int64(envInt("MAX_DROPS", 16)),
		},
		Store: StoreConfig{
			QueueSize: envInt("STORE_QUEUE", 1024),
			Workers:   envInt("STORE_WORKERS", 4),
		},
		Hub: HubConfig{
			DedupSize: envInt("DEDUP_SIZE", 10000),
			DedupTTL:  envDuration("DEDUP_TTL", 60*time.Second),
		},
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
