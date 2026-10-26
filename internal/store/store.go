// Package store owns the persistence path: an async write-behind queue and
// the MongoDB-backed Store it drains into. It is deliberately decoupled from
// the low-latency broadcast path in package hub.
package store

import (
	"context"

	"minidanmaku/internal/model"
)

// Store persists danmaku and serves history queries.
type Store interface {
	Insert(ctx context.Context, d *model.Danmaku) error
	// Query returns up to `limit` danmaku in `room` with ts < `from`,
	// ordered newest-first. `from` <= 0 means "no lower bound" (latest).
	Query(ctx context.Context, room string, from int64, limit int) ([]*model.Danmaku, error)
	Close(ctx context.Context) error
}
