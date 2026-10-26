// Package hub implements the room fan-out and the per-connection pumps.
package hub

import (
	"sync"
	"time"

	"minidanmaku/internal/model"
)

// Room is the per-room seam. InMemoryRoom is the Phase-1 implementation.
// Phase 2 can drop in a Redis-backed implementation behind this same
// interface (Broadcast becomes pub/sub, Ingest uses SETNX/INCR) without
// touching the client or gateway — this is the multi-instance extension point.
type Room interface {
	Join(c *Client)
	Leave(c *Client)
	// Broadcast fans msg out to members without blocking.
	Broadcast(msg []byte)
	// Ingest dedups by clientMsgID, assigns seq + a fresh id, and returns
	// the event plus whether this was a duplicate resent.
	Ingest(c *Client, clientMsgID, content, color string, size int, ts int64) (*model.Danmaku, bool)
	// Seq returns the room's current sequence number.
	Seq() int64
}

// Hub owns rooms and lazily creates them on first use. Rooms are never
// eagerly reaped in Phase 1: the number of rooms is small and bounded by
// usage, and their empty-member overhead is negligible.
type Hub struct {
	mu        sync.RWMutex
	rooms     map[string]Room
	dedupSize int
	dedupTTL  time.Duration
}

// NewHub creates a hub whose rooms all share the given dedup-cache sizing.
func NewHub(dedupSize int, dedupTTL time.Duration) *Hub {
	return &Hub{
		rooms:     make(map[string]Room),
		dedupSize: dedupSize,
		dedupTTL:  dedupTTL,
	}
}

// Room returns the room for id, creating it on first use.
func (h *Hub) Room(id string) Room {
	h.mu.RLock()
	r, ok := h.rooms[id]
	h.mu.RUnlock()
	if ok {
		return r
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok = h.rooms[id]; ok { // re-check after taking the write lock
		return r
	}
	r = NewRoom(id, h.dedupSize, h.dedupTTL)
	h.rooms[id] = r
	return r
}
