package hub

import (
	"sync"
	"time"

	"minidanmaku/internal/model"
)

// InMemoryRoom holds the connected clients of one room plus the per-room
// message-pipeline state: a monotonic sequence and a dedup cache.
type InMemoryRoom struct {
	mu      sync.RWMutex
	id      string
	members map[*Client]struct{}
	seq     int64
	recent  *dedupCache
}

// NewRoom builds an empty room with the given dedup-cache sizing.
func NewRoom(id string, dedupSize int, dedupTTL time.Duration) *InMemoryRoom {
	return &InMemoryRoom{
		id:      id,
		members: make(map[*Client]struct{}),
		recent:  newDedupCache(dedupSize, dedupTTL),
	}
}

// Join registers a client as a room member.
func (r *InMemoryRoom) Join(c *Client) {
	r.mu.Lock()
	r.members[c] = struct{}{}
	r.mu.Unlock()
}

// Leave removes a client from the room.
func (r *InMemoryRoom) Leave(c *Client) {
	r.mu.Lock()
	delete(r.members, c)
	r.mu.Unlock()
}

// Broadcast fans msg out to every member without blocking. A member whose
// outbound buffer is full simply drops the message (see Client.trySend);
// a persistently-full member is disconnected there.
func (r *InMemoryRoom) Broadcast(msg []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for c := range r.members {
		c.trySend(msg)
	}
}

// Ingest dedups by clientMsgID and, for new messages, assigns the next
// sequence number and a fresh globally-unique id. The returned bool is true
// when the id was already seen, in which case the caller should ack but not
// re-broadcast or re-enqueue.
func (r *InMemoryRoom) Ingest(c *Client, clientMsgID, content, color string, size int, ts int64) (*model.Danmaku, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if d, ok := r.recent.Get(clientMsgID); ok {
		return d, true
	}

	r.seq++
	d := &model.Danmaku{
		ID:      model.NewID(),
		Room:    r.id,
		Seq:     r.seq,
		UID:     c.uid,
		Content: content,
		Color:   color,
		Size:    size,
		TS:      ts,
	}
	r.recent.Add(clientMsgID, d)
	return d, false
}

// Seq returns the room's current (highest assigned) sequence number.
func (r *InMemoryRoom) Seq() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.seq
}
