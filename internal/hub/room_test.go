package hub

import (
	"fmt"
	"testing"
	"time"

	"minidanmaku/internal/model"
)

// newTestClient builds a Client with a nil connection, safe to use for
// exercising trySend/Broadcast without starting the pumps.
func newTestClient(sendBuf int, maxDrops int64) *Client {
	return NewClient(ClientOptions{
		Room:     "100",
		UID:      "u1",
		SendBuf:  sendBuf,
		MaxDrops: maxDrops,
	})
}

func TestIngestAssignsSeqAndDedups(t *testing.T) {
	r := NewRoom("100", 100, time.Minute)
	c := newTestClient(4, 16)

	d1, dup1 := r.Ingest(c, "id-1", "hello", "#fff", 24, 1000)
	if dup1 {
		t.Fatal("first ingest should not be a duplicate")
	}
	if d1.Seq != 1 {
		t.Fatalf("seq = %d, want 1", d1.Seq)
	}
	if d1.ID == "" {
		t.Fatal("expected a generated id")
	}

	// Same idempotency key -> same event, flagged as duplicate.
	d2, dup2 := r.Ingest(c, "id-1", "hello", "#fff", 24, 2000)
	if !dup2 {
		t.Fatal("resent id should be a duplicate")
	}
	if d2.ID != d1.ID || d2.Seq != d1.Seq {
		t.Fatalf("dedup returned a different event: %+v vs %+v", d2, d1)
	}

	d3, dup3 := r.Ingest(c, "id-2", "world", "#fff", 24, 3000)
	if dup3 {
		t.Fatal("id-2 should be new")
	}
	if d3.Seq != 2 {
		t.Fatalf("seq = %d, want 2", d3.Seq)
	}
}

func TestDedupCacheIsBounded(t *testing.T) {
	dc := newDedupCache(3, time.Minute)
	for i := range 100 {
		dc.Add(fmt.Sprintf("k%d", i), &model.Danmaku{ID: fmt.Sprintf("d%d", i)})
	}
	dc.mu.Lock()
	n := len(dc.items)
	dc.mu.Unlock()
	if n > 3 {
		t.Fatalf("cache size = %d, want <= 3", n)
	}
}

func TestBroadcastDropsAndDisconnectsSlowConsumer(t *testing.T) {
	r := NewRoom("100", 100, time.Minute)
	// Buffer 2, disconnect once 2 messages are dropped.
	c := newTestClient(2, 2)
	r.Join(c)

	msg := []byte("x")
	for range 4 {
		r.Broadcast(msg)
	}

	if got := c.dropped.Load(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}
	select {
	case <-c.done:
		// disconnected as expected
	default:
		t.Fatal("slow consumer should have been disconnected")
	}
}

func TestLeaveRemovesMember(t *testing.T) {
	r := NewRoom("100", 100, time.Minute)
	c := newTestClient(4, 16)
	r.Join(c)
	r.Leave(c)

	r.mu.RLock()
	n := len(r.members)
	r.mu.RUnlock()
	if n != 0 {
		t.Fatalf("members = %d, want 0", n)
	}
}
