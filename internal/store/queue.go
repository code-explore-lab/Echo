package store

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"minidanmaku/internal/model"
)

// PersistQueue is a bounded async write-behind queue. It decouples the
// low-latency broadcast path from MongoDB I/O: writer goroutines drain the
// queue into the store, and when the queue is full a message is dropped and
// counted — never silently claimed as persisted.
type PersistQueue struct {
	ch     chan *model.Danmaku
	store  Store
	log    *slog.Logger
	cancel context.CancelFunc

	wg sync.WaitGroup

	Dropped atomic.Int64 // queue overflow + failed writes
	Written atomic.Int64 // successful writes
}

// NewPersistQueue creates the queue and starts `workers` writer goroutines.
func NewPersistQueue(s Store, capacity, workers int, log *slog.Logger) *PersistQueue {
	q := &PersistQueue{
		ch:    make(chan *model.Danmaku, capacity),
		store: s,
		log:   log,
	}
	ctx, cancel := context.WithCancel(context.Background())
	q.cancel = cancel
	for range workers {
		q.wg.Add(1)
		go q.worker(ctx)
	}
	return q
}

// Enqueue hands d to the queue without blocking. It returns false (and counts
// a drop) when the queue is full.
func (q *PersistQueue) Enqueue(d *model.Danmaku) bool {
	select {
	case q.ch <- d:
		return true
	default:
		if n := q.Dropped.Add(1); n == 1 || n%100 == 0 {
			q.log.Warn("persist queue full, dropping", "dropped", n)
		}
		return false
	}
}

// Shutdown stops the writers and waits for them to drain and exit.
func (q *PersistQueue) Shutdown(ctx context.Context) {
	q.cancel()
	done := make(chan struct{})
	go func() { q.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (q *PersistQueue) worker(ctx context.Context) {
	defer q.wg.Done()
	for {
		select {
		case d := <-q.ch:
			if err := q.insertWithRetry(ctx, d); err != nil {
				q.Dropped.Add(1)
				q.log.Error("persist failed", "id", d.ID, "room", d.Room, "err", err)
			} else {
				q.Written.Add(1)
			}
		case <-ctx.Done():
			q.drain()
			return
		}
	}
}

// drain flushes anything still queued on shutdown, best-effort. Each worker
// calls it on shutdown; the channel hands each message to exactly one drainer.
func (q *PersistQueue) drain() {
	for {
		select {
		case d := <-q.ch:
			if err := q.insertWithRetry(context.Background(), d); err != nil {
				q.Dropped.Add(1)
				q.log.Error("persist failed on drain", "id", d.ID, "err", err)
			} else {
				q.Written.Add(1)
			}
		default:
			return
		}
	}
}

// insertWithRetry retries transient errors a bounded number of times. The
// store treats duplicate _id as success, so retries are idempotent.
func (q *PersistQueue) insertWithRetry(ctx context.Context, d *model.Danmaku) error {
	var err error
	for attempt := range 3 {
		if err = q.store.Insert(ctx, d); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
	}
	return err
}
