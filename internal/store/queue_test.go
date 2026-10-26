package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"minidanmaku/internal/model"
)

type fakeStore struct {
	inserts atomic.Int64
	failN   int // fail the next N inserts before succeeding
}

func (f *fakeStore) Insert(_ context.Context, _ *model.Danmaku) error {
	if f.failN > 0 {
		f.failN--
		return errors.New("boom")
	}
	f.inserts.Add(1)
	return nil
}

func (f *fakeStore) Query(_ context.Context, _ string, _ int64, _ int) ([]*model.Danmaku, error) {
	return nil, nil
}

func (f *fakeStore) Close(_ context.Context) error { return nil }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestEnqueueOverflowDrops(t *testing.T) {
	q := NewPersistQueue(&fakeStore{}, 1, 0, discardLogger()) // capacity 1, no workers

	if !q.Enqueue(&model.Danmaku{ID: "a"}) {
		t.Fatal("first enqueue should succeed")
	}
	if q.Enqueue(&model.Danmaku{ID: "b"}) {
		t.Fatal("second enqueue should drop (queue full)")
	}
	if q.Dropped.Load() != 1 {
		t.Fatalf("dropped = %d, want 1", q.Dropped.Load())
	}
}

func TestWorkerWritesAndRetries(t *testing.T) {
	fs := &fakeStore{failN: 1} // first insert fails, then succeeds
	q := NewPersistQueue(fs, 10, 1, discardLogger())
	q.Enqueue(&model.Danmaku{ID: "a"})

	deadline := time.Now().Add(2 * time.Second)
	for fs.inserts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if fs.inserts.Load() != 1 {
		t.Fatalf("inserts = %d, want 1 (after retry)", fs.inserts.Load())
	}
	if q.Written.Load() != 1 {
		t.Fatalf("written = %d, want 1", q.Written.Load())
	}
	q.Shutdown(context.Background())
}

func TestWorkerCountsFailedWritesAsDropped(t *testing.T) {
	fs := &fakeStore{failN: 100} // always fails within the retry budget
	q := NewPersistQueue(fs, 10, 1, discardLogger())
	q.Enqueue(&model.Danmaku{ID: "a"})

	deadline := time.Now().Add(2 * time.Second)
	for q.Dropped.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if q.Dropped.Load() == 0 {
		t.Fatal("failed write should be counted as dropped")
	}
	q.Shutdown(context.Background())
}
