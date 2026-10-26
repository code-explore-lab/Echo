package store

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"minidanmaku/internal/model"
)

// TestMongoStoreIntegration exercises the real Mongo path. It is skipped
// unless MINIDANMAKU_MONGO_URI is set, so `go test ./...` stays fast offline.
func TestMongoStoreIntegration(t *testing.T) {
	uri := os.Getenv("MINIDANMAKU_MONGO_URI")
	if uri == "" {
		t.Skip("set MINIDANMAKU_MONGO_URI to run the Mongo integration test")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewMongoStore(ctx, uri, "minidanmaku_test", logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close(ctx)

	now := time.Now().UnixMilli()
	docs := []*model.Danmaku{
		{ID: model.NewID(), Room: "it-room", Seq: 1, UID: "u1", Content: "one", Color: "#fff", Size: 24, TS: now - 200},
		{ID: model.NewID(), Room: "it-room", Seq: 2, UID: "u1", Content: "two", Color: "#fff", Size: 24, TS: now - 100},
		{ID: model.NewID(), Room: "it-room", Seq: 3, UID: "u1", Content: "three", Color: "#fff", Size: 24, TS: now},
	}
	for _, d := range docs {
		if err := s.Insert(ctx, d); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Re-inserting the same _id must be idempotent, not an error.
	if err := s.Insert(ctx, docs[2]); err != nil {
		t.Fatalf("idempotent re-insert failed: %v", err)
	}

	items, err := s.Query(ctx, "it-room", 0, 10)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("got %d items, want 3", len(items))
	}
	// Newest first.
	for i := 0; i < len(items)-1; i++ {
		if items[i].TS < items[i+1].TS {
			t.Fatalf("items not sorted newest-first: %v then %v", items[i].TS, items[i+1].TS)
		}
	}

	// Keyset pagination: from = the oldest ts we already saw.
	oldest := items[len(items)-1].TS
	older, err := s.Query(ctx, "it-room", oldest, 10)
	if err != nil {
		t.Fatalf("query(older): %v", err)
	}
	if len(older) != 0 {
		t.Fatalf("expected 0 older items, got %d", len(older))
	}
}
