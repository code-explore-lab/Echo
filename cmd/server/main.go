// Command server runs the MiniDanmaku gateway: HTTP + WebSocket + async
// MongoDB persistence, all on a single node (Phase 1).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"minidanmaku/internal/config"
	"minidanmaku/internal/gateway"
	"minidanmaku/internal/hub"
	"minidanmaku/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg := config.Load()

	// Signal-driven cancellation: used both for graceful shutdown and as the
	// parent of the store's background index-creation goroutine.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoStore, err := store.NewMongoStore(ctx, cfg.MongoURI, cfg.MongoDB, logger)
	if err != nil {
		logger.Error("failed to init mongo", "err", err)
		os.Exit(1)
	}
	queue := store.NewPersistQueue(mongoStore, cfg.StoreQueue, cfg.StoreWorkers, logger)

	h := hub.NewHub(cfg.DedupSize, cfg.DedupTTL)
	gw := gateway.New(cfg, h, queue, mongoStore, logger)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           gw.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx) // closes the listener; WS conns end on exit
		queue.Shutdown(shutdownCtx)   // drains the persistence queue
		_ = mongoStore.Close(shutdownCtx)
	}()

	logger.Info("listening", "addr", cfg.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", "err", err)
		os.Exit(1)
	}
	logger.Info("stopped")
}
