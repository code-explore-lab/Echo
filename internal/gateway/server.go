// Package gateway exposes the HTTP endpoints: the WebSocket upgrade, a
// health check, and the REST history query.
package gateway

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gorilla/websocket"

	"minidanmaku/internal/config"
	"minidanmaku/internal/hub"
	"minidanmaku/internal/store"
)

const maxHistoryLimit = 500

// Server ties the HTTP handlers to the hub and the persistence path.
type Server struct {
	cfg      config.Config
	hub      *hub.Hub
	enqueuer hub.Persister
	store    store.Store
	log      *slog.Logger
	upgrader websocket.Upgrader
}

// New builds a Server. enq is the async persistence queue (satisfying
// hub.Persister); st is the history/query store.
func New(cfg config.Config, h *hub.Hub, enq hub.Persister, st store.Store, log *slog.Logger) *Server {
	return &Server{
		cfg:      cfg,
		hub:      h,
		enqueuer: enq,
		store:    st,
		log:      log,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// Phase 1: allow browser clients from any origin. Tighten this
			// (same-origin or an explicit allowlist) before exposing it.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

// Handler returns the routed http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /ws", s.handleWS)
	mux.HandleFunc("GET /history", s.handleHistory)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleHistory serves persisted danmaku: GET /history?room=100&from=<ts>&limit=50.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	room := r.URL.Query().Get("room")
	if room == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing room"})
		return
	}

	from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > maxHistoryLimit {
				n = maxHistoryLimit
			}
			limit = n
		}
	}

	items, err := s.store.Query(r.Context(), room, from, limit)
	if err != nil {
		s.log.Error("history query failed", "room", room, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"room": room, "items": items})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
