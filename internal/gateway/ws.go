package gateway

import (
	"net/http"

	"golang.org/x/time/rate"

	"minidanmaku/internal/hub"
	"minidanmaku/internal/model"
)

// handleWS authenticates, upgrades to WebSocket, and wires a hub.Client.
// The room is fixed per connection (from ?room=); a client that wants another
// room opens another connection.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if !s.authenticate(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	room := r.URL.Query().Get("room")
	if room == "" {
		http.Error(w, "missing room", http.StatusBadRequest)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Debug("upgrade failed", "err", err)
		return
	}

	limiter := rate.NewLimiter(rate.Limit(s.cfg.RateLimit), s.cfg.RateBurst)
	client := hub.NewClient(hub.ClientOptions{
		Room:       room,
		UID:        model.ShortID(),
		Conn:       conn,
		RoomRef:    s.hub.Room(room),
		Persist:    s.enqueuer,
		Limiter:    limiter,
		SendBuf:    s.cfg.ClientSendBuf,
		PingPeriod: s.cfg.PingPeriod,
		PongWait:   s.cfg.PongWait,
		WriteWait:  s.cfg.WriteWait,
		MaxMsgSize: s.cfg.MaxMsgSize,
		MaxDrops:   s.cfg.MaxDrops,
		Logger:     s.log,
	})
	client.Start()
}
