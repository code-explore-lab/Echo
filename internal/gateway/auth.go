package gateway

import (
	"net/http"
	"strings"
)

// authenticate checks the static shared token from ?token= or an
// Authorization: Bearer header. An empty configured token disables auth
// (the Phase 1 default, suitable for local development).
func (s *Server) authenticate(r *http.Request) bool {
	if s.cfg.Server.AuthToken == "" {
		return true
	}
	token := r.URL.Query().Get("token")
	if token == "" {
		token, _ = strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	return token == s.cfg.Server.AuthToken
}
