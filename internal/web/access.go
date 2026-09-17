package web

import (
	"fmt"
	"mime"
	"net"
	"net/http"
)

// The dashboard is a local administration tool. Host validation also prevents
// DNS rebinding; origin and content-type checks reject cross-site mutations.
func localRequests(port int, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, requestPort, err := net.SplitHostPort(r.Host)
		if err != nil || requestPort != fmt.Sprint(port) || (host != "localhost" && host != "127.0.0.1" && host != "::1") {
			httpJSON(w, http.StatusForbidden, map[string]string{"error": "localhost access required"})
			return
		}
		origin := r.Header.Get("Origin")
		if (origin != "" && origin != "http://"+r.Host) || r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			httpJSON(w, http.StatusForbidden, map[string]string{"error": "same-origin access required"})
			return
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method == http.MethodPost {
			mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || mediaType != "application/json" {
				httpJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "application/json required"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) connectionRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/connect", "/api/test-source", "/api/test-dest":
			s.connectionMu.Lock()
			defer s.connectionMu.Unlock()
			migrateMu.Lock()
			active := migrateActive
			migrateMu.Unlock()
			if active {
				httpJSON(w, http.StatusConflict, map[string]string{"error": "cannot change connections during migration"})
				return
			}
		default:
			s.connectionMu.RLock()
			defer s.connectionMu.RUnlock()
		}
		next.ServeHTTP(w, r)
	})
}
