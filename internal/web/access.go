package web

import (
	"fmt"
	"mime"
	"net"
	"net/http"
	"strings"
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
		if r.Method == http.MethodPost {
			s.operationMu.Lock()
			defer s.operationMu.Unlock()
			if err := s.recoverPreview(); err != nil {
				httpJSON(w, 409, map[string]string{"error": err.Error()})
				return
			}
			if strings.HasPrefix(r.URL.Path, "/api/indices/") || r.URL.Path == "/api/preview-query" {
				migrateMu.Lock()
				busy := migrateActive || GetState().Active != nil
				migrateMu.Unlock()
				if busy {
					httpJSON(w, 409, map[string]string{"error": "index actions and previews are blocked until the migration is resolved"})
					return
				}
			}
		}
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

func validateIndexNames(names []string) error {
	if len(names) == 0 {
		return fmt.Errorf("no indices provided")
	}
	for _, name := range names {
		if name == "" || name == "_all" || strings.ContainsAny(name, "*?,/\\<> ") {
			return fmt.Errorf("an exact index name is required: %q", name)
		}
	}
	return nil
}
