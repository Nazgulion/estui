package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLocalAccessBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, host, origin, site, content string
		want                              int
	}{
		{"local browser", "localhost:8080", "http://localhost:8080", "same-origin", "application/json", 200},
		{"local CLI", "127.0.0.1:8080", "", "", "application/json", 200},
		{"foreign origin", "localhost:8080", "https://evil.example", "", "application/json", 403},
		{"null origin", "localhost:8080", "null", "", "application/json", 403},
		{"cross-site fetch", "localhost:8080", "", "cross-site", "application/json", 403},
		{"DNS rebinding", "evil.example:8080", "http://evil.example:8080", "same-origin", "application/json", 403},
		{"wrong port", "localhost:8081", "", "", "application/json", 403},
		{"form POST", "localhost:8080", "", "", "text/plain", 415},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reached := false
			h := localRequests(8080, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true; w.WriteHeader(200) }))
			r := httptest.NewRequest("POST", "http://"+tc.host+"/api/indices/delete", strings.NewReader(`{}`))
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("Sec-Fetch-Site", tc.site)
			r.Header.Set("Content-Type", tc.content)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want || reached != (tc.want == 200) {
				t.Fatalf("code=%d reached=%t", w.Code, reached)
			}
			if w.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Fatal("CORS enabled")
			}
		})
	}
	if got := (&Server{Port: 8080}).listenAddress(); got != "127.0.0.1:8080" {
		t.Fatalf("unsafe listen address: %s", got)
	}
}
