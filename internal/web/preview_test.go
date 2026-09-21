package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"estui/internal/config"
	"estui/internal/elastic"
	"estui/internal/logging"
)

type previewBackend struct {
	mu                       sync.Mutex
	closed                   bool
	requests                 []string
	countBody                string
	countStatus              int
	closeStatus              int
	listError                bool
	openError                bool
	keepClosed               bool
	truncateCount            bool
	query                    string
	cancelAt                 string
	cancel                   context.CancelFunc
	waitForCountCancellation bool
}

func (f *previewBackend) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
	path := r.URL.Path
	f.requests = append(f.requests, path)
	switch path {
	case "/":
		_, _ = io.WriteString(w, `{}`)
	case "/_cat/indices":
		if f.listError {
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `{"error":"status forbidden"}`)
			return
		}
		if f.cancelAt == "waiting" && len(f.requests) > 3 {
			f.cancel()
		}
		status := "open"
		if f.closed {
			status = "close"
		}
		_ = json.NewEncoder(w).Encode([]map[string]string{{"index": "test", "status": status}})
	case "/test/_open":
		if f.openError {
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `{"error":"open forbidden"}`)
			return
		}
		if !f.keepClosed {
			f.closed = false
		}
		if f.cancelAt == "opening" {
			f.cancel()
		}
		_, _ = io.WriteString(w, `{"acknowledged":true}`)
	case "/test/_close":
		// The cleanup request must arrive despite cancellation of the preview.
		if err := r.Context().Err(); err != nil {
			panic(err)
		}
		if f.closeStatus != 0 {
			w.WriteHeader(f.closeStatus)
			_, _ = io.WriteString(w, `{"error":"close forbidden"}`)
			return
		}
		f.closed = true
		_, _ = io.WriteString(w, `{"acknowledged":true}`)
	case "/test/_count":
		body, _ := io.ReadAll(r.Body)
		f.query = string(body)
		if f.cancelAt == "counting" {
			f.cancel()
		}
		if f.cancelAt == "counting" || f.waitForCountCancellation {
			<-r.Context().Done()
			return
		}
		if f.truncateCount {
			w.Header().Set("Content-Length", "100")
		}
		if f.countStatus != 0 {
			w.WriteHeader(f.countStatus)
		}
		_, _ = io.WriteString(w, f.countBody)
	default:
		w.WriteHeader(404)
	}
}

func newPreviewServer(t *testing.T, f *previewBackend) *Server {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(backend.Close)
	log := logging.New(filepath.Join(t.TempDir(), "preview.log"), logging.ERROR)
	t.Cleanup(func() { _ = log.Close() })
	cfg := config.ESConnection{Hosts: []string{backend.URL}}
	client, err := elastic.NewClient(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	return &Server{Source: client, Target: client, Log: log}
}

func requestPreview(s *Server, ctx context.Context) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/api/preview-query", strings.NewReader(`{"host":"source","index":"test","query":"status:active"}`)).WithContext(ctx)
	w := httptest.NewRecorder()
	s.handlePreviewQuery(w, r)
	return w
}

func TestPreviewRestoresOnlyOriginallyClosedIndex(t *testing.T) {
	for _, closed := range []bool{true, false} {
		t.Run(fmt.Sprintf("closed=%t", closed), func(t *testing.T) {
			f := &previewBackend{closed: closed, countBody: `{"count":0,"_shards":{"failed":0}}`}
			s := newPreviewServer(t, f)
			w := requestPreview(s, context.Background())
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"count":0`) {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.closed != closed {
				t.Fatal("index state was not restored")
			}
			if !strings.Contains(f.query, `"query_string":{"query":"status:active"}`) {
				t.Fatalf("query lost: %s", f.query)
			}
			want := "/,/_cat/indices,/test/_count"
			if closed {
				want = "/,/_cat/indices,/test/_open,/_cat/indices,/test/_count,/test/_close"
			}
			if got := strings.Join(f.requests, ","); got != want {
				t.Fatalf("sequence=%s want=%s", got, want)
			}
		})
	}
}

func TestPreviewReportsQueryFailuresAndStillCloses(t *testing.T) {
	for _, tc := range []struct {
		name, body, message string
		status              int
		truncate            bool
	}{
		{"Lucene syntax", `{"error":{"reason":"Failed to parse query"}}`, "Failed to parse query", 400, false},
		{"permission denied", `{"error":"count forbidden"}`, "count forbidden", 403, false},
		{"malformed response", `{`, "parse query preview", 200, false},
		{"missing count", `{}`, "missing a valid count", 200, false},
		{"null count", `{"count":null}`, "missing a valid count", 200, false},
		{"partial shards", `{"count":2,"_shards":{"failed":1}}`, "shards failed", 200, false},
		{"truncated body", `{"count":1}`, "read query preview", 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &previewBackend{closed: true, countStatus: tc.status, countBody: tc.body, truncateCount: tc.truncate}
			s := newPreviewServer(t, f)
			w := requestPreview(s, context.Background())
			if w.Code != 502 || !strings.Contains(w.Body.String(), tc.message) || strings.Contains(w.Body.String(), `"count":0`) {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if !f.closed || f.requests[len(f.requests)-1] != "/test/_close" {
				t.Fatal("failed preview did not restore closed state")
			}
		})
	}
}

func TestPreviewCancellationStillCloses(t *testing.T) {
	for _, stage := range []string{"opening", "waiting", "counting"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f := &previewBackend{closed: true, countBody: `{"count":1}`, cancelAt: stage, cancel: cancel, keepClosed: stage == "waiting"}
			s := newPreviewServer(t, f)
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- requestPreview(s, ctx) }()
			select {
			case w := <-done:
				if w.Code == 200 {
					t.Fatalf("cancelled preview reported success: %s", w.Body.String())
				}
			case <-time.After(3 * time.Second):
				t.Fatal("cancelled preview failed to return promptly")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if !f.closed || f.requests[len(f.requests)-1] != "/test/_close" {
				t.Fatalf("cleanup missed after %s: %v", stage, f.requests)
			}
		})
	}
}

func TestPreviewDeadlineStillCloses(t *testing.T) {
	f := &previewBackend{closed: true, waitForCountCancellation: true}
	s := newPreviewServer(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	w := requestPreview(s, ctx)
	if w.Code != 504 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed || f.requests[len(f.requests)-1] != "/test/_close" {
		t.Fatal("deadline prevented cleanup")
	}
}

func TestPreviewReportsCloseFailure(t *testing.T) {
	for _, queryFails := range []bool{true, false} {
		t.Run(fmt.Sprintf("queryFails=%t", queryFails), func(t *testing.T) {
			f := &previewBackend{closed: true, countBody: `{"count":3}`, closeStatus: 403}
			if queryFails {
				f.countStatus = 400
				f.countBody = `{"error":"query invalid"}`
			}
			s := newPreviewServer(t, f)
			w := requestPreview(s, context.Background())
			if w.Code != 502 || !strings.Contains(w.Body.String(), "failed to restore closed index") {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if queryFails && !strings.Contains(w.Body.String(), "query invalid") {
				t.Fatal("close failure hid query failure")
			}
		})
	}
}

func TestPreviewStopsWhenStateCannotBeVerifiedOrOpened(t *testing.T) {
	for _, failAt := range []string{"status", "open"} {
		t.Run(failAt, func(t *testing.T) {
			f := &previewBackend{closed: true, listError: failAt == "status", openError: failAt == "open"}
			s := newPreviewServer(t, f)
			if w := requestPreview(s, context.Background()); w.Code == 200 {
				t.Fatal("failure reported success")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			for _, path := range f.requests {
				if path == "/test/_count" {
					t.Fatal("count issued after prerequisite failed")
				}
			}
		})
	}
}
