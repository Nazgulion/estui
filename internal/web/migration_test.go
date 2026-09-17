package web

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"estui/internal/config"
	"estui/internal/elastic"
	"estui/internal/logging"
)

type fakeCluster struct {
	mu                                           sync.Mutex
	uuid                                         string
	indices                                      map[string]bool
	documents                                    map[string]string
	requests                                     []string
	query                                        string
	failCreate, failDelete, failBulk, failDetail bool
	reindexCode                                  int
	taskBody                                     string
	taskCode                                     int
}

func (f *fakeCluster) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
	path := r.URL.Path
	f.requests = append(f.requests, r.Method+" "+path)
	write := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	switch {
	case path == "/":
		write(map[string]string{"cluster_name": f.uuid, "cluster_uuid": f.uuid})
	case path == "/_cat/indices":
		list := []map[string]string{}
		for name := range f.indices {
			list = append(list, map[string]string{"index": name, "status": "open", "docs.count": "1"})
		}
		write(list)
	case strings.HasSuffix(path, "/_count") || path == "/_count":
		write(map[string]int{"count": 1})
	case path == "/_reindex":
		if f.reindexCode != 0 {
			w.WriteHeader(f.reindexCode)
			write(map[string]string{"error": "reindex unavailable"})
			return
		}
		// Simulate a document already copied by a native task before it fails.
		f.documents["tenant/original"] = `{"v":1}`
		write(map[string]string{"task": "task:1"})
	case strings.HasPrefix(path, "/_tasks/"):
		if f.taskCode != 0 {
			w.WriteHeader(f.taskCode)
		}
		if f.taskBody != "" {
			_, _ = io.WriteString(w, f.taskBody)
		} else {
			_, _ = io.WriteString(w, `{"completed":true,"task":{"status":{"created":1,"total":1}},"response":{"failures":[]}}`)
		}
	case path == "/_search/scroll":
		if r.Method == "DELETE" {
			write(map[string]bool{"succeeded": true})
			return
		}
		_, _ = io.WriteString(w, `{"_scroll_id":"last","hits":{"hits":[]}}`)
	case strings.HasSuffix(path, "/_search"):
		raw, _ := io.ReadAll(r.Body)
		f.query = string(raw)
		_, _ = io.WriteString(w, `{"_scroll_id":"first","hits":{"hits":[{"_id":"original","_routing":"tenant","_source":{"v":1}}]}}`)
	case strings.HasSuffix(path, "/_bulk"):
		if f.failBulk {
			w.WriteHeader(400)
			write(map[string]string{"error": "bulk rejected"})
			return
		}
		raw, _ := io.ReadAll(r.Body)
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		var meta map[string]map[string]string
		_ = json.Unmarshal([]byte(lines[0]), &meta)
		f.documents[meta["index"]["routing"]+"/"+meta["index"]["_id"]] = lines[1]
		_, _ = io.WriteString(w, `{"errors":false,"items":[{"index":{"status":201}}]}`)
	case strings.HasSuffix(path, "/_close"), strings.HasSuffix(path, "/_open"):
		write(map[string]bool{"acknowledged": true})
	case strings.HasPrefix(path, "/_"):
		write(map[string]any{})
	default:
		name := strings.TrimPrefix(path, "/")
		switch r.Method {
		case "GET":
			if f.failDetail {
				w.WriteHeader(400)
				write(map[string]string{"error": "detail rejected"})
				return
			}
			write(map[string]any{name: map[string]any{"settings": map[string]any{"index": map[string]string{"uuid": "index-" + name}}, "mappings": map[string]any{}}})
		case "PUT":
			if f.failCreate {
				w.WriteHeader(400)
				write(map[string]string{"error": "create rejected"})
				return
			}
			f.indices[name] = true
			write(map[string]bool{"acknowledged": true})
		case "DELETE":
			if f.failDelete {
				w.WriteHeader(403)
				write(map[string]string{"error": "delete rejected"})
				return
			}
			delete(f.indices, name)
			write(map[string]bool{"acknowledged": true})
		}
	}
}
func testServer(t *testing.T) (*Server, *fakeCluster, *fakeCluster) {
	t.Helper()
	migrateMu.Lock()
	migrateActive = false
	migrateJobs = nil
	migrateMu.Unlock()
	stateMu.Lock()
	state = MigrationState{}
	stateMu.Unlock()
	log := logging.New(filepath.Join(t.TempDir(), "test.log"), logging.ERROR)
	t.Cleanup(func() { _ = log.Close() })
	a := &fakeCluster{uuid: "source", indices: map[string]bool{"test": true}, documents: map[string]string{}}
	b := &fakeCluster{uuid: "dest", indices: map[string]bool{}, documents: map[string]string{}}
	makeClient := func(f *fakeCluster) (*elastic.Client, config.ESConnection) {
		backend := httptest.NewServer(http.HandlerFunc(f.serve))
		t.Cleanup(backend.Close)
		cfg := config.ESConnection{Hosts: []string{backend.URL}}
		client, err := elastic.NewClient(cfg, log)
		if err != nil {
			t.Fatal(err)
		}
		return client, cfg
	}
	src, srcCfg := makeClient(a)
	dst, dstCfg := makeClient(b)
	s := New(src, dst, srcCfg.Hosts[0], dstCfg.Hosts[0], srcCfg, dstCfg, "", log, 8080)
	s.StatePath = filepath.Join(t.TempDir(), "state.json")
	return s, a, b
}
func call(s *Server, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "http://localhost:8080"+path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, r)
	return w
}
func waitMigration(t *testing.T, s *Server) MigrationState {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		migrateMu.Lock()
		active := migrateActive
		migrateMu.Unlock()
		if !active {
			break
		}
		// Exercise concurrent snapshots and JSON encoding while the worker writes.
		w := httptest.NewRecorder()
		s.handleMigrateState(w, httptest.NewRequest("GET", "/", nil))
		select {
		case <-deadline:
			t.Fatal("migration did not finish (possible deadlock)")
		case <-time.After(time.Millisecond):
		}
	}
	raw, err := os.ReadFile(s.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var saved MigrationState
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	return saved
}
func TestSameClusterRejectedBeforeMutation(t *testing.T) {
	for _, direction := range []string{"source->target", "target->source"} {
		t.Run(direction, func(t *testing.T) {
			s, a, b := testServer(t)
			b.uuid = a.uuid
			w := call(s, "/api/migrate", `{"indices":["test"],"direction":"`+direction+`"}`)
			if w.Code != 400 || !strings.Contains(w.Body.String(), "same cluster") {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			for _, f := range []*fakeCluster{a, b} {
				for _, r := range f.requests {
					if strings.HasPrefix(r, "DELETE ") || strings.HasPrefix(r, "PUT ") {
						t.Fatalf("unsafe mutation: %s", r)
					}
				}
			}
		})
	}
}
func TestMigrationPersistsEveryOutcome(t *testing.T) {
	for _, mode := range []string{"filtered proxy", "initial fallback", "partial native fallback", "native", "skipped", "create failure", "bulk failure", "detail failure", "delete failure", "unknown start", "poll failure"} {
		t.Run(mode, func(t *testing.T) {
			s, a, b := testServer(t)
			force := false
			overwrite := true
			want := "completed"
			switch mode {
			case "filtered proxy":
				force = true
			case "initial fallback":
				b.reindexCode = 400
			case "partial native fallback":
				b.taskBody = `{"completed":true,"task":{"status":{"created":1,"total":2}},"error":{"reason":"remote disconnected"}}`
			case "skipped":
				b.indices["test"] = true
				overwrite = false
				want = "skipped"
			case "create failure":
				b.failCreate = true
				want = "failed"
			case "bulk failure":
				force = true
				b.failBulk = true
				want = "failed"
			case "detail failure":
				a.failDetail = true
				want = "failed"
			case "delete failure":
				b.indices["test"] = true
				b.failDelete = true
				want = "failed"
			case "unknown start":
				b.reindexCode = 500
				want = "failed"
			case "poll failure":
				b.taskCode = 404
				b.taskBody = `{"error":"unavailable"}`
				want = "failed"
			}
			raw, _ := json.Marshal(map[string]any{"indices": []string{"test"}, "query": "status:active", "force_scroll": force, "overwrite": overwrite})
			if w := call(s, "/api/migrate", string(raw)); w.Code != 200 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			saved := waitMigration(t, s)
			var idx IndexState
			if want == "failed" {
				if saved.Active == nil {
					t.Fatal("failed migration archived as success")
				}
				idx = saved.Active.Indices[0]
				if idx.Error == "" {
					t.Fatal("failure reason not persisted")
				}
			} else {
				if saved.Active != nil || len(saved.History) != 1 {
					t.Fatalf("not archived: %+v", saved)
				}
				idx = saved.History[0].Indices[0]
			}
			if idx.Status != want || len(idx.Logs) == 0 {
				t.Fatalf("state=%+v want %s", idx, want)
			}
			if want == "completed" && idx.Transferred != 1 {
				t.Fatalf("progress lost: %+v", idx)
			}
			if mode == "partial native fallback" && len(b.documents) != 1 {
				t.Fatalf("fallback duplicated native copy: %+v", b.documents)
			}
			if mode == "filtered proxy" && !strings.Contains(a.query, "status:active") {
				t.Fatal("query was lost")
			}
			if mode == "poll failure" && idx.TaskID != "task:1" {
				t.Fatal("native task ID not persisted")
			}
			if mode == "unknown start" && !idx.ReindexUncertain {
				t.Fatal("uncertain start not recorded")
			}
			if mode == "poll failure" || mode == "unknown start" {
				for _, r := range a.requests {
					if strings.Contains(r, "/_search") {
						t.Fatal("unsafe concurrent fallback")
					}
				}
				if w := call(s, "/api/migrate/continue", `{}`); w.Code != 409 {
					t.Fatalf("unsafe continue accepted: %s", w.Body.String())
				}
			}
		})
	}
}
func TestResumePreservesOverwriteAndSkippedIndices(t *testing.T) {
	s, a, b := testServer(t)
	a.indices["existing"] = true
	b.indices["existing"] = true
	b.failCreate = true
	w := call(s, "/api/migrate", `{"indices":["existing","test"],"overwrite":false,"force_scroll":true}`)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	first := waitMigration(t, s)
	if first.Active == nil || first.Active.Overwrite == nil || *first.Active.Overwrite || first.Active.Indices[0].Status != "skipped" {
		t.Fatalf("bad saved state: %+v", first)
	}
	// Reload the serialized checkpoint, as on restart.
	if err := LoadState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.failCreate = false
	b.mu.Unlock()
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	saved := waitMigration(t, s)
	if saved.Active != nil || saved.History[0].Indices[0].Status != "skipped" || saved.History[0].Indices[1].Status != "completed" {
		t.Fatalf("resume failed: %+v", saved)
	}
	for _, r := range b.requests {
		if strings.HasPrefix(r, "DELETE ") {
			t.Fatalf("resume deleted preserved destination: %s", r)
		}
	}
}
func TestResumeChecksClusterIdentity(t *testing.T) {
	s, _, b := testServer(t)
	SetActiveMigration(&ActiveMigration{Source: ConnectionState{ClusterUUID: "source"}, Dest: ConnectionState{ClusterUUID: "original-dest"}, Indices: []IndexState{{Name: "test", Status: "failed"}}})
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 409 {
		t.Fatalf("wrong cluster accepted: %s", w.Body.String())
	}
	for _, r := range b.requests {
		if strings.HasPrefix(r, "DELETE ") {
			t.Fatalf("unsafe delete: %s", r)
		}
	}
}
func TestConnectionChangeRejectedDuringMigration(t *testing.T) {
	s, _, _ := testServer(t)
	migrateMu.Lock()
	migrateActive = true
	migrateMu.Unlock()
	defer func() { migrateMu.Lock(); migrateActive = false; migrateMu.Unlock() }()
	for _, path := range []string{"/api/connect", "/api/test-source", "/api/test-dest"} {
		if w := call(s, path, `{"host":"http://example.invalid"}`); w.Code != 409 {
			t.Fatalf("connection switch accepted: %s", path)
		}
	}
}
func TestInitialCheckpointFailurePreventsWrites(t *testing.T) {
	s, _, b := testServer(t)
	s.StatePath = filepath.Join(t.TempDir(), "missing", "state.json")
	if w := call(s, "/api/migrate", `{"indices":["test"]}`); w.Code != 500 {
		t.Fatalf("expected save failure: %s", w.Body.String())
	}
	for _, r := range b.requests {
		if strings.HasPrefix(r, "PUT ") || strings.HasPrefix(r, "DELETE ") {
			t.Fatalf("wrote without checkpoint: %s", r)
		}
	}
}
func TestStateSnapshotDoesNotAliasLiveState(t *testing.T) {
	_, _, _ = testServer(t)
	SetActiveMigration(&ActiveMigration{Indices: []IndexState{{Name: "test", Status: "queued", Logs: []string{"start"}}}})
	snapshot := GetState()
	snapshot.Active.Indices[0].Status = "completed"
	snapshot.Active.Indices[0].Logs[0] = "changed"
	if got := GetState().Active.Indices[0]; got.Status != "queued" || got.Logs[0] != "start" {
		t.Fatalf("snapshot aliases state: %+v", got)
	}
}

func TestResumeDoesNotSilentlySkipPartialCopy(t *testing.T) {
	s, _, b := testServer(t)
	b.failBulk = true
	if w := call(s, "/api/migrate", `{"indices":["test"],"overwrite":false,"force_scroll":true}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	waitMigration(t, s)
	b.mu.Lock()
	b.failBulk = false
	b.mu.Unlock()
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	saved := waitMigration(t, s)
	if saved.Active == nil || saved.Active.Indices[0].Status != "failed" || !strings.Contains(saved.Active.Indices[0].Error, "overwrite is disabled") {
		t.Fatalf("partial index treated as done: %+v", saved)
	}
	for _, r := range b.requests {
		if strings.HasPrefix(r, "DELETE ") {
			t.Fatalf("partial index deleted without permission: %s", r)
		}
	}
}

func TestLegacyResumeDoesNotAuthorizeOverwrite(t *testing.T) {
	s, _, b := testServer(t)
	b.indices["test"] = true
	SetActiveMigration(&ActiveMigration{Source: ConnectionState{ClusterUUID: "source"}, Dest: ConnectionState{ClusterUUID: "dest"}, Indices: []IndexState{{Name: "test", Status: "queued"}}})
	if err := SaveState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	saved := waitMigration(t, s)
	if saved.Active != nil || saved.History[0].Indices[0].Status != "skipped" {
		t.Fatalf("legacy choice mishandled: %+v", saved)
	}
	for _, r := range b.requests {
		if strings.HasPrefix(r, "DELETE ") {
			t.Fatalf("legacy checkpoint authorized deletion: %s", r)
		}
	}
}

func TestReverseMigration(t *testing.T) {
	s, a, b := testServer(t)
	delete(a.indices, "test")
	b.indices["test"] = true
	if w := call(s, "/api/migrate", `{"indices":["test"],"direction":"target->source","force_scroll":true}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	saved := waitMigration(t, s)
	if saved.Active != nil || len(a.documents) != 1 || len(b.documents) != 0 {
		t.Fatalf("reverse migration failed: %+v", saved)
	}
}

func TestNativeTaskMustStopBeforeResume(t *testing.T) {
	s, _, b := testServer(t)
	b.taskCode = 404
	b.taskBody = `{"error":"unavailable"}`
	if w := call(s, "/api/migrate", `{"indices":["test"],"overwrite":true}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	waitMigration(t, s)
	b.mu.Lock()
	b.taskCode = 0
	b.taskBody = `{"completed":false,"task":{"status":{"created":1,"total":2}}}`
	b.mu.Unlock()
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 409 {
		t.Fatal("resumed while native task was running")
	}
	b.mu.Lock()
	b.taskBody = `{"completed":true,"response":{"created":1,"total":1,"failures":[]}}`
	b.mu.Unlock()
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	saved := waitMigration(t, s)
	if saved.Active != nil || saved.History[0].Indices[0].Status != "completed" {
		t.Fatalf("terminal task could not resume: %+v", saved)
	}
}

func TestLoadStateReplacesPreviousActiveState(t *testing.T) {
	s, _, _ := testServer(t)
	SetActiveMigration(&ActiveMigration{Indices: []IndexState{{Name: "test"}}})
	if err := os.WriteFile(s.StatePath, []byte(`{"history":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := LoadState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	if GetState().Active != nil {
		t.Fatal("load retained stale active migration")
	}
}
