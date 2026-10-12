package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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
	filterQuery, filterID string
	indexMappings         map[string]map[string]any
	failBulkIndex         string
	corruptBulkIndex      string
	ignoreMapping         bool
	indexDocs             map[string]map[string]string
	writeBlocks           map[string]bool
	writable              bool
	corruptBulk           bool

	indexUUIDs     map[string]string
	nextUUID       int
	blockBulkIndex string
	bulkBlocked    chan struct{}

	mu                                           sync.Mutex
	uuid                                         string
	version                                      string
	indices                                      map[string]bool
	documents                                    map[string]string
	requests                                     []string
	query                                        string
	failCreate, failDelete, failBulk, failDetail bool
	failClone, failHealth, replaceOnHealth       bool
	cloneBody                                    string
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
		version := f.version
		if version == "" {
			version = "8.19.7"
		}
		write(map[string]any{"cluster_name": f.uuid, "cluster_uuid": f.uuid, "version": map[string]string{"number": version}})
	case path == "/_index_template":
		write(map[string]any{"index_templates": []any{}})
	case strings.HasPrefix(path, "/_cluster/health/"):
		if f.replaceOnHealth {
			f.indexUUIDs["test"] = "external-replacement"
		}
		write(map[string]any{"status": "green", "timed_out": f.failHealth})
	case strings.Contains(path, "/_clone/"):
		parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
		from, to := parts[0], parts[2]
		raw, _ := io.ReadAll(r.Body)
		f.cloneBody = string(raw)
		_, exists := f.indices[to]
		if f.failClone || exists {
			w.WriteHeader(400)
			write(map[string]string{"error": "clone rejected"})
			return
		}
		f.indices[to] = true
		f.indexDocs[to] = map[string]string{}
		for k, v := range f.indexDocs[from] {
			f.indexDocs[to][k] = v
		}
		f.writeBlocks[to] = true
		f.nextUUID++
		f.indexUUIDs[to] = fmt.Sprintf("clone-%d", f.nextUUID)
		write(map[string]bool{"acknowledged": true})
	case strings.HasSuffix(path, "/_block/write"):
		name := strings.Split(strings.TrimPrefix(path, "/"), "/")[0]
		if f.writeBlocks == nil {
			f.writeBlocks = map[string]bool{}
		}
		f.writeBlocks[name] = true
		write(map[string]any{"acknowledged": true, "shards_acknowledged": true})
	case strings.HasSuffix(path, "/_refresh"):
		write(map[string]any{"_shards": map[string]int{"total": 1, "failed": 0}})
	case path == "/_cat/indices":
		list := []map[string]string{}
		for name, open := range f.indices {
			status := "open"
			if !open {
				status = "close"
			}
			list = append(list, map[string]string{"index": name, "uuid": fakeIndexUUID(f, name), "status": status, "docs.count": "1"})
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
		name := strings.Split(strings.TrimPrefix(path, "/"), "/")[0]
		hits := []map[string]any{}
		if docs, exists := f.indexDocs[name]; exists {
			for key, value := range docs {
				var query struct {
					Query struct {
						QueryString struct {
							Query string `json:"query"`
						} `json:"query_string"`
					} `json:"query"`
				}
				_ = json.Unmarshal(raw, &query)
				if f.filterQuery != "" && query.Query.QueryString.Query == f.filterQuery && key != f.filterID {
					continue
				}
				parts := strings.SplitN(key, "/", 2)
				hits = append(hits, map[string]any{"_id": parts[1], "_routing": parts[0], "_source": json.RawMessage(value)})
			}
		} else {
			hits = append(hits, map[string]any{"_id": "original", "_routing": "tenant", "_source": map[string]int{"v": 1}})
		}
		write(map[string]any{"_scroll_id": "first", "hits": map[string]any{"hits": hits}})
	case strings.HasSuffix(path, "/_bulk"):
		if f.blockBulkIndex != "" && path == "/"+f.blockBulkIndex+"/_bulk" {
			// Simulate a partial transfer while the child process is still alive.
			f.documents["partial-"+f.blockBulkIndex] = "copied before crash"
			_, _ = io.Copy(io.Discard, r.Body)
			f.bulkBlocked <- struct{}{}
			f.mu.Unlock()
			<-r.Context().Done()
			f.mu.Lock()
			return
		}
		if f.failBulk || nameFromPath(path) == f.failBulkIndex {
			w.WriteHeader(400)
			write(map[string]string{"error": "bulk rejected"})
			return
		}
		raw, _ := io.ReadAll(r.Body)
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		name := nameFromPath(path)
		if f.indexDocs == nil {
			f.indexDocs = map[string]map[string]string{}
		}
		if f.indexDocs[name] == nil {
			f.indexDocs[name] = map[string]string{}
		}
		items := []any{}
		for i := 0; i+1 < len(lines); i += 2 {
			var meta map[string]map[string]string
			_ = json.Unmarshal([]byte(lines[i]), &meta)
			key := meta["index"]["routing"] + "/" + meta["index"]["_id"]
			value := lines[i+1]
			if f.corruptBulk || name == f.corruptBulkIndex {
				value = `{"corrupted":true}`
			}
			f.documents[key] = value
			f.indexDocs[name][key] = value
			items = append(items, map[string]any{"index": map[string]int{"status": 201}})
		}
		write(map[string]any{"errors": false, "items": items})
	case strings.HasSuffix(path, "/_close"), strings.HasSuffix(path, "/_open"):
		name := strings.Split(strings.TrimPrefix(path, "/"), "/")[0]
		f.indices[name] = strings.HasSuffix(path, "/_open")
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
			uuid := f.indexUUIDs[name]
			if uuid == "" {
				uuid = "index-" + name
			}
			write(map[string]any{name: map[string]any{"settings": map[string]any{"index": map[string]string{"uuid": uuid, "blocks.write": fmt.Sprint(fakeWriteBlocked(f, name))}}, "mappings": fakeMapping(f, name)}})
		case "PUT":
			if _, exists := f.indices[name]; exists {
				w.WriteHeader(400)
				write(map[string]string{"error": "index already exists"})
				return
			}
			var body struct {
				Mappings map[string]any `json:"mappings"`
			}
			dec := json.NewDecoder(r.Body)
			dec.UseNumber()
			_ = dec.Decode(&body)
			if f.indexMappings == nil {
				f.indexMappings = map[string]map[string]any{}
			}
			if !f.ignoreMapping {
				f.indexMappings[name] = body.Mappings
			}
			if f.failCreate {
				w.WriteHeader(400)
				write(map[string]string{"error": "create rejected"})
				return
			}
			f.indices[name] = true
			if f.indexDocs == nil {
				f.indexDocs = map[string]map[string]string{}
			}
			f.indexDocs[name] = map[string]string{}
			if f.writeBlocks == nil {
				f.writeBlocks = map[string]bool{}
			}
			f.writeBlocks[name] = false
			if f.indexUUIDs == nil {
				f.indexUUIDs = make(map[string]string)
			}
			f.nextUUID++
			f.indexUUIDs[name] = fmt.Sprintf("index-%s-%d", name, f.nextUUID)
			write(map[string]bool{"acknowledged": true})
		case "DELETE":
			if f.failDelete {
				w.WriteHeader(403)
				write(map[string]string{"error": "delete rejected"})
				return
			}
			delete(f.indices, name)
			delete(f.indexUUIDs, name)
			delete(f.indexDocs, name)
			delete(f.documents, "partial-"+name)
			write(map[string]bool{"acknowledged": true})
		}
	}
}
func nameFromPath(path string) string { return strings.Split(strings.TrimPrefix(path, "/"), "/")[0] }
func fakeMapping(f *fakeCluster, name string) map[string]any {
	if m := f.indexMappings[name]; m != nil {
		return m
	}
	return map[string]any{}
}
func fakeWriteBlocked(f *fakeCluster, name string) bool {
	if blocked, ok := f.writeBlocks[name]; ok {
		return blocked
	}
	return !f.writable
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
func TestSameClusterCreatesPrefixedCopy(t *testing.T) {
	for _, direction := range []string{"source->target", "target->source"} {
		t.Run(direction, func(t *testing.T) {
			s, a, _ := testServer(t)
			s.Target, s.TargetCfg, s.TargetURL = s.Source, s.SourceCfg, s.SourceURL
			w := call(s, "/api/migrate", `{"indices":["test"],"direction":"`+direction+`"}`)
			if w.Code != 200 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			st := waitMigration(t, s)
			if st.Active != nil {
				t.Fatalf("migration failed: %+v", st.Active.Indices)
			}
			idx := st.History[0].Indices[0]
			if idx.Status != "completed" || !strings.HasPrefix(idx.TargetIndex, "reindex_test_") {
				t.Fatalf("%+v", idx)
			}
			if fakeIndexUUID(a, "test") != "index-test" || !a.indices["test"] {
				t.Fatal("source changed")
			}
			for _, req := range a.requests {
				if req == "DELETE /test" {
					t.Fatal("source deleted")
				}
			}
		})
	}
}
func TestMigrationPersistsEveryOutcome(t *testing.T) {
	for _, mode := range []string{"filtered proxy", "skipped", "create failure", "bulk failure", "detail failure", "existing preserved"} {
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
			case "existing preserved":
				b.indices["test"] = true
				b.failDelete = true
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
		if strings.HasPrefix(r, "DELETE ") && r != "DELETE /_search/scroll" {
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
		if strings.HasPrefix(r, "DELETE ") && r != "DELETE /_search/scroll" {
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

func TestResumeReplacesItsOwnPartialCopy(t *testing.T) {
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
	if saved.Active != nil || saved.History[0].Indices[0].Status != "completed" {
		t.Fatalf("partial index not resumed: %+v", saved)
	}
	deletes := 0
	for _, r := range b.requests {
		if strings.HasPrefix(r, "DELETE ") && r != "DELETE /_search/scroll" {
			if r != "DELETE /test" {
				t.Fatalf("unrelated delete: %s", r)
			}
			deletes++
		}
	}
	if deletes != 0 {
		t.Fatalf("partial copy was deleted: %d", deletes)
	}
	if saved.History[0].Indices[0].TargetIndex == "test" {
		t.Fatal("partial destination reused")
	}
	if _, ok := b.indices["test"]; !ok {
		t.Fatal("partial destination was not preserved")
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
		if strings.HasPrefix(r, "DELETE ") && r != "DELETE /_search/scroll" {
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

func TestContinueReattachesToRunningNativeTask(t *testing.T) {
	s, _, b := testServer(t)
	seedNativeCheckpoint(t, s, b)
	b.mu.Lock()
	b.taskCode = 0
	b.taskBody = `{"completed":false,"task":{"status":{"created":1,"total":2}}}`
	b.requests = nil
	b.mu.Unlock()
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		b.mu.Lock()
		polls := 0
		for _, request := range b.requests {
			if strings.Contains(request, "/_tasks/") {
				polls++
			}
		}
		b.mu.Unlock()
		if polls >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("did not reattach to native task")
		}
		time.Sleep(time.Millisecond)
	}
	b.mu.Lock()
	b.taskBody = `{"completed":true,"response":{"created":2,"total":2,"failures":[]}}`
	b.mu.Unlock()
	saved := waitMigration(t, s)
	if saved.Active != nil || saved.History[0].Indices[0].Transferred != 1 {
		t.Fatalf("task result not recovered: %+v", saved)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, request := range b.requests {
		if request == "DELETE /test" || request == "PUT /test" || strings.Contains(request, "/_reindex") || strings.Contains(request, "/_bulk") {
			t.Fatalf("started another transfer while recovering task: %s", request)
		}
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

// This helper runs in a separate OS process so the parent can terminate it
// without letting defers or in-memory state save a graceful final checkpoint.
func TestMigrationCrashWorker(t *testing.T) {
	if os.Getenv("ESTUI_CRASH_WORKER") != "1" {
		t.Skip("subprocess helper")
	}
	log := logging.New(os.Getenv("ESTUI_CRASH_LOG"), logging.ERROR)
	defer log.Close()
	sourceCfg := config.ESConnection{Hosts: []string{os.Getenv("ESTUI_CRASH_SOURCE")}}
	targetCfg := config.ESConnection{Hosts: []string{os.Getenv("ESTUI_CRASH_TARGET")}}
	source, err := elastic.NewClient(sourceCfg, log)
	if err != nil {
		t.Fatal(err)
	}
	target, err := elastic.NewClient(targetCfg, log)
	if err != nil {
		t.Fatal(err)
	}
	s := New(source, target, sourceCfg.Hosts[0], targetCfg.Hosts[0], sourceCfg, targetCfg, "", log, 8080)
	s.StatePath = os.Getenv("ESTUI_CRASH_STATE")
	body := `{"indices":["index-1","index-2","index-3","index-4","index-5","index-6","index-7"],"overwrite":` + os.Getenv("ESTUI_CRASH_OVERWRITE") + `,"force_scroll":true}`
	if w := call(s, "/api/migrate", body); w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	<-time.After(30 * time.Second)
	t.Fatal("parent never interrupted the migration")
}

func TestRestartContinuesAtInterruptedIndex(t *testing.T) {
	for _, overwrite := range []bool{true, false} {
		t.Run(fmt.Sprintf("overwrite=%t", overwrite), func(t *testing.T) {
			s, a, b := testServer(t)
			a.mu.Lock()
			a.indices = map[string]bool{}
			for i := 1; i <= 7; i++ {
				a.indices[fmt.Sprintf("index-%d", i)] = true
			}
			a.indices["index-4"] = false // originally closed; must survive the restart
			a.mu.Unlock()
			b.mu.Lock()
			b.blockBulkIndex = "index-4"
			b.bulkBlocked = make(chan struct{}, 1)
			b.mu.Unlock()
			cmd := exec.Command(os.Args[0], "-test.run=^TestMigrationCrashWorker$", "-test.timeout=35s")
			cmd.Env = append(os.Environ(),
				"ESTUI_CRASH_WORKER=1", "ESTUI_CRASH_SOURCE="+s.SourceCfg.Hosts[0],
				"ESTUI_CRASH_TARGET="+s.TargetCfg.Hosts[0], "ESTUI_CRASH_STATE="+s.StatePath,
				"ESTUI_CRASH_LOG="+filepath.Join(t.TempDir(), "worker.log"),
				fmt.Sprintf("ESTUI_CRASH_OVERWRITE=%t", overwrite))
			output, err := os.Create(filepath.Join(t.TempDir(), "worker-output.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			cmd.Stdout = output
			cmd.Stderr = output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			stopped := false
			defer func() {
				if !stopped {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			select {
			case <-b.bulkBlocked:
			case <-time.After(10 * time.Second):
				t.Fatalf("worker never reached index 4; output: %s", output.Name())
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			stopped = true
			// The test process never held the worker's memory. Load only its disk state.
			if err := LoadState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			checkpoint := GetState()
			if checkpoint.Active == nil {
				t.Fatal("missing interrupted migration")
			}
			for i, idx := range checkpoint.Active.Indices {
				want := "queued"
				if i < 3 {
					want = "completed"
				}
				if i == 3 {
					want = "running"
				}
				if idx.Status != want {
					t.Fatalf("index %d: %s want %s", i+1, idx.Status, want)
				}
			}
			fourth := checkpoint.Active.Indices[3]
			if fourth.TargetUUID == "" || fourth.SourceWasClosed == nil || !*fourth.SourceWasClosed {
				t.Fatalf("recovery metadata missing: %+v", fourth)
			}
			a.mu.Lock()
			a.requests = nil
			a.mu.Unlock()
			b.mu.Lock()
			b.blockBulkIndex = ""
			b.requests = nil
			b.mu.Unlock()
			restarted := New(s.Source, s.Target, s.SourceURL, s.TargetURL, s.SourceCfg, s.TargetCfg, "", s.Log, s.Port)
			restarted.StatePath = s.StatePath
			if w := call(restarted, "/api/migrate/continue", `{}`); w.Code != 200 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			saved := waitMigration(t, restarted)
			if saved.Active != nil || len(saved.History) != 1 {
				t.Fatalf("migration did not finish: %+v", saved)
			}
			for _, idx := range saved.History[0].Indices {
				if idx.Status != "completed" {
					t.Fatalf("unfinished index: %+v", idx)
				}
			}
			b.mu.Lock()
			var recreated []string
			for _, request := range b.requests {
				if strings.HasPrefix(request, "PUT /") && !strings.Contains(strings.TrimPrefix(request, "PUT /"), "/") {
					recreated = append(recreated, strings.TrimPrefix(request, "PUT /"))
				}
				for i := 1; i <= 3; i++ {
					if request == fmt.Sprintf("DELETE /index-%d", i) || request == fmt.Sprintf("PUT /index-%d", i) || request == fmt.Sprintf("POST /index-%d/_bulk", i) {
						t.Errorf("completed index touched: %s", request)
					}
				}
			}
			if len(recreated) != 4 || !strings.HasPrefix(recreated[0], "index-4_reindex_") || strings.Join(recreated[1:], ",") != "index-5,index-6,index-7" {
				t.Errorf("wrong resume order: %v", recreated)
			}
			if _, exists := b.documents["partial-index-4"]; !exists {
				t.Error("partial destination was not preserved")
			}
			b.mu.Unlock()
			a.mu.Lock()
			if a.indices["index-4"] {
				t.Error("original closed source state not restored")
			}
			a.mu.Unlock()
		})
	}
}

func TestResumeProtectsReplacedDestination(t *testing.T) {
	s, _, b := testServer(t)
	b.failBulk = true
	if w := call(s, "/api/migrate", `{"indices":["test"],"overwrite":false,"force_scroll":true}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	waitMigration(t, s)
	b.mu.Lock()
	b.indexUUIDs["test"] = "another-index-created-externally"
	b.requests = nil
	b.mu.Unlock()
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	saved := waitMigration(t, s)
	if saved.Active == nil || !strings.Contains(saved.Active.Indices[0].Error, "was replaced") {
		t.Fatalf("replacement not detected: %+v", saved)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, request := range b.requests {
		if request == "DELETE /test" {
			t.Fatal("deleted unrelated replacement index")
		}
	}
}

func TestContinueRecognizesNativeCompletionWhileStopped(t *testing.T) {
	s, _, b := testServer(t)
	seedNativeCheckpoint(t, s, b)
	restoreState(MigrationState{})
	if err := LoadState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.taskCode = 0
	b.taskBody = `{"completed":true,"response":{"created":7,"total":7,"failures":[]}}`
	b.requests = nil
	b.mu.Unlock()
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	saved := waitMigration(t, s)
	if saved.Active != nil || saved.History[0].Indices[0].Transferred != 1 {
		t.Fatalf("completion not recovered: %+v", saved)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, request := range b.requests {
		if request == "DELETE /test" || request == "PUT /test" || strings.Contains(request, "/_bulk") || strings.Contains(request, "/_reindex") {
			t.Fatalf("completed native copy restarted: %s", request)
		}
	}
}

func seedNativeCheckpoint(t *testing.T, s *Server, b *fakeCluster) {
	t.Helper()
	b.indices["test"] = true
	b.indexUUIDs = map[string]string{"test": "native-target"}
	b.indexDocs = map[string]map[string]string{"test": {"tenant/original": `{"v":1}`}}
	SetActiveMigration(&ActiveMigration{Source: ConnectionState{ClusterUUID: "source"}, Dest: ConnectionState{ClusterUUID: "dest"}, Indices: []IndexState{{Name: "test", TargetIndex: "test", SourceUUID: "index-test", TargetUUID: "native-target", TaskID: "task:1", Status: "running"}}})
	if err := SaveState(s.StatePath); err != nil {
		t.Fatal(err)
	}
}

func fakeIndexUUID(f *fakeCluster, name string) string {
	if uuid := f.indexUUIDs[name]; uuid != "" {
		return uuid
	}
	return "index-" + name
}
