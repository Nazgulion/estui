package web

import (
	"context"
	"encoding/json"
	"estui/internal/config"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrationVersionAdmissionAndContinue(t *testing.T) {
	for _, versions := range [][2]string{{"7.17.16", "7.17.20"}, {"8.17.0", "8.19.7"}, {"9.0.0", "9.1.0"}} {
		t.Run(versions[0], func(t *testing.T) {
			s, a, b := testServer(t)
			a.version, b.version = versions[0], versions[1]
			if w := call(s, "/api/migrate", `{"indices":["test"]}`); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			st := waitMigration(t, s)
			if st.Active != nil || len(st.History) != 1 {
				t.Fatalf("migration failed: %+v", st)
			}
			h := st.History[0]
			SetActiveMigration(&ActiveMigration{Source: h.Source, Dest: h.Dest, Indices: h.Indices})
			if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if GetState().Active != nil {
				t.Fatal("completed copy was not revalidated")
			}
		})
	}
}

func TestUnsupportedVersionsRejectedBeforeMutations(t *testing.T) {
	for _, versions := range [][2]string{{"7.17.16", "8.17.0"}, {"8.17.0", "9.0.0"}, {"9.0.0", "7.17.16"}, {"6.8.0", "6.8.0"}, {"7.13.4", "7.13.4"}, {"10.0.0", "10.0.0"}, {"8.bad", "8.17.0"}} {
		t.Run(versions[0]+"/"+versions[1], func(t *testing.T) {
			s, a, b := testServer(t)
			a.version, b.version = versions[0], versions[1]
			for _, direction := range []string{"source->target", "target->source"} {
				if w := call(s, "/api/migrate", `{"indices":["test"],"direction":"`+direction+`"}`); w.Code != 400 {
					t.Fatal(w.Code, w.Body.String())
				}
			}
			SetActiveMigration(&ActiveMigration{Source: ConnectionState{ClusterUUID: "source"}, Dest: ConnectionState{ClusterUUID: "dest"}, Indices: []IndexState{{Name: "test", Status: "queued"}}})
			if w := call(s, "/api/migrate/continue", `{}`); w.Code != 409 {
				t.Fatal(w.Code, w.Body.String())
			}
			for _, cluster := range []*fakeCluster{a, b} {
				for _, request := range cluster.requests {
					if !strings.HasPrefix(request, "GET ") {
						t.Fatal("mutation before compatibility check:", request)
					}
				}
			}
		})
	}
}

func TestRenameNeverDeletesExistingDestination(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "verified separate copy", true: "creation failure"}[fail], func(t *testing.T) {
			s, _, b := testServer(t)
			b.indices["test"] = true
			b.indexUUIDs = map[string]string{"test": "original"}
			b.failCreate = fail
			if w := call(s, "/api/migrate", `{"indices":["test"],"overwrite":true}`); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			st := waitMigration(t, s)
			if b.indexUUIDs["test"] != "original" {
				t.Fatal("original destination changed")
			}
			for _, r := range b.requests {
				if r == "DELETE /test" {
					t.Fatal("original deleted")
				}
			}
			if !fail {
				idx := st.History[0].Indices[0]
				if idx.TargetIndex == "test" || idx.VerifiedHash == "" {
					t.Fatalf("unverified or non-isolated result: %+v", idx)
				}
			}
		})
	}
}
func TestCollisionRequiresChoice(t *testing.T) {
	s, _, b := testServer(t)
	b.indices["test"] = true
	if w := call(s, "/api/migrate", `{"indices":["test"]}`); w.Code != 409 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	for _, r := range b.requests {
		if strings.HasPrefix(r, "PUT /") || strings.HasPrefix(r, "DELETE /") {
			t.Fatal(r)
		}
	}
}
func TestAbandonRejectsOutstandingTask(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		s, _, _ := testServer(t)
		idx := IndexState{Name: "test", Status: "failed", ReindexUncertain: uncertain}
		if !uncertain {
			idx.TaskID = "task:1"
		}
		SetActiveMigration(&ActiveMigration{Indices: []IndexState{idx}})
		if w := call(s, "/api/migrate/abandon", `{}`); w.Code != 409 {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
		if GetState().Active == nil {
			t.Fatal("lost protection")
		}
	}
}
func TestAdministrativeActionsBlockedByCheckpoint(t *testing.T) {
	s, a, _ := testServer(t)
	SetActiveMigration(&ActiveMigration{Indices: []IndexState{{Name: "test", Status: "failed"}}})
	for _, p := range []string{"/api/indices/delete", "/api/indices/open", "/api/indices/close", "/api/preview-query"} {
		if w := call(s, p, `{"indices":["test"],"index":"test","host":"source"}`); w.Code != 409 {
			t.Fatalf("%s: %d", p, w.Code)
		}
	}
	if len(a.requests) != 1 {
		t.Fatalf("unexpected cluster requests: %v", a.requests)
	}
}
func TestReconnectDoesNotForwardCredentials(t *testing.T) {
	s, _, _ := testServer(t)
	s.SourceCfg.APIKey = "test-secret"
	var auth string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		_, _ = w.Write([]byte(`{"version":{"number":"8.17.0"}}`))
	}))
	defer other.Close()
	raw, _ := json.Marshal(map[string]string{"source": other.URL})
	if w := call(s, "/api/connect", string(raw)); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if auth != "" {
		t.Fatal("credential forwarded to different host")
	}
}
func TestWritableSourceRejectedBeforeDestinationMutation(t *testing.T) {
	s, a, b := testServer(t)
	a.writable = true
	if w := call(s, "/api/migrate", `{"indices":["test"]}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	if st.Active == nil || !strings.Contains(st.Active.Indices[0].Error, "write block") {
		t.Fatalf("unexpected result %+v", st)
	}
	for _, r := range b.requests {
		if strings.HasPrefix(r, "PUT ") || (strings.HasPrefix(r, "POST ") && r != "POST /_count") {
			t.Fatal(r)
		}
	}
}
func TestVerificationRejectsChangedDocumentContents(t *testing.T) {
	s, _, b := testServer(t)
	b.corruptBulk = true
	if w := call(s, "/api/migrate", `{"indices":["test"]}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	if st.Active == nil || !strings.Contains(st.Active.Indices[0].Error, "verification failed") {
		t.Fatalf("corruption not detected: %+v", st)
	}
}
func TestContinueRevalidatesCompletedCopy(t *testing.T) {
	s, _, b := testServer(t)
	if w := call(s, "/api/migrate", `{"indices":["test"]}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	completed := st.History[0].Indices[0]
	b.indexDocs[completed.TargetIndex]["tenant/original"] = `{"tampered":true}`
	SetActiveMigration(&ActiveMigration{Source: ConnectionState{ClusterUUID: "source"}, Dest: ConnectionState{ClusterUUID: "dest"}, Indices: []IndexState{completed, {Name: "next", Status: "queued"}}})
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 409 || !strings.Contains(w.Body.String(), "verification failed") {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestNativeRemoteOverrideNeverUsedForNewTransfer(t *testing.T) {
	s, _, b := testServer(t)
	s.SourceURL = "http://unverified.invalid:9200"
	if w := call(s, "/api/migrate", `{"indices":["test"]}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	if st.Active != nil {
		t.Fatal(st.Active)
	}
	for _, r := range b.requests {
		if strings.Contains(r, "_reindex") {
			t.Fatal("used unverified native endpoint")
		}
	}
}
func TestStatePathBoundToConfigDirectory(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yaml")
	s := New(nil, nil, "", "", config.ESConnection{}, config.ESConnection{}, p, nil, 8080)
	if s.StatePath != filepath.Join(filepath.Dir(p), "migration_state.json") {
		t.Fatal(s.StatePath)
	}
}
func TestProcessLockExcludesOtherConfigs(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	path := filepath.Join(t.TempDir(), "state.json")
	release, err := acquireProcessLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := acquireProcessLock(filepath.Join(t.TempDir(), "other.json")); err == nil {
		other()
		release()
		t.Fatal("second instance accepted")
	}
	release()
	release, err = acquireProcessLock(path)
	if err != nil {
		t.Fatal(err)
	}
	release()
}
func TestVerificationUsesNumericPrecisionAndRouting(t *testing.T) {
	s, a, b := testServer(t)
	a.indexDocs = map[string]map[string]string{"test": {"tenant/original": `{"n":9007199254740993}`}}
	b.indexDocs = map[string]map[string]string{"test": {"tenant/original": `{"n":9007199254740992}`}}
	first, err := s.Source.Fingerprint(context.Background(), "test", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Target.Fingerprint(context.Background(), "test", "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Hash == second.Hash {
		t.Fatal("integer precision lost")
	}
	b.indexDocs["test"] = map[string]string{"other/original": `{"n":9007199254740993}`}
	third, err := s.Target.Fingerprint(context.Background(), "test", "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Hash == third.Hash {
		t.Fatal("routing omitted")
	}
}

func TestRestartRestoresInterruptedPreview(t *testing.T) {
	s, a, _ := testServer(t)
	a.indices["test"] = false
	restore, err := s.acquirePreview("source", "test")
	if err != nil {
		t.Fatal(err)
	}
	_ = restore // Simulate process death before the deferred restoration executes.
	if err := s.Source.OpenIndex(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	restoreState(MigrationState{})
	if err := LoadState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverPreview(); err != nil {
		t.Fatal(err)
	}
	if a.indices["test"] || GetState().Preview != nil {
		t.Fatal("interrupted preview was not restored")
	}
}
func TestPreviewRecoveryRejectsReplacedIndex(t *testing.T) {
	s, a, _ := testServer(t)
	a.indices["test"] = false
	_, err := s.acquirePreview("source", "test")
	if err != nil {
		t.Fatal(err)
	}
	a.indexUUIDs = map[string]string{"test": "replacement"}
	a.indices["test"] = true
	if err := s.recoverPreview(); err == nil {
		t.Fatal("replaced index accepted")
	}
	if !a.indices["test"] || GetState().Preview == nil {
		t.Fatal("replacement was closed or checkpoint erased")
	}
}
func TestStaleClusterConfirmationRejected(t *testing.T) {
	s, _, _ := testServer(t)
	for _, req := range []struct{ path, body string }{{"/api/migrate", `{"indices":["test"],"source_uuid":"previous-cluster"}`}, {"/api/indices/delete", `{"indices":["test"],"source_uuid":"previous-cluster"}`}} {
		if w := call(s, req.path, req.body); w.Code != 409 {
			t.Fatalf("%s: %d %s", req.path, w.Code, w.Body.String())
		}
	}
}
func TestSourceIdentityPinnedAcrossRestart(t *testing.T) {
	s, a, b := testServer(t)
	b.failBulk = true
	if w := call(s, "/api/migrate", `{"indices":["test"]}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	waitMigration(t, s)
	a.indexUUIDs = map[string]string{"test": "replacement"}
	b.failBulk = false
	b.requests = nil
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	if st.Active == nil || !strings.Contains(st.Active.Indices[0].Error, "source index test was replaced") {
		t.Fatal(st)
	}
	for _, r := range b.requests {
		if strings.HasPrefix(r, "PUT /") || strings.Contains(r, "/_bulk") {
			t.Fatal(r)
		}
	}
}
