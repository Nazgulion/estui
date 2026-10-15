package web

import (
	"strings"
	"testing"
)

func assertSourceUntouched(t *testing.T, source *fakeCluster, name string) {
	t.Helper()
	for _, request := range source.requests {
		parts := strings.SplitN(request, " ", 2)
		if parts[1] == "/"+name || strings.HasPrefix(parts[1], "/"+name+"/") {
			if parts[0] == "GET" || request == "POST /"+name+"/_search" || request == "POST /"+name+"/_count" {
				continue
			}
			t.Fatal("source mutation:", request)
		}
	}
}

func TestLiveCopyLeavesSourceUntouchedAndVerifiesCapturedDocuments(t *testing.T) {
	for _, mode := range []string{"cross", "local", "reverse", "mapping", "empty", "changing"} {
		t.Run(mode, func(t *testing.T) {
			s, a, b, req := remapServer(t, "keep", mode == "local")
			req.AllowLiveSource = true
			if mode != "mapping" {
				req.Remaps = nil
			}
			source, dest := a, b
			if mode == "reverse" {
				s.Source, s.Target = s.Target, s.Source
				s.SourceCfg, s.TargetCfg = s.TargetCfg, s.SourceCfg
				req.SourceUUID, req.TargetUUID = "dest", "source"
				req.Direction = "target->source"
			}
			source.writable = true
			source.indexDocs["test"] = map[string]string{"tenant/original": `{"v":9007199254740993}`}
			if mode == "empty" {
				source.indexDocs["test"] = map[string]string{}
			}
			if mode == "changing" {
				source.afterSearch = func() { source.indexDocs["test"] = map[string]string{"other/new": `{"v":2}`} }
			}
			if code, body := sendRemap(s, req); code != 200 {
				t.Fatal(code, body)
			}
			st := waitMigration(t, s)
			if st.Active != nil || len(st.History) != 1 {
				t.Fatalf("failed: %+v", st.Active)
			}
			idx := st.History[0].Indices[0]
			want := int64(1)
			if mode == "empty" {
				want = 0
			}
			if idx.Status != "copied_live" || !idx.AllowLiveSource || idx.CapturedHash == "" || idx.CapturedHash != idx.VerifiedHash || idx.VerifiedCount != want || idx.CapturedCount != want || idx.DocsTotal != want {
				t.Fatalf("wrong verification scope: %+v", idx)
			}
			if !source.indices["test"] || fakeWriteBlocked(source, "test") {
				t.Fatal("source state changed")
			}
			if dest.indices[idx.TargetIndex] || fakeWriteBlocked(dest, idx.TargetIndex) {
				t.Fatal("destination not closed with source write setting")
			}
			if mode != "empty" && dest.indexDocs[idx.TargetIndex]["tenant/original"] != `{"v":9007199254740993}` {
				t.Fatal("captured document changed")
			}
			assertSourceUntouched(t, source, "test")
			// Continue after reload verifies the captured destination, even if the
			// current source has changed; it must never block or reopen the source.
			source.indexDocs["test"] = map[string]string{"later/new": `{"v":3}`}
			h := st.History[0]
			SetActiveMigration(&ActiveMigration{AllowLiveSource: true, Source: h.Source, Dest: h.Dest, Direction: h.Direction, Indices: h.Indices})
			if err := SaveState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			restoreState(MigrationState{})
			if err := LoadState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if GetState().Active != nil {
				t.Fatal("live checkpoint not completed")
			}
			if fakeWriteBlocked(dest, idx.TargetIndex) || dest.indices[idx.TargetIndex] {
				t.Fatal("Continue did not restore destination state")
			}
			if mode != "local" && (dest.unblockedSearch || dest.unblockWhileOpen) {
				t.Fatal("destination verification was not protected")
			}
			assertSourceUntouched(t, source, "test")
		})
	}
}

func TestLiveCopyRejectsCorruptDestination(t *testing.T) {
	for _, mode := range []string{"corrupt", "bulk failure"} {
		t.Run(mode, func(t *testing.T) {
			s, a, b := testServer(t)
			a.writable = true
			b.corruptBulk, b.failBulk = mode == "corrupt", mode == "bulk failure"
			if w := call(s, "/api/migrate", `{"indices":["test"],"allow_live_source":true}`); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			st := waitMigration(t, s)
			if st.Active == nil || st.Active.Indices[0].Status != "failed" {
				t.Fatalf("failure missed: %+v", st)
			}
			if mode == "corrupt" && !strings.Contains(st.Active.Indices[0].Error, "verification failed") {
				t.Fatal(st.Active.Indices[0].Error)
			}
			assertSourceUntouched(t, a, "test")
		})
	}
}

func TestLiveCopyCannotRemoveOriginalOrOverwriteDestination(t *testing.T) {
	for _, action := range []string{"backup", "delete", "overwrite"} {
		t.Run(action, func(t *testing.T) {
			s, a, b, req := remapServer(t, action, false)
			req.AllowLiveSource = true
			if action == "overwrite" {
				req.Remaps = nil
				req.CollisionAction = "overwrite"
			}
			if code, body := sendRemap(s, req); code != 400 {
				t.Fatal(code, body)
			}
			for _, cluster := range []*fakeCluster{a, b} {
				for _, request := range cluster.requests {
					if !strings.HasPrefix(request, "GET ") {
						t.Fatal(request)
					}
				}
			}
		})
	}
}

func TestContinueCanOptIntoLiveCopyAndRetryWithoutSourceChanges(t *testing.T) {
	s, a, b := testServer(t)
	a.writable = true
	if w := call(s, "/api/migrate", `{"indices":["test"]}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	if st.Active == nil || !strings.Contains(st.Active.Indices[0].Error, "write block") {
		t.Fatal("expected write-block prerequisite failure")
	}
	b.failBulk = true
	if w := call(s, "/api/migrate/continue", `{"allow_live_source":true}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st = waitMigration(t, s)
	if st.Active == nil || !st.Active.AllowLiveSource || !st.Active.Indices[0].AllowLiveSource {
		t.Fatal("consent not persisted")
	}
	partial := st.Active.Indices[0].TargetIndex
	b.failBulk = false
	if err := LoadState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st = waitMigration(t, s)
	if st.Active != nil || st.History[0].Indices[0].Status != "copied_live" || st.History[0].Indices[0].TargetIndex == partial {
		t.Fatalf("bad live retry: %+v", st)
	}
	if _, ok := b.indices[partial]; !ok {
		t.Fatal("partial copy removed")
	}
	assertSourceUntouched(t, a, "test")
}

func TestLiveContinueRejectsDestinationTampering(t *testing.T) {
	s, a, b := testServer(t)
	a.writable = true
	if w := call(s, "/api/migrate", `{"indices":["test"],"allow_live_source":true}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	h := st.History[0]
	idx := h.Indices[0]
	b.indexDocs[idx.TargetIndex]["tenant/original"] = `{"tampered":true}`
	SetActiveMigration(&ActiveMigration{AllowLiveSource: true, Source: h.Source, Dest: h.Dest, Indices: h.Indices})
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 409 || !strings.Contains(w.Body.String(), "verification failed") {
		t.Fatal(w.Code, w.Body.String())
	}
	assertSourceUntouched(t, a, "test")
}

func TestLiveContinueRejectsDestructiveCheckpoint(t *testing.T) {
	s, a, b := testServer(t)
	SetActiveMigration(&ActiveMigration{Source: ConnectionState{ClusterUUID: "source"}, Dest: ConnectionState{ClusterUUID: "dest"}, Indices: []IndexState{{Name: "test", Status: "failed", FinalIndex: "test", ReplacePhase: "delete_pending"}}})
	if w := call(s, "/api/migrate/continue", `{"allow_live_source":true}`); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if GetState().Active.AllowLiveSource {
		t.Fatal("destructive checkpoint converted")
	}
	for _, cluster := range []*fakeCluster{a, b} {
		for _, r := range cluster.requests {
			if !strings.HasPrefix(r, "GET ") {
				t.Fatal(r)
			}
		}
	}
}

// Closed-source copies may open and close the index, but must not change its
// mappings, write block, documents or any other settings.
func assertSourceOnlyOpenedAndClosed(t *testing.T, source *fakeCluster, name string) {
	t.Helper()
	for _, request := range source.requests {
		if request == "POST /"+name+"/_open" || request == "POST /"+name+"/_close" {
			continue
		}
		assertSourceUntouched(t, &fakeCluster{requests: []string{request}}, name)
	}
}

func TestLiveCopyRestoresClosedSource(t *testing.T) {
	for _, mode := range []string{"cross", "local", "mapping", "bulk failure", "corrupt", "open failure", "close failure", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			s, a, b, req := remapServer(t, "keep", mode == "local")
			req.AllowLiveSource = true
			if mode != "mapping" {
				req.Remaps = nil
			}
			a.writable, a.indices["test"] = true, false
			b.failBulk, b.corruptBulk = mode == "bulk failure", mode == "corrupt"
			a.failOpen, a.failClose = mode == "open failure", mode == "close failure"
			if mode == "replaced" {
				a.afterSearch = func() { a.indexUUIDs = map[string]string{"test": "external-replacement"} }
			}
			if code, body := sendRemap(s, req); code != 200 {
				t.Fatal(code, body)
			}
			st := waitMigration(t, s)
			success := mode == "cross" || mode == "local" || mode == "mapping"
			if success {
				if st.Active != nil || len(st.History) != 1 {
					t.Fatalf("migration failed: %+v", st.Active)
				}
				idx := st.History[0].Indices[0]
				if idx.Status != "copied_live" || idx.SourceWasClosed == nil || !*idx.SourceWasClosed {
					t.Fatalf("missing restoration metadata: %+v", idx)
				}
			} else if st.Active == nil || st.Active.Indices[0].Status != "failed" {
				t.Fatalf("failure not reported: %+v", st)
			}
			wantOpen := mode == "close failure" || mode == "replaced"
			if a.indices["test"] != wantOpen || fakeWriteBlocked(a, "test") {
				t.Fatal("wrong source state after cleanup")
			}
			if mode == "replaced" {
				for _, request := range a.requests {
					if request == "POST /test/_close" {
						t.Fatal("replacement index was closed")
					}
				}
			}
			assertSourceOnlyOpenedAndClosed(t, a, "test")
			if mode == "close failure" {
				if !strings.Contains(st.Active.Indices[0].Error, "close rejected") {
					t.Fatal(st.Active.Indices[0].Error)
				}
				a.failClose = false
				if err := LoadState(s.StatePath); err != nil {
					t.Fatal(err)
				}
				if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
					t.Fatal(w.Body.String())
				}
				st = waitMigration(t, s)
				if st.Active != nil || a.indices["test"] {
					t.Fatal("Continue did not recover source closure")
				}
				assertSourceOnlyOpenedAndClosed(t, a, "test")
			}
		})
	}
}

func TestLiveFinishedContinueRestoresClosedSource(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "verified", true: "corrupt"}[corrupt], func(t *testing.T) {
			s, a, b := testServer(t)
			a.writable, a.indices["test"] = true, false
			if w := call(s, "/api/migrate", `{"indices":["test"],"allow_live_source":true}`); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			st := waitMigration(t, s)
			if st.Active != nil || len(st.History) != 1 {
				t.Fatalf("migration failed: %+v", st.Active)
			}
			h := st.History[0]
			// Model a process stop with a completed checkpoint and an open source.
			a.indices["test"], a.requests = true, nil
			if corrupt {
				b.indexDocs[h.Indices[0].TargetIndex]["tenant/original"] = `{"changed":true}`
			}
			SetActiveMigration(&ActiveMigration{AllowLiveSource: true, Source: h.Source, Dest: h.Dest, Indices: h.Indices})
			if err := SaveState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			restoreState(MigrationState{})
			if err := LoadState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			wantCode := 200
			if corrupt {
				wantCode = 409
			}
			if w := call(s, "/api/migrate/continue", `{}`); w.Code != wantCode {
				t.Fatal(w.Code, w.Body.String())
			}
			if a.indices["test"] {
				t.Fatal("source was not closed on Continue")
			}
			assertSourceOnlyOpenedAndClosed(t, a, "test")
		})
	}
}

func TestLiveDestinationWriteBlockMatchesCapturedSource(t *testing.T) {
	for _, mode := range []string{"blocked", "unblocked", "settings failure", "unacknowledged", "ignored", "corrupt", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			s, a, b := testServer(t)
			a.writable = mode != "blocked"
			b.failSettings, b.unackSettings, b.ignoreSettings = mode == "settings failure", mode == "unacknowledged", mode == "ignored"
			b.corruptBulk = mode == "corrupt"
			if w := call(s, "/api/migrate", `{"indices":["test"],"allow_live_source":true}`); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			st := waitMigration(t, s)
			success := mode == "blocked" || mode == "unblocked" || mode == "legacy"
			if !success {
				if st.Active == nil || st.Active.Indices[0].Status != "failed" {
					t.Fatal("failure hidden")
				}
				// An unacknowledged release may already have applied, but must
				// never be reported as success. The verified copy stays closed.
				if mode == "unacknowledged" {
					if b.indices[st.Active.Indices[0].TargetIndex] || !strings.Contains(st.Active.Indices[0].Error, "not acknowledged") {
						t.Fatal("ambiguous restoration was not reported safely")
					}
				} else if !fakeWriteBlocked(b, st.Active.Indices[0].TargetIndex) {
					t.Fatal("failed copy unblocked")
				}
				return
			}
			if st.Active != nil || len(st.History) != 1 {
				t.Fatalf("copy failed: %+v", st.Active)
			}
			h := st.History[0]
			idx := h.Indices[0]
			if idx.SourceWriteBlocked == nil || *idx.SourceWriteBlocked != (mode == "blocked") {
				t.Fatal("source setting not captured")
			}
			if fakeWriteBlocked(b, idx.TargetIndex) != (mode == "blocked") {
				t.Fatal("destination setting differs from source")
			}
			// The saved setting, not a later source change, controls Continue.
			a.writable = !a.writable
			if mode == "legacy" {
				h.Indices[0].SourceWriteBlocked = nil
				b.writeBlocks[idx.TargetIndex] = true
			}
			SetActiveMigration(&ActiveMigration{AllowLiveSource: true, Source: h.Source, Dest: h.Dest, Indices: h.Indices})
			if err := SaveState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			restoreState(MigrationState{})
			if err := LoadState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if fakeWriteBlocked(b, idx.TargetIndex) != (mode == "blocked" || mode == "legacy") {
				t.Fatal("Continue changed captured write policy")
			}
			if b.unblockedSearch || b.unblockWhileOpen {
				t.Fatal("verification protection failed")
			}
			assertSourceUntouched(t, a, "test")
		})
	}
}
