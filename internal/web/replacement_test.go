package web

import (
	"encoding/json"
	"strings"
	"testing"
)

const overwriteRequest = `{"indices":["test"],"collision_action":"overwrite","source_uuid":"source","target_uuid":"dest","overwrite_uuids":{"test":"original"}}`

func replacementServer(t *testing.T) (*Server, *fakeCluster, *fakeCluster) {
	s, a, b := testServer(t)
	b.indices["test"] = true
	b.indexUUIDs = map[string]string{"test": "original"}
	b.indexDocs = map[string]map[string]string{"test": {"old/id": `{"old":true}`}}
	return s, a, b
}
func countRequest(f *fakeCluster, request string) int {
	n := 0
	for _, r := range f.requests {
		if r == request {
			n++
		}
	}
	return n
}
func TestReplacementAuthorization(t *testing.T) {
	for _, mode := range []string{"missing index UUID", "wrong index UUID", "missing cluster UUID", "writable destination", "same cluster"} {
		t.Run(mode, func(t *testing.T) {
			s, _, b := replacementServer(t)
			var req map[string]any
			_ = json.Unmarshal([]byte(overwriteRequest), &req)
			switch mode {
			case "missing index UUID":
				delete(req, "overwrite_uuids")
			case "wrong index UUID":
				req["overwrite_uuids"] = map[string]string{"test": "stale"}
			case "missing cluster UUID":
				delete(req, "source_uuid")
			case "writable destination":
				b.writable = true
			case "same cluster":
				s.Target, s.TargetCfg, s.TargetURL = s.Source, s.SourceCfg, s.SourceURL
				req["target_uuid"] = "source"
			}
			raw, _ := json.Marshal(req)
			w := call(s, "/api/migrate", string(raw))
			if w.Code != 400 && w.Code != 409 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			for _, r := range b.requests {
				if strings.HasPrefix(r, "DELETE ") || strings.HasPrefix(r, "PUT ") {
					t.Fatal(r)
				}
			}
		})
	}
}
func TestReplacementPreservesOriginalUntilVerified(t *testing.T) {
	for _, mode := range []string{"create failure", "bulk failure", "corrupted copy", "unhealthy stage", "replaced destination"} {
		t.Run(mode, func(t *testing.T) {
			s, _, b := replacementServer(t)
			switch mode {
			case "create failure":
				b.failCreate = true
			case "bulk failure":
				b.failBulk = true
			case "corrupted copy":
				b.corruptBulk = true
			case "unhealthy stage":
				b.failHealth = true
			case "replaced destination":
				b.replaceOnHealth = true
			}
			if w := call(s, "/api/migrate", overwriteRequest); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			st := waitMigration(t, s)
			if st.Active == nil || st.Active.Indices[0].Status != "failed" {
				t.Fatalf("%+v", st)
			}
			if countRequest(b, "DELETE /test") != 0 || b.indexDocs["test"]["old/id"] == "" {
				t.Fatal("original data was removed")
			}
		})
	}
}
func TestReplacementPublishesVerifiedCopy(t *testing.T) {
	s, a, b := replacementServer(t)
	if w := call(s, "/api/migrate", overwriteRequest); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	if st.Active != nil {
		t.Fatalf("%+v", st.Active.Indices)
	}
	idx := st.History[0].Indices[0]
	if idx.Status != "completed" || idx.ReplacePhase != "complete" || idx.TargetIndex != "test" || idx.TargetUUID == "original" {
		t.Fatalf("%+v", idx)
	}
	if !strings.HasPrefix(idx.StageIndex, "test_reindex_") || len(idx.RetainedTargets) != 1 || idx.RetainedTargets[0] != idx.StageIndex {
		t.Fatalf("%+v", idx)
	}
	if b.indexDocs["test"]["tenant/original"] != `{"v":1}` || len(b.indexDocs["test"]) != 1 {
		t.Fatal(b.indexDocs)
	}
	if b.indices["test"] || !fakeWriteBlocked(b, "test") || !fakeWriteBlocked(b, idx.StageIndex) {
		t.Fatal("published copy is not closed and blocked")
	}
	if countRequest(b, "DELETE /test") != 1 || countRequest(a, "DELETE /test") != 0 {
		t.Fatal("wrong delete count")
	}
	verified, healthy, deleted, cloned := -1, -1, -1, -1
	for i, r := range b.requests {
		if r == "POST /"+idx.StageIndex+"/_search" && deleted < 0 {
			verified = i
		}
		if r == "GET /_cluster/health/"+idx.StageIndex {
			healthy = i
		}
		if r == "DELETE /test" {
			deleted = i
		}
		if r == "POST /"+idx.StageIndex+"/_clone/test" {
			cloned = i
		}
	}
	if verified < 0 || healthy < verified || deleted < healthy || cloned < deleted {
		t.Fatalf("unsafe order: %v", b.requests)
	}
	var clone struct {
		Settings map[string]any `json:"settings"`
	}
	_ = json.Unmarshal([]byte(b.cloneBody), &clone)
	if clone.Settings["index.blocks.write"] != true {
		t.Fatal(b.cloneBody)
	}
}
func TestReplacementUncertainRequestsNeverReplay(t *testing.T) {
	for _, phase := range []string{"delete_pending", "clone_pending"} {
		t.Run(phase, func(t *testing.T) {
			s, _, b := replacementServer(t)
			b.failDelete = phase == "delete_pending"
			b.failClone = phase == "clone_pending"
			if w := call(s, "/api/migrate", overwriteRequest); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			st := waitMigration(t, s)
			if st.Active == nil || st.Active.Indices[0].ReplacePhase != phase {
				t.Fatalf("%+v", st)
			}
			idx := st.Active.Indices[0]
			if b.indexDocs[idx.StageIndex]["tenant/original"] == "" {
				t.Fatal("verified copy lost")
			}
			// Reload the durable checkpoint as a newly started application would.
			if err := LoadState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"/api/migrate/continue", "/api/migrate/abandon"} {
				if w := call(s, path, `{}`); w.Code != 409 {
					t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
				}
			}
			if countRequest(b, "DELETE /test") != 1 {
				t.Fatal("delete replayed")
			}
			wantClone := 0
			if phase == "clone_pending" {
				wantClone = 1
			}
			if countRequest(b, "POST /"+idx.StageIndex+"/_clone/test") != wantClone {
				t.Fatal("clone replayed")
			}
		})
	}
}
func TestReplacementResumeAfterConfirmedDelete(t *testing.T) {
	s, _, b := replacementServer(t)
	b.failClone = true
	if w := call(s, "/api/migrate", overwriteRequest); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	// Simulate the durable state between an acknowledged delete and clone submission.
	st.Active.Indices[0].ReplacePhase = "deleted"
	restoreState(st)
	if err := SaveState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	b.failClone = false
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st = waitMigration(t, s)
	if st.Active != nil || st.History[0].Indices[0].ReplacePhase != "complete" {
		t.Fatalf("%+v", st)
	}
	if countRequest(b, "DELETE /test") != 1 {
		t.Fatal("delete replayed")
	}
}

func TestReplacementFinalizingRecoveryDoesNotDeleteAgain(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "same copy", true: "external replacement"}[changed], func(t *testing.T) {
			s, _, b := replacementServer(t)
			if w := call(s, "/api/migrate", overwriteRequest); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			st := waitMigration(t, s)
			h := st.History[0]
			idx := h.Indices[0]
			idx.Status = "failed"
			idx.ReplacePhase = "finalizing"
			st.Active = &ActiveMigration{Source: h.Source, Dest: h.Dest, Direction: h.Direction, Indices: []IndexState{idx}}
			st.History = nil
			restoreState(st)
			if err := SaveState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			if changed {
				b.indexUUIDs["test"] = "external-replacement"
			}
			if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			st = waitMigration(t, s)
			if changed && (st.Active == nil || st.Active.Indices[0].Status != "failed") {
				t.Fatal("accepted replaced index")
			}
			if !changed && st.Active != nil {
				t.Fatalf("%+v", st.Active.Indices)
			}
			if countRequest(b, "DELETE /test") != 1 || countRequest(b, "POST /"+idx.StageIndex+"/_clone/test") != 1 {
				t.Fatal("repeated destructive step")
			}
		})
	}
}
func TestReplacementReverseDirection(t *testing.T) {
	s, a, b := replacementServer(t)
	s.Source, s.Target = s.Target, s.Source
	s.SourceCfg, s.TargetCfg = s.TargetCfg, s.SourceCfg
	s.SourceURL, s.TargetURL = s.TargetURL, s.SourceURL
	req := `{"indices":["test"],"direction":"target->source","collision_action":"overwrite","source_uuid":"dest","target_uuid":"source","overwrite_uuids":{"test":"original"}}`
	if w := call(s, "/api/migrate", req); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st := waitMigration(t, s)
	if st.Active != nil {
		t.Fatalf("%+v", st.Active.Indices)
	}
	if countRequest(b, "DELETE /test") != 1 || countRequest(a, "DELETE /test") != 0 {
		t.Fatal("overwrote wrong cluster")
	}
}
