package web

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

const newMapping = `{"properties":{"v":{"type":"keyword"}}}`

func remapServer(t *testing.T, action string, local bool) (*Server, *fakeCluster, *fakeCluster, migrateRequest) {
	s, a, b := testServer(t)
	a.indexMappings = map[string]map[string]any{"test": {"properties": map[string]any{"v": map[string]any{"type": "long"}}}}
	a.indexDocs = map[string]map[string]string{"test": {"tenant/original": `{"v":1}`}}
	targetUUID := "dest"
	if local {
		s.Target, s.TargetCfg, s.TargetURL = s.Source, s.SourceCfg, s.SourceURL
		b = a
		targetUUID = "source"
	}
	req := migrateRequest{Indices: []string{"test"}, SourceUUID: "source", TargetUUID: targetUUID, Remaps: map[string]RemapOptions{"test": {Mapping: json.RawMessage(newMapping), Destination: "tmp_reindexed_test", OriginalAction: action, BackupIndex: "original_test", SourceUUID: "index-test", ConfirmRemoval: action != "keep"}}}
	return s, a, b, req
}
func sendRemap(s *Server, req migrateRequest) (int, string) {
	raw, _ := json.Marshal(req)
	w := call(s, "/api/migrate", string(raw))
	return w.Code, w.Body.String()
}
func TestRemapMappingAndOriginalActions(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, action := range []string{"keep", "backup", "delete"} {
			t.Run(action+map[bool]string{false: " cross", true: " local"}[local], func(t *testing.T) {
				s, a, b, req := remapServer(t, action, local)
				a.indices["test"] = false
				if code, body := sendRemap(s, req); code != 200 {
					t.Fatalf("%d %s", code, body)
				}
				st := waitMigration(t, s)
				if st.Active != nil {
					t.Fatalf("%+v", st.Active.Indices)
				}
				idx := st.History[0].Indices[0]
				if idx.Status != "completed" || idx.Remap.Phase != "complete" || idx.TargetIndex != "tmp_reindexed_test" {
					t.Fatalf("%+v", idx)
				}
				want, _ := decodeMapping(json.RawMessage(newMapping), "test")
				if !mappingContains(b.indexMappings[idx.TargetIndex], want) || b.indices[idx.TargetIndex] || !fakeWriteBlocked(b, idx.TargetIndex) {
					t.Fatal("destination mapping/state incorrect")
				}
				_, originalExists := a.indices["test"]
				if originalExists != (action == "keep") || idx.Remap.SourceDeleted != (action != "keep") {
					t.Fatalf("original policy not applied: %+v", idx)
				}
				if action == "keep" && a.indices["test"] {
					t.Fatal("original closed state lost")
				}
				if action == "backup" {
					if a.indices["original_test"] || !fakeWriteBlocked(a, "original_test") || idx.Remap.BackupHash == "" {
						t.Fatal("backup not verified, blocked and closed")
					}
					if !mappingContains(a.indexMappings["original_test"], a.indexMappings["test"]) {
						t.Fatal("original mapping lost")
					}
					if a.indexDocs["original_test"]["tenant/original"] != `{"v":1}` {
						t.Fatal("backup data lost")
					}
					verified, deleted := -1, -1
					for i, r := range a.requests {
						if r == "POST /original_test/_search" {
							verified = i
						}
						if r == "DELETE /test" {
							deleted = i
						}
					}
					if verified < 0 || deleted < verified {
						t.Fatal("deletion preceded backup verification")
					}
				}
			})
		}
	}
}
func TestRemapRejectsUnsafePlans(t *testing.T) {
	for _, mode := range []string{"bad json", "settings body", "filtered source", "synthetic source", "existing destination", "existing backup", "same source", "invalid name", "stale source", "no acknowledgement", "filtered deletion", "missing cluster", "unselected plan"} {
		t.Run(mode, func(t *testing.T) {
			s, a, b, req := remapServer(t, "backup", false)
			o := req.Remaps["test"]
			switch mode {
			case "bad json":
				o.Mapping = json.RawMessage(`null`)
			case "settings body":
				o.Mapping = json.RawMessage(`{"settings":{},"mappings":{}}`)
			case "filtered source":
				o.Mapping = json.RawMessage(`{"_source":{"excludes":["secret"]}}`)
			case "synthetic source":
				o.Mapping = json.RawMessage(`{"_source":{"mode":"synthetic"}}`)
			case "existing destination":
				b.indices[o.Destination] = true
			case "existing backup":
				a.indices[o.BackupIndex] = true
			case "same source":
				o.Destination = "test"
			case "invalid name":
				o.Destination = "Bad:name"
			case "stale source":
				o.SourceUUID = "replaced"
			case "no acknowledgement":
				o.ConfirmRemoval = false
			case "filtered deletion":
				o.OriginalAction = "delete"
				req.Query = "v:1"
			case "missing cluster":
				req.SourceUUID = ""
			case "unselected plan":
				req.Remaps["other"] = o
			}
			req.Remaps["test"] = o
			if code, body := sendRemap(s, req); code != 400 && code != 409 {
				t.Fatalf("%d %s", code, body)
			}
			for _, f := range []*fakeCluster{a, b} {
				for _, r := range f.requests {
					if strings.HasPrefix(r, "DELETE ") || strings.HasPrefix(r, "PUT ") {
						t.Fatal("mutation before validation", r)
					}
				}
			}
		})
	}
}
func TestRemapFailuresPreserveOriginal(t *testing.T) {
	for _, mode := range []string{"destination create", "destination bulk", "destination corruption", "mapping ignored", "backup create", "backup bulk", "backup corruption"} {
		t.Run(mode, func(t *testing.T) {
			s, a, b, req := remapServer(t, "backup", false)
			switch mode {
			case "destination create":
				b.failCreate = true
			case "destination bulk":
				b.failBulk = true
			case "destination corruption":
				b.corruptBulk = true
			case "mapping ignored":
				b.ignoreMapping = true
			case "backup create":
				a.failCreate = true
			case "backup bulk":
				a.failBulkIndex = "original_test"
			case "backup corruption":
				a.corruptBulkIndex = "original_test"
			}
			if code, body := sendRemap(s, req); code != 200 {
				t.Fatal(code, body)
			}
			st := waitMigration(t, s)
			if st.Active == nil || st.Active.Indices[0].Status != "failed" {
				t.Fatalf("%+v", st)
			}
			if countRequest(a, "DELETE /test") != 0 || a.indexDocs["test"]["tenant/original"] == "" {
				t.Fatal("original removed on failure")
			}
		})
	}
}
func TestRemapUncertainDeletionCannotReplayOrAbandon(t *testing.T) {
	s, a, _, req := remapServer(t, "backup", false)
	a.failDelete = true
	if code, body := sendRemap(s, req); code != 200 {
		t.Fatal(code, body)
	}
	st := waitMigration(t, s)
	if st.Active == nil || st.Active.Indices[0].Remap.Phase != "delete_pending" {
		t.Fatalf("%+v", st)
	}
	if err := LoadState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/migrate/continue", "/api/migrate/abandon"} {
		if w := call(s, path, `{}`); w.Code != 409 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if countRequest(a, "DELETE /test") != 1 {
		t.Fatal("delete replayed")
	}
}
func TestRemapBackupRetryKeepsPartialAndMapping(t *testing.T) {
	s, a, b, req := remapServer(t, "backup", false)
	a.failBulkIndex = "original_test"
	if code, body := sendRemap(s, req); code != 200 {
		t.Fatal(code, body)
	}
	st := waitMigration(t, s)
	targetUUID := st.Active.Indices[0].TargetUUID
	a.failBulkIndex = ""
	if err := LoadState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	st = waitMigration(t, s)
	if st.Active != nil {
		t.Fatalf("%+v", st.Active.Indices)
	}
	idx := st.History[0].Indices[0]
	if idx.TargetUUID != targetUUID || idx.Remap.BackupIndex == "original_test" {
		t.Fatalf("bad recovery: %+v", idx)
	}
	if _, ok := a.indices["original_test"]; !ok {
		t.Fatal("partial backup removed")
	}
	if b.indexDocs[idx.TargetIndex]["tenant/original"] == "" {
		t.Fatal("destination lost")
	}
}
func TestRemapCompletedRecoveryWithoutOriginal(t *testing.T) {
	for _, changed := range []string{"none", "destination data", "backup mapping"} {
		t.Run(changed, func(t *testing.T) {
			s, a, b, req := remapServer(t, "backup", false)
			if code, body := sendRemap(s, req); code != 200 {
				t.Fatal(code, body)
			}
			st := waitMigration(t, s)
			h := st.History[0]
			if changed == "destination data" {
				b.indexDocs["tmp_reindexed_test"]["tenant/original"] = `{"v":2}`
			}
			if changed == "backup mapping" {
				a.indexMappings["original_test"] = map[string]any{}
			}
			SetActiveMigration(&ActiveMigration{Source: h.Source, Dest: h.Dest, Indices: h.Indices})
			if err := SaveState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			if err := LoadState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			want := 200
			if changed != "none" {
				want = 409
			}
			if w := call(s, "/api/migrate/continue", `{}`); w.Code != want {
				t.Fatal(w.Code, w.Body.String())
			}
			if countRequest(a, "DELETE /test") != 1 {
				t.Fatal("delete replayed")
			}
		})
	}
}
func TestMappingTextPreservesPrecisionAndWrappers(t *testing.T) {
	raw := `{"test":{"mappings":{"properties":{"v":{"type":"long","null_value":9007199254740993}}}}}`
	quoted, _ := json.Marshal(raw)
	m, err := decodeMapping(quoted, "test")
	if err != nil {
		t.Fatal(err)
	}
	normalized, _ := json.Marshal(m)
	if !strings.Contains(string(normalized), "9007199254740993") {
		t.Fatal(string(normalized))
	}
}
func TestRemapReverseDirection(t *testing.T) {
	s, a, b, req := remapServer(t, "backup", false)
	s.Source, s.Target = s.Target, s.Source
	s.SourceCfg, s.TargetCfg = s.TargetCfg, s.SourceCfg
	s.SourceURL, s.TargetURL = s.TargetURL, s.SourceURL
	req.SourceUUID, req.TargetUUID = "dest", "source"
	req.Direction = "target->source"
	if code, body := sendRemap(s, req); code != 200 {
		t.Fatal(code, body)
	}
	st := waitMigration(t, s)
	if st.Active != nil {
		t.Fatalf("%+v", st.Active.Indices)
	}
	if countRequest(a, "DELETE /test") != 1 || countRequest(b, "DELETE /test") != 0 {
		t.Fatal("wrong source deleted")
	}
	if _, err := b.indexDocs["tmp_reindexed_test"]["tenant/original"]; !err {
		t.Fatal("destination missing")
	}
}
func TestRemapMappingCheckedBeforeOriginalRemoval(t *testing.T) {
	s, a, b, req := remapServer(t, "keep", false)
	if code, body := sendRemap(s, req); code != 200 {
		t.Fatal(code, body)
	}
	st := waitMigration(t, s)
	idx := st.History[0].Indices[0]
	b.indexMappings[idx.TargetIndex] = map[string]any{}
	j := &migrateJob{SourceIndex: idx.Name, SourceUUID: idx.SourceUUID, TargetIndex: idx.TargetIndex, TargetUUID: idx.TargetUUID, Remap: idx.Remap}
	j.Remap.Options.OriginalAction = "delete"
	j.Remap.Phase = "copied"
	if err := s.finishRemap(context.Background(), s.Source, s.Target, j); err == nil {
		t.Fatal("accepted changed mapping")
	}
	if countRequest(a, "DELETE /test") != 0 {
		t.Fatal("deleted source")
	}
}

func TestFilteredRemapBacksUpAllOriginalDocuments(t *testing.T) {
	s, a, b, req := remapServer(t, "backup", false)
	a.indexDocs["test"]["tenant/unmatched"] = `{"v":2}`
	a.filterQuery = "v:1"
	a.filterID = "tenant/original"
	req.Query = "v:1"
	if code, body := sendRemap(s, req); code != 200 {
		t.Fatal(code, body)
	}
	st := waitMigration(t, s)
	if st.Active != nil {
		t.Fatalf("%+v", st.Active.Indices)
	}
	idx := st.History[0].Indices[0]
	if idx.VerifiedCount != 1 || idx.Remap.BackupCount != 2 || len(a.indexDocs["original_test"]) != 2 || len(b.indexDocs["tmp_reindexed_test"]) != 1 {
		t.Fatalf("filtered backup lost documents: %+v", idx)
	}
}
func TestRemapResumesAcknowledgedSourceDeletion(t *testing.T) {
	s, a, b, req := remapServer(t, "backup", false)
	if code, body := sendRemap(s, req); code != 200 {
		t.Fatal(code, body)
	}
	st := waitMigration(t, s)
	h := st.History[0]
	idx := h.Indices[0]
	idx.Remap.Phase = "deleted"
	idx.Status = "verifying"
	// Also exercise restoration after interruption during completed-copy verification.
	b.indices[idx.TargetIndex] = true
	a.indices[idx.Remap.BackupIndex] = true
	SetActiveMigration(&ActiveMigration{Source: h.Source, Dest: h.Dest, Indices: []IndexState{idx}})
	if err := SaveState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	if err := LoadState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	st = waitMigration(t, s)
	if st.Active != nil {
		t.Fatalf("%+v", st.Active.Indices)
	}
	if b.indices[idx.TargetIndex] || a.indices[idx.Remap.BackupIndex] || countRequest(a, "DELETE /test") != 1 {
		t.Fatal("incorrect restoration/repeated deletion")
	}
}

func TestRemapInterruptedDestinationRetainsCustomMapping(t *testing.T) {
	s, a, b, req := remapServer(t, "backup", false)
	b.failBulk = true
	if code, body := sendRemap(s, req); code != 200 {
		t.Fatal(code, body)
	}
	st := waitMigration(t, s)
	if st.Active == nil {
		t.Fatal("expected failed copy")
	}
	b.failBulk = false
	if err := LoadState(s.StatePath); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	st = waitMigration(t, s)
	if st.Active != nil {
		t.Fatalf("%+v", st.Active.Indices)
	}
	idx := st.History[0].Indices[0]
	if idx.TargetIndex == "tmp_reindexed_test" {
		t.Fatal("partial destination reused")
	}
	if _, exists := b.indices["tmp_reindexed_test"]; !exists {
		t.Fatal("partial destination removed")
	}
	want, _ := decodeMapping(json.RawMessage(newMapping), "test")
	if !mappingContains(b.indexMappings[idx.TargetIndex], want) || countRequest(a, "DELETE /test") != 1 {
		t.Fatal("mapping or retention choice lost on restart")
	}
}
