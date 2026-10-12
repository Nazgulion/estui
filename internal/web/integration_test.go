package web

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"estui/internal/config"
	"estui/internal/elastic"
	"estui/internal/logging"
)

// Opt-in only. Never reads config.yaml. Use two disposable clusters: this test
// creates, write-blocks, opens/closes, and deletes uniquely named test indices.
func TestElasticsearchIntegration(t *testing.T) {
	sourceURL, targetURL := os.Getenv("ESTUI_TEST_SOURCE"), os.Getenv("ESTUI_TEST_TARGET")
	if sourceURL == "" || targetURL == "" {
		t.Skip("set ESTUI_TEST_SOURCE and ESTUI_TEST_TARGET to disposable Elasticsearch clusters")
	}
	log := logging.New(filepath.Join(t.TempDir(), "test.log"), logging.ERROR)
	defer log.Close()
	sourceCfg := config.ESConnection{Hosts: []string{sourceURL}, APIKey: os.Getenv("ESTUI_TEST_SOURCE_API_KEY"), CACert: os.Getenv("ESTUI_TEST_SOURCE_CA")}
	targetCfg := config.ESConnection{Hosts: []string{targetURL}, APIKey: os.Getenv("ESTUI_TEST_TARGET_API_KEY"), CACert: os.Getenv("ESTUI_TEST_TARGET_CA")}
	source, err := elastic.NewClient(sourceCfg, log)
	if err != nil {
		t.Fatal(err)
	}
	target, err := elastic.NewClient(targetCfg, log)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"new", "rename", "overwrite", "local", "remap-cross", "remap-local"} {
		t.Run(mode, func(t *testing.T) {
			target, targetCfg, targetURL := target, targetCfg, targetURL
			if mode == "local" || mode == "remap-local" {
				target, targetCfg, targetURL = source, sourceCfg, sourceURL
			}
			existing := mode == "rename" || mode == "overwrite"
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			token := make([]byte, 12)
			if _, err := rand.Read(token); err != nil {
				t.Fatal(err)
			}
			name := fmt.Sprintf("estui-integration-%x", token)
			type ownedIndex struct {
				client     *elastic.Client
				name, uuid string
			}
			var owned []ownedIndex
			defer func() {
				for _, item := range owned {
					cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					uuid, err := migrationIndexUUID(cleanup, item.client, item.name)
					if err == nil && uuid == item.uuid {
						if err := item.client.DeleteIndex(cleanup, item.name); err != nil {
							t.Errorf("cleanup %s: %v", item.name, err)
						}
					}
					cancel()
				}
			}()
			create := func(c *elastic.Client) {
				response, err := c.ES().Indices.Create(name, c.ES().Indices.Create.WithContext(ctx), c.ES().Indices.Create.WithBody(bytes.NewBufferString(`{"settings":{"number_of_shards":1,"number_of_replicas":0}}`)))
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				if response.IsError() {
					t.Fatal(response.String())
				}
				uuid, err := migrationIndexUUID(ctx, c, name)
				if err != nil {
					t.Fatal(err)
				}
				owned = append(owned, ownedIndex{c, name, uuid})
			}
			create(source)
			docs := []elastic.Document{{ID: "one", Routing: "tenant-a", Source: json.RawMessage(`{"large":9007199254740993,"text":"first"}`)}, {ID: "two", Routing: "tenant-b", Source: json.RawMessage(`{"large":9007199254740992,"text":"second"}`)}}
			if _, err := source.BulkInsert(ctx, name, docs); err != nil {
				t.Fatal(err)
			}
			if err := source.BlockWrites(ctx, name); err != nil {
				t.Fatal(err)
			}
			if err := source.CloseIndex(ctx, name); err != nil {
				t.Fatal(err)
			}
			if existing {
				create(target)
				if _, err := target.BulkInsert(ctx, name, []elastic.Document{{ID: "keep", Source: json.RawMessage(`{"original":true}`)}}); err != nil {
					t.Fatal(err)
				}
				if mode == "overwrite" {
					if err := target.BlockWrites(ctx, name); err != nil {
						t.Fatal(err)
					}
				}
			}
			migrateMu.Lock()
			migrateActive = false
			migrateJobs = nil
			migrateMu.Unlock()
			restoreState(MigrationState{})
			s := New(source, target, sourceURL, targetURL, sourceCfg, targetCfg, "", log, 8080)
			s.StatePath = filepath.Join(t.TempDir(), "state.json")
			request := map[string]any{"indices": []string{name}, "collision_action": "rename"}
			if mode == "overwrite" {
				a, b, err := s.validateClusters(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				request["collision_action"], request["source_uuid"], request["target_uuid"] = "overwrite", a.ClusterUUID, b.ClusterUUID
				request["overwrite_uuids"] = map[string]string{name: owned[1].uuid}
			}
			if strings.HasPrefix(mode, "remap-") {
				a, b, err := s.validateClusters(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				request["source_uuid"], request["target_uuid"] = a.ClusterUUID, b.ClusterUUID
				request["query"] = "text:first"
				request["remaps"] = map[string]RemapOptions{name: {Mapping: json.RawMessage(`{"properties":{"text":{"type":"keyword"},"large":{"type":"long"}}}`), Destination: "tmp_reindexed_" + name, OriginalAction: "backup", BackupIndex: "original_" + name, SourceUUID: owned[0].uuid, ConfirmRemoval: true}}
			}
			raw, _ := json.Marshal(request)
			if w := call(s, "/api/migrate", string(raw)); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			for {
				migrateMu.Lock()
				active := migrateActive
				migrateMu.Unlock()
				if !active {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(100 * time.Millisecond):
				}
			}
			state := GetState()
			// Record only checkpointed UUIDs for cleanup, even when the migration fails.
			var idx IndexState
			if state.Active != nil {
				idx = state.Active.Indices[0]
			} else if len(state.History) > 0 {
				idx = state.History[0].Indices[0]
			}
			if idx.TargetUUID != "" {
				owned = append(owned, ownedIndex{target, idx.TargetIndex, idx.TargetUUID})
			}
			if idx.StageUUID != "" && idx.StageIndex != idx.TargetIndex {
				owned = append(owned, ownedIndex{target, idx.StageIndex, idx.StageUUID})
			}
			if idx.Remap.BackupUUID != "" {
				owned = append(owned, ownedIndex{source, idx.Remap.BackupIndex, idx.Remap.BackupUUID})
			}
			wantCount := int64(2)
			if strings.HasPrefix(mode, "remap-") {
				wantCount = 1
			}
			if state.Active != nil || idx.Status != "completed" || idx.VerifiedCount != wantCount || idx.VerifiedHash == "" {
				t.Fatalf("migration failed: %+v", idx)
			}
			if (mode == "rename" || mode == "local") && idx.TargetIndex == name {
				t.Fatal("existing destination reused")
			}
			if (mode == "new" || mode == "overwrite") && idx.TargetIndex != name {
				t.Fatal("unexpected destination name")
			}
			if mode == "rename" {
				uuid, err := migrationIndexUUID(ctx, target, name)
				if err != nil || uuid != owned[1].uuid {
					t.Fatal("original destination changed", err)
				}
			}
			// Simulate restart after the last index checkpoint but before whole-job
			// completion. Continue must re-read the saved copy and restore closed
			// states, including when the mapping workflow removed the original.
			history := state.History[0]
			SetActiveMigration(&ActiveMigration{Source: history.Source, Dest: history.Dest, Query: history.Query, Direction: history.Direction, Indices: history.Indices})
			if err := SaveState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			restoreState(MigrationState{})
			if err := LoadState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			if w := call(s, "/api/migrate/continue", `{}`); w.Code != 200 || GetState().Active != nil {
				t.Fatal("continue failed:", w.Code, w.Body.String())
			}
			if status, err := previewIndexStatus(ctx, target, idx.TargetIndex); err != nil || status != "close" {
				t.Fatal("destination state not restored", err)
			}
			if strings.HasPrefix(mode, "remap-") {
				if !idx.Remap.SourceDeleted || idx.Remap.BackupCount != 2 {
					t.Fatalf("incomplete original backup: %+v", idx)
				}
				j := &migrateJob{SourceIndex: name, SourceUUID: idx.SourceUUID, TargetIndex: idx.TargetIndex, TargetUUID: idx.TargetUUID, VerifiedHash: idx.VerifiedHash, VerifiedCount: idx.VerifiedCount, Remap: idx.Remap}
				if err := s.verifyRemapAfterDeletion(ctx, source, target, j); err != nil {
					t.Fatal(err)
				}
				indices, err := source.ListIndices(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, i := range indices {
					if i.Name == name {
						t.Fatal("original name still exists")
					}
				}
				return
			}
			if status, err := previewIndexStatus(ctx, source, name); err != nil || status != "close" {
				t.Fatal("source state not restored", err)
			}
		})
	}
}
