package web

import (
	"encoding/json"
	"testing"
)

func TestReviewAbandonRestoresSourceOrKeepsCheckpoint(t *testing.T) {
	for _, mode := range []string{"restore", "close failure", "replaced", "wrong cluster", "reverse"} {
		t.Run(mode, func(t *testing.T) {
			s, a, b := testServer(t)
			closed := true
			source := a
			direction := "source->target"
			if mode == "reverse" {
				source = b
				direction = "target->source"
			}
			source.indices["test"] = true
			source.failClose = mode == "close failure"
			uuid := "index-test"
			if mode == "replaced" {
				uuid = "replaced-original-uuid"
			}
			sourceCluster := "source"
			if mode == "wrong cluster" {
				sourceCluster = "original-source-cluster"
			}
			SetActiveMigration(&ActiveMigration{Direction: direction, Source: ConnectionState{ClusterUUID: sourceCluster}, Dest: ConnectionState{ClusterUUID: "dest"}, Indices: []IndexState{{Name: "test", SourceUUID: uuid, SourceWasClosed: &closed, Status: "running"}}})
			if err := SaveState(s.StatePath); err != nil {
				t.Fatal(err)
			}
			w := call(s, "/api/migrate/abandon", `{}`)
			if mode == "restore" || mode == "reverse" {
				if w.Code != 200 || source.indices["test"] || GetState().Active != nil {
					t.Fatalf("Abandon did not restore source before discarding checkpoint: code=%d open=%t", w.Code, source.indices["test"])
				}
			} else {
				if w.Code != 409 || GetState().Active == nil {
					t.Fatalf("restoration failure discarded checkpoint: code=%d body=%s", w.Code, w.Body.String())
				}
			}
		})
	}
}

func TestReviewClustersRemainIndependent(t *testing.T) {
	s, _, b := testServer(t)
	b.failInfo = true
	w := call(s, "/api/clusters", `{}`)
	var result struct {
		Source *struct {
			UUID string `json:"cluster_uuid"`
		} `json:"source"`
		Target any `json:"target"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || result.Source == nil || result.Source.UUID != "source" || result.Target != nil {
		t.Fatalf("one failed cluster hides the healthy cluster: code=%d body=%s", w.Code, w.Body.String())
	}
}
