package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"estui/internal/config"
	"estui/internal/logging"
)

func TestProductionClientDoesNotRetryAdvancingOperations(t *testing.T) {
	for _, operation := range []string{"scroll", "reindex"} {
		t.Run(operation, func(t *testing.T) {
			calls := 0
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/" {
					_, _ = io.WriteString(w, `{"version":{"number":"8.17.0"}}`)
					return
				}
				if r.URL.Path == "/test/_search" {
					_, _ = io.WriteString(w, `{"_scroll_id":"cursor","hits":{"hits":[{"_id":"1","_source":{"v":1}}]}}`)
					return
				}
				if r.Method == "DELETE" {
					_, _ = io.WriteString(w, `{}`)
					return
				}
				calls++
				if calls == 1 {
					w.WriteHeader(502)
					_, _ = io.WriteString(w, `{"error":"response lost after server processed request"}`)
					return
				}
				if operation == "reindex" {
					_, _ = io.WriteString(w, `{"task":"duplicate"}`)
				} else {
					_, _ = io.WriteString(w, `{"_scroll_id":"cursor","hits":{"hits":[]}}`)
				}
			}))
			defer backend.Close()
			log := logging.New(filepath.Join(t.TempDir(), "test.log"), logging.ERROR)
			defer log.Close()
			client, err := NewClient(config.ESConnection{Hosts: []string{backend.URL}}, log)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "reindex" {
				_, err = client.ReindexAsync(context.Background(), "http://unused.invalid:9200", "test", "copy", "")
				if err == nil || errors.Is(err, ErrReindexRejected) {
					t.Fatal("ambiguous start was not rejected as uncertain", err)
				}
			} else {
				docs, errs := client.ScrollAll(context.Background(), "test", 1, "")
				for range docs {
				}
				failed := false
				for err := range errs {
					if err != nil {
						failed = true
					}
				}
				if !failed {
					t.Fatal("lost scroll response silently accepted")
				}
			}
			if calls != 1 {
				t.Fatalf("request was retried %d times", calls)
			}
		})
	}
}

func TestCreationWithholdsAliasesLifecycleAndPipelines(t *testing.T) {
	var body map[string]any
	client := mockClient(t, func(r *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return response(200, `{"acknowledged":true}`), nil
	})
	detail := &IndexDetail{Settings: map[string]any{"index": map[string]any{"uuid": "old", "verified_read_only": "true", "verified_before_close": "true", "number_of_shards": "3", "lifecycle": map[string]any{"name": "delete-old"}, "blocks": map[string]any{"write": "true"}, "default_pipeline": "transform", "final_pipeline": "drop"}}, Aliases: map[string]any{"live": map[string]any{}}}
	if err := client.CreateIndexFromSource(context.Background(), "copy", detail); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(body)
	for _, forbidden := range []string{"delete-old", "transform", "drop", "aliases", "old", "verified_read_only", "verified_before_close"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatal(string(raw))
		}
	}
	got := body["settings"].(map[string]any)["index"].(map[string]any)
	if got["number_of_shards"] != "3" || got["default_pipeline"] != "_none" || got["final_pipeline"] != "_none" || got["blocks.write"] != false {
		t.Fatal(got)
	}
}
func TestUnsafeDestinationTemplatesRejected(t *testing.T) {
	for _, kind := range []string{"legacy aliases", "composable lifecycle", "component aliases", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			client := mockClient(t, func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/_template":
					if kind == "legacy aliases" {
						return response(200, `{"legacy":{"index_patterns":["logs*"],"aliases":{"live":{}}}}`), nil
					}
					return response(200, `{}`), nil
				case "/_index_template":
					if kind == "composable lifecycle" {
						return response(200, `{"index_templates":[{"name":"managed","index_template":{"index_patterns":["logs*"],"template":{"settings":{"index.lifecycle.name":"delete-old"}}}}]}`), nil
					}
					if kind == "malformed" {
						return response(200, `{}`), nil
					}
					return response(200, `{"index_templates":[{"name":"composed","index_template":{"index_patterns":["logs*"],"composed_of":["part"]}}]}`), nil
				default:
					return response(200, `{"component_templates":[{"component_template":{"template":{"aliases":{"live":{}}}}}]}`), nil
				}
			})
			if err := client.CheckTemplates(context.Background(), "logs-copy"); err == nil {
				t.Fatal("unsafe template accepted")
			}
		})
	}
}
func TestUnacknowledgedIndexOperationsFail(t *testing.T) {
	client := mockClient(t, func(*http.Request) (*http.Response, error) { return response(200, `{"acknowledged":false}`), nil })
	for _, op := range []func(context.Context, string) error{client.OpenIndex, client.CloseIndex, client.DeleteIndex} {
		if err := op(context.Background(), "test"); err == nil {
			t.Fatal("unacknowledged operation accepted")
		}
	}
}

func TestAlreadyBlockedRequiresConfirmedWriteBlock(t *testing.T) {
	for _, tt := range []struct {
		name, ack, settings string
		ok                  bool
	}{
		{"already blocked", `{"acknowledged":true,"shards_acknowledged":false,"indices":[]}`, `{"blocks":{"write":"true"}}`, true},
		{"block disappeared", `{"acknowledged":true,"shards_acknowledged":false,"indices":[]}`, `{"blocks":{"write":"false"}}`, false},
		{"read only is insufficient", `{"acknowledged":true,"shards_acknowledged":false,"indices":[]}`, `{"blocks":{"read_only":"true"}}`, false},
		{"partial result", `{"acknowledged":true,"shards_acknowledged":false,"indices":[{"name":"test","blocked":false}]}`, `{}`, false},
		{"missing results", `{"acknowledged":true,"shards_acknowledged":false}`, `{}`, false},
		{"unacknowledged", `{"acknowledged":false,"shards_acknowledged":false,"indices":[]}`, `{"blocks":{"write":"true"}}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := mockClient(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == "PUT" && r.URL.Path == "/test/_block/write" {
					return response(200, tt.ack), nil
				}
				if r.Method != "GET" || r.URL.Path != "/test" {
					t.Fatalf("unexpected request: %s %s", r.Method, r.URL)
				}
				return response(200, `{"test":{"settings":{"index":`+tt.settings+`},"mappings":{}}}`), nil
			})
			if err := client.BlockWrites(context.Background(), "test"); (err == nil) != tt.ok {
				t.Fatalf("ok=%t, error=%v", tt.ok, err)
			}
		})
	}
}

func TestAlreadyClosedRequiresConfirmedClosedIndex(t *testing.T) {
	for _, tt := range []struct {
		name, ack, indices string
		ok                 bool
	}{
		{"already closed", `{"acknowledged":true,"shards_acknowledged":false,"indices":{}}`, `[{"index":"test","status":"close"}]`, true},
		{"still open", `{"acknowledged":true,"shards_acknowledged":false,"indices":{}}`, `[{"index":"test","status":"open"}]`, false},
		{"missing index", `{"acknowledged":true,"shards_acknowledged":false,"indices":{}}`, `[]`, false},
		{"different index", `{"acknowledged":true,"shards_acknowledged":false,"indices":{}}`, `[{"index":"other","status":"close"}]`, false},
		{"partial result", `{"acknowledged":true,"shards_acknowledged":false,"indices":{"test":{"closed":false}}}`, `[]`, false},
		{"missing results", `{"acknowledged":true,"shards_acknowledged":false}`, `[]`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := mockClient(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == "POST" && r.URL.Path == "/test/_close" {
					return response(200, tt.ack), nil
				}
				if r.Method != "GET" || r.URL.Path != "/_cat/indices/test" {
					t.Fatalf("unexpected request: %s %s", r.Method, r.URL)
				}
				return response(200, tt.indices), nil
			})
			if err := client.CloseIndex(context.Background(), "test"); (err == nil) != tt.ok {
				t.Fatalf("ok=%t, error=%v", tt.ok, err)
			}
		})
	}
}

func TestClientRejectsUnsafeEndpointsBeforeConnecting(t *testing.T) {
	log := logging.New(filepath.Join(t.TempDir(), "test.log"), logging.ERROR)
	defer log.Close()
	for _, cfg := range []config.ESConnection{
		{Hosts: []string{"ftp://example.invalid:9200"}},
		{Hosts: []string{"https://user:secret@example.invalid"}},
		{Hosts: []string{"https://example.invalid"}, Insecure: true},
		{Hosts: []string{"https://one.invalid", "https://two.invalid"}},
	} {
		if _, err := NewClient(cfg, log); err == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
}
