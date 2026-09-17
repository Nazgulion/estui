package elastic

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Header: http.Header{"X-Elastic-Product": []string{"Elasticsearch"}}, Body: io.NopCloser(strings.NewReader(body))}
}
func mockClient(t *testing.T, f roundTripFunc) *Client {
	t.Helper()
	es, err := elasticsearch.NewClient(elasticsearch.Config{Transport: f, DisableRetry: true})
	if err != nil {
		t.Fatal(err)
	}
	return &Client{es: es}
}

func TestTaskStatus(t *testing.T) {
	for _, tc := range []struct{ name, body, status string }{
		{"top-level error", `{"completed":true,"task":{"status":{"created":1,"total":10}},"error":{"reason":"disconnected"}}`, "failed"},
		{"unstructured error", `{"completed":true,"error":"unknown failure"}`, "failed"},
		{"item failure", `{"completed":true,"response":{"failures":[{"cause":"mapping error"}]}}`, "failed"},
		{"timeout", `{"completed":true,"response":{"timed_out":true}}`, "failed"},
		{"success", `{"completed":true,"error":null,"response":{"failures":[]}}`, "completed"},
		{"running", `{"completed":false,"task":{"status":{"created":1,"total":10}}}`, "running"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := mockClient(t, func(*http.Request) (*http.Response, error) { return response(200, tc.body), nil })
			task, err := c.GetTaskStatus(context.Background(), "task:1")
			if err != nil {
				t.Fatal(err)
			}
			if task.Status != tc.status {
				t.Fatalf("status=%s want %s", task.Status, tc.status)
			}
			if tc.status == "failed" && task.Error == "" {
				t.Fatal("failure reason lost")
			}
		})
	}
}

func TestScrollRotatesCursorAndPreservesMetadata(t *testing.T) {
	calls := 0
	cleared := ""
	c := mockClient(t, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		if r.Method == "DELETE" {
			cleared = string(raw)
			return response(200, `{}`), nil
		}
		calls++
		switch calls {
		case 1:
			return response(200, `{"_scroll_id":"first","hits":{"hits":[{"_id":"a","_routing":"tenant","_source":{"v":1}}]}}`), nil
		case 2:
			if !strings.Contains(string(raw), `"first"`) {
				t.Errorf("wrong cursor: %s", raw)
			}
			return response(200, `{"_scroll_id":"second","hits":{"hits":[{"_id":"b","_source":{"v":2}}]}}`), nil
		default:
			if !strings.Contains(string(raw), `"second"`) {
				t.Errorf("stale cursor: %s", raw)
			}
			return response(200, `{"_scroll_id":"final","hits":{"hits":[]}}`), nil
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	docs, errs := c.ScrollAll(ctx, "test", 1, "status:active")
	var got []Document
	for batch := range docs {
		got = append(got, batch...)
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 || got[0].ID != "a" || got[0].Routing != "tenant" || got[1].ID != "b" {
		t.Fatalf("metadata lost: %+v", got)
	}
	if !strings.Contains(cleared, `"final"`) {
		t.Fatalf("cleanup used stale cursor: %s", cleared)
	}
}

func TestScrollRejectsIncompletePages(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		body string
	}{
		{"expired cursor", 404, `{"error":{"type":"search_context_missing_exception"}}`},
		{"invalid JSON", 200, `{`},
		{"missing hits", 200, `{}`},
		{"timeout", 200, `{"timed_out":true,"hits":{"hits":[]}}`},
		{"failed shard", 200, `{"_shards":{"failed":1},"hits":{"hits":[]}}`},
		{"missing identity", 200, `{"_scroll_id":"s","hits":{"hits":[{"_source":{"v":1}}]}}`},
		{"missing source", 200, `{"_scroll_id":"s","hits":{"hits":[{"_id":"a"}]}}`},
		{"missing cursor", 200, `{"hits":{"hits":[{"_id":"a","_source":{}}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, cleared := 0, false
			c := mockClient(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == "DELETE" {
					cleared = true
					return response(200, `{}`), nil
				}
				calls++
				if calls == 1 {
					return response(200, `{"_scroll_id":"first","hits":{"hits":[{"_id":"a","_source":{}}]}}`), nil
				}
				return response(tc.code, tc.body), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			docs, errs := c.ScrollAll(ctx, "test", 1, "")
			count := 0
			for batch := range docs {
				count += len(batch)
			}
			gotErr := <-errs
			if gotErr == nil || count != 1 || !cleared {
				t.Fatalf("count=%d err=%v cleared=%t", count, gotErr, cleared)
			}
		})
	}
}

func TestScrollCancellationCleansUp(t *testing.T) {
	cleaned := false
	c := mockClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method == "DELETE" {
			cleaned = true
			return response(200, `{}`), nil
		}
		return response(200, `{"_scroll_id":"cursor","hits":{"hits":[{"_id":"a","_source":{}}]}}`), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	docs, errs := c.ScrollAll(ctx, "test", 1, "")
	<-docs
	cancel()
	for range docs {
	}
	if err := <-errs; err == nil {
		t.Fatal("cancellation treated as success")
	}
	if !cleaned {
		t.Fatal("cursor leaked")
	}
}

func TestBulkPreservesIdentityAndRouting(t *testing.T) {
	c := mockClient(t, func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		if len(lines) != 2 {
			t.Fatalf("invalid NDJSON: %s", raw)
		}
		var meta map[string]map[string]string
		if err := json.Unmarshal([]byte(lines[0]), &meta); err != nil {
			t.Fatal(err)
		}
		if meta["index"]["_id"] != "original" || meta["index"]["routing"] != "tenant" {
			t.Fatalf("metadata lost: %s", raw)
		}
		return response(200, `{"errors":false,"items":[{"index":{"status":201}}]}`), nil
	})
	n, err := c.BulkInsert(context.Background(), "test", []Document{{ID: "original", Routing: "tenant", Source: json.RawMessage("{\n\"v\":1\n}")}})
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestBulkRejectsUnconfirmedWrites(t *testing.T) {
	for _, body := range []string{`{`, `{}`, `{"items":[]}`, `{"errors":true,"items":[{"index":{"status":400,"error":{"reason":"mapping"}}}]}`, `{"items":[{"index":{"status":0}}]}`} {
		c := mockClient(t, func(*http.Request) (*http.Response, error) { return response(200, body), nil })
		if _, err := c.BulkInsert(context.Background(), "test", []Document{{ID: "a", Source: json.RawMessage(`{}`)}}); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}

func TestIndexMutationsRejectHTTPFailures(t *testing.T) {
	c := mockClient(t, func(*http.Request) (*http.Response, error) { return response(403, `{"error":"forbidden"}`), nil })
	for name, op := range map[string]func(context.Context, string) error{"open": c.OpenIndex, "close": c.CloseIndex, "delete": c.DeleteIndex} {
		if err := op(context.Background(), "test"); err == nil {
			t.Errorf("%s accepted HTTP 403", name)
		}
	}
}

func TestTaskResultCountersAndMalformedResponses(t *testing.T) {
	c := mockClient(t, func(*http.Request) (*http.Response, error) {
		return response(200, `{"completed":true,"response":{"created":5,"total":5,"failures":[]}}`), nil
	})
	task, err := c.GetTaskStatus(context.Background(), "task:1")
	if err != nil || task.Status != "completed" || task.Created != 5 || task.Total != 5 {
		t.Fatalf("task=%+v err=%v", task, err)
	}
	for _, body := range []string{`{}`, `{"completed":true}`, `null`} {
		c := mockClient(t, func(*http.Request) (*http.Response, error) { return response(200, body), nil })
		if _, err := c.GetTaskStatus(context.Background(), "task:1"); err == nil {
			t.Fatalf("accepted malformed task: %s", body)
		}
	}
}
