package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// Document keeps the metadata needed to copy a hit without changing its identity.
type Document struct {
	ID      string          `json:"_id"`
	Routing string          `json:"_routing,omitempty"`
	Source  json.RawMessage `json:"_source"`
}

func (c *Client) ScrollAll(ctx context.Context, index string, batchSize int, query string) (<-chan []Document, <-chan error) {
	docs := make(chan []Document)
	errs := make(chan error, 1)
	go func() {
		defer close(docs)
		defer close(errs)
		scrollID := ""
		defer func() {
			if scrollID != "" {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = c.clearScroll(cleanup, scrollID)
			}
		}()
		if batchSize <= 0 {
			errs <- fmt.Errorf("batch size must be positive")
			return
		}
		q := map[string]any{"match_all": map[string]any{}}
		if query != "" && query != "*" {
			q = map[string]any{"query_string": map[string]any{"query": query}}
		}
		raw, _ := json.Marshal(map[string]any{"size": batchSize, "query": q, "sort": []string{"_doc"}, "stored_fields": []string{"_routing"}, "_source": true})
		resp, err := c.es.Search(c.es.Search.WithContext(ctx), c.es.Search.WithIndex(index), c.es.Search.WithBody(bytes.NewReader(raw)), c.es.Search.WithScroll(2*time.Minute))
		for {
			if err != nil {
				errs <- fmt.Errorf("scroll request: %w", err)
				return
			}
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				errs <- fmt.Errorf("read scroll: %w", readErr)
				return
			}
			if resp.IsError() {
				errs <- fmt.Errorf("scroll failed (status %d): %s", resp.StatusCode, body)
				return
			}
			// Decode into a fresh value: an error or missing field must never replay a page.
			var page struct {
				ScrollID string `json:"_scroll_id"`
				TimedOut bool   `json:"timed_out"`
				Shards   struct {
					Failed int `json:"failed"`
				} `json:"_shards"`
				Hits *struct {
					Hits []Document `json:"hits"`
				} `json:"hits"`
			}
			if err := json.Unmarshal(body, &page); err != nil {
				errs <- fmt.Errorf("parse scroll: %w", err)
				return
			}
			if page.ScrollID != "" {
				scrollID = page.ScrollID
			}
			if page.TimedOut || page.Shards.Failed > 0 {
				errs <- fmt.Errorf("incomplete scroll response (timeout=%t, failed shards=%d)", page.TimedOut, page.Shards.Failed)
				return
			}
			if page.Hits == nil || page.Hits.Hits == nil {
				errs <- fmt.Errorf("scroll response missing hits")
				return
			}
			if len(page.Hits.Hits) == 0 {
				return
			}
			if page.ScrollID == "" {
				errs <- fmt.Errorf("scroll response missing cursor")
				return
			}
			for _, doc := range page.Hits.Hits {
				if doc.ID == "" || len(doc.Source) == 0 || bytes.Equal(doc.Source, []byte("null")) {
					errs <- fmt.Errorf("scroll hit missing _id or _source")
					return
				}
			}
			select {
			case docs <- page.Hits.Hits:
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			}
			raw, _ = json.Marshal(map[string]any{"scroll": "2m", "scroll_id": scrollID})
			resp, err = c.es.Scroll(c.es.Scroll.WithContext(ctx), c.es.Scroll.WithBody(bytes.NewReader(raw)))
		}
	}()
	return docs, errs
}

func (c *Client) clearScroll(ctx context.Context, scrollID string) error {
	raw, _ := json.Marshal(map[string]string{"scroll_id": scrollID})
	resp, err := c.es.ClearScroll(c.es.ClearScroll.WithContext(ctx), c.es.ClearScroll.WithBody(bytes.NewReader(raw)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return fmt.Errorf("clear scroll failed (status %d)", resp.StatusCode)
	}
	return nil
}

func (c *Client) BulkInsert(ctx context.Context, index string, docs []Document) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, doc := range docs {
		if doc.ID == "" || len(doc.Source) == 0 || bytes.Equal(doc.Source, []byte("null")) {
			return 0, fmt.Errorf("document missing _id or _source")
		}
		metadata := map[string]string{"_id": doc.ID}
		if doc.Routing != "" {
			metadata["routing"] = doc.Routing
		}
		if err := encoder.Encode(map[string]any{"index": metadata}); err != nil {
			return 0, err
		}
		// Encoding RawMessage compacts multiline JSON into one NDJSON record.
		if err := encoder.Encode(doc.Source); err != nil {
			return 0, fmt.Errorf("encode document: %w", err)
		}
	}
	resp, err := c.es.Bulk(bytes.NewReader(buf.Bytes()), c.es.Bulk.WithContext(ctx), c.es.Bulk.WithIndex(index))
	if err != nil {
		return 0, fmt.Errorf("bulk insert: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read bulk response: %w", err)
	}
	if resp.IsError() {
		return 0, fmt.Errorf("bulk failed (status %d): %s", resp.StatusCode, body)
	}
	var result struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int             `json:"status"`
			Error  json.RawMessage `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("parse bulk response: %w", err)
	}
	if len(result.Items) != len(docs) {
		return 0, fmt.Errorf("bulk response has %d items for %d documents", len(result.Items), len(docs))
	}
	succeeded := 0
	for _, item := range result.Items {
		op, ok := item["index"]
		if ok && op.Status >= 200 && op.Status < 300 && (len(op.Error) == 0 || string(op.Error) == "null") {
			succeeded++
		}
	}
	if result.Errors || succeeded != len(docs) {
		return succeeded, fmt.Errorf("bulk insert incomplete: %d/%d documents succeeded", succeeded, len(docs))
	}
	return succeeded, nil
}
