package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

func (c *Client) ScrollAll(ctx context.Context, index string, batchSize int, query string) (<-chan []json.RawMessage, <-chan error) {
	docCh := make(chan []json.RawMessage)
	errCh := make(chan error, 1)

	go func() {
		defer close(docCh)
		defer close(errCh)

		cleanCtx := context.Background()
		scrollID := ""
		total := int64(0)

		// Initial search
		q := map[string]any{"match_all": map[string]any{}}
		if query != "" && query != "*" {
			q = map[string]any{
				"query_string": map[string]any{"query": query},
			}
		}
		reqBody := map[string]any{
			"size":  batchSize,
			"query": q,
			"sort":  []string{"_doc"},
		}
		bodyBytes, _ := json.Marshal(reqBody)

		resp, err := c.es.Search(
			c.es.Search.WithContext(ctx),
			c.es.Search.WithIndex(index),
			c.es.Search.WithBody(bytes.NewReader(bodyBytes)),
			c.es.Search.WithScroll(2 * time.Minute),
		)
		if err != nil {
			errCh <- fmt.Errorf("initial scroll: %w", err)
			return
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			errCh <- fmt.Errorf("read scroll response: %w", err)
			return
		}

		if resp.IsError() {
			errCh <- fmt.Errorf("scroll failed (status %d): %s", resp.StatusCode, string(body))
			return
		}

		var result struct {
			ScrollID string `json:"_scroll_id"`
			Hits     struct {
				Total struct {
					Value int64 `json:"value"`
				} `json:"total"`
				Hits []struct {
					Source json.RawMessage `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			errCh <- fmt.Errorf("parse scroll response: %w", err)
			return
		}

		scrollID = result.ScrollID
		total = result.Hits.Total.Value

		// Send first batch
		batch := make([]json.RawMessage, 0, batchSize)
		for _, hit := range result.Hits.Hits {
			batch = append(batch, hit.Source)
		}
		if len(batch) > 0 {
			select {
			case docCh <- batch:
			case <-ctx.Done():
				_ = c.clearScroll(cleanCtx, scrollID)
				return
			}
		}

		// Continue scrolling
		for {
			if total > 0 && int64(len(batch)) >= total {
				break
			}

			scrollBody := map[string]any{
				"scroll":    "2m",
				"scroll_id": scrollID,
			}
			sb, _ := json.Marshal(scrollBody)

			resp, err := c.es.Scroll(
				c.es.Scroll.WithContext(ctx),
				c.es.Scroll.WithBody(bytes.NewReader(sb)),
			)
			if err != nil {
				errCh <- fmt.Errorf("scroll: %w", err)
				return
			}

			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				errCh <- fmt.Errorf("read scroll: %w", err)
				return
			}

			if err := json.Unmarshal(body, &result); err != nil {
				errCh <- fmt.Errorf("parse scroll: %w", err)
				return
			}

			batch = make([]json.RawMessage, 0, batchSize)
			for _, hit := range result.Hits.Hits {
				batch = append(batch, hit.Source)
			}

			if len(batch) == 0 {
				break
			}

			select {
			case docCh <- batch:
			case <-ctx.Done():
				_ = c.clearScroll(cleanCtx, scrollID)
				return
			}
		}

		_ = c.clearScroll(cleanCtx, scrollID)
	}()

	return docCh, errCh
}

func (c *Client) clearScroll(ctx context.Context, scrollID string) error {
	resp, err := c.es.ClearScroll(
		c.es.ClearScroll.WithContext(ctx),
		c.es.ClearScroll.WithBody(bytes.NewReader([]byte(`{"scroll_id":"`+scrollID+`"}`))),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *Client) BulkInsert(ctx context.Context, index string, docs []json.RawMessage) (int, error) {
	var buf bytes.Buffer
	action := []byte("{\"index\":{}}\n")
	created := 0
	for _, doc := range docs {
		buf.Write(action)
		buf.Write(doc)
		buf.WriteByte('\n')
		created++
	}

	resp, err := c.es.Bulk(
		bytes.NewReader(buf.Bytes()),
		c.es.Bulk.WithContext(ctx),
		c.es.Bulk.WithIndex(index),
	)
	if err != nil {
		return 0, fmt.Errorf("bulk insert: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.IsError() {
		return 0, fmt.Errorf("bulk failed (status %d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Index  string `json:"_index"`
			Status int    `json:"status"`
			Error  struct {
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return created, nil // assume ok if we can't parse
	}

	failures := 0
	for _, item := range result.Items {
		for _, op := range item {
			if op.Status >= 400 {
				failures++
			}
		}
	}

	if failures > 0 {
		return created - failures, fmt.Errorf("%d documents failed to index", failures)
	}

	return created, nil
}
