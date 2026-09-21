package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"estui/internal/elastic"
)

func (s *Server) handlePreviewQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		Host  string `json:"host"`
		Index string `json:"index"`
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.Index == "" || (req.Host != "" && req.Host != "source" && req.Host != "target") {
		httpJSON(w, http.StatusBadRequest, map[string]string{"error": "index and a valid source/target host are required"})
		return
	}
	client := s.Source
	if req.Host == "target" {
		client = s.Target
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	count, err := previewCount(ctx, client, req.Index, req.Query)
	if err != nil {
		s.Log.Error("preview", "query preview for %s failed: %v", req.Index, err)
		status := http.StatusBadGateway
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		httpJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	httpJSON(w, http.StatusOK, map[string]any{"index": req.Index, "count": count, "query": req.Query})
}

func previewCount(ctx context.Context, client *elastic.Client, index, query string) (count int64, err error) {
	status, err := previewIndexStatus(ctx, client, index)
	if err != nil {
		return 0, err
	}
	if status == "close" {
		// Register restoration before opening: the server may open the index even if
		// the caller times out before receiving the acknowledgement.
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if closeErr := client.CloseIndex(cleanup, index); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to restore closed index %s: %w", index, closeErr))
			}
		}()
		if err := client.OpenIndex(ctx, index); err != nil {
			return 0, fmt.Errorf("open closed index: %w", err)
		}
		if err := waitPreviewIndexOpen(ctx, client, index); err != nil {
			return 0, err
		}
	}

	q := map[string]any{"match_all": map[string]any{}}
	if query != "" && query != "*" {
		q = map[string]any{"query_string": map[string]any{"query": query}}
	}
	body, _ := json.Marshal(map[string]any{"query": q})
	resp, err := client.ES().Count(client.ES().Count.WithContext(ctx), client.ES().Count.WithIndex(index), client.ES().Count.WithBody(bytes.NewReader(body)))
	if err != nil {
		return 0, fmt.Errorf("query preview: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read query preview: %w", err)
	}
	if resp.IsError() {
		return 0, fmt.Errorf("query preview failed (status %d): %s", resp.StatusCode, raw)
	}
	var result struct {
		Count  *int64 `json:"count"`
		Shards struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return 0, fmt.Errorf("parse query preview: %w", err)
	}
	if result.Count == nil || *result.Count < 0 {
		return 0, fmt.Errorf("query preview response missing a valid count")
	}
	if result.Shards.Failed > 0 {
		return 0, fmt.Errorf("query preview incomplete: %d shards failed", result.Shards.Failed)
	}
	return *result.Count, nil
}

func previewIndexStatus(ctx context.Context, client *elastic.Client, index string) (string, error) {
	indices, err := client.ListIndices(ctx)
	if err != nil {
		return "", fmt.Errorf("check index status: %w", err)
	}
	for _, idx := range indices {
		if idx.Name == index {
			if idx.Status != "open" && idx.Status != "close" {
				return "", fmt.Errorf("unknown status for index %s: %q", index, idx.Status)
			}
			return idx.Status, nil
		}
	}
	return "", fmt.Errorf("index %s not found", index)
}

func waitPreviewIndexOpen(ctx context.Context, client *elastic.Client, index string) error {
	readyCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		status, err := previewIndexStatus(readyCtx, client, index)
		if err != nil {
			return err
		}
		if status == "open" {
			return nil
		}
		select {
		case <-readyCtx.Done():
			return fmt.Errorf("wait for index %s to open: %w", index, readyCtx.Err())
		case <-ticker.C:
		}
	}
}
