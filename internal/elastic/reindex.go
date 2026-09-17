package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrReindexRejected means the request was rejected before an async task started.
var ErrReindexRejected = errors.New("reindex request rejected")

type MigrationTask struct {
	ID          string
	SourceIndex string
	TargetIndex string
	Status      string
	Error       string
	Created     int64
	Updated     int64
	Total       int64
	Deleted     int64
	Batches     int64
}

var readOnlySettings = map[string]bool{
	"creation_date":         true,
	"provided_name":         true,
	"uuid":                  true,
	"version":               true,
	"routing_num_shards":    true,
	"verified_before_close": true,
	"store":                 true,
	"history":               true,
	"history.uuid":          true,
	"creation_date_string":  true,
}

func stripReadOnly(settings map[string]any) map[string]any {
	if idx, ok := settings["index"].(map[string]any); ok {
		cleaned := make(map[string]any)
		for k, v := range idx {
			if !readOnlySettings[k] {
				cleaned[k] = v
			}
		}
		return map[string]any{"index": cleaned}
	}
	return settings
}

func (c *Client) CreateIndexFromSource(ctx context.Context, name string, detail *IndexDetail) error {
	body := map[string]any{}

	if detail.Settings != nil {
		body["settings"] = stripReadOnly(detail.Settings)
	}
	if detail.Mappings != nil {
		body["mappings"] = detail.Mappings
	}
	if detail.Aliases != nil {
		body["aliases"] = detail.Aliases
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal index body: %w", err)
	}

	resp, err := c.es.Indices.Create(
		name,
		c.es.Indices.Create.WithContext(ctx),
		c.es.Indices.Create.WithBody(bytes.NewReader(raw)),
	)
	if err != nil {
		return fmt.Errorf("create index %s: %w", name, err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create index failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (c *Client) ReindexAsync(ctx context.Context, sourceHost, sourceIndex, targetIndex string, query string) (*MigrationTask, error) {
	source := map[string]any{
		"index": sourceIndex,
	}
	if sourceHost != "" {
		source["remote"] = map[string]string{"host": sourceHost}
	}
	if query != "" && query != "*" {
		source["query"] = map[string]any{
			"query_string": map[string]any{"query": query},
		}
	}

	body := map[string]any{
		"source": source,
		"dest":   map[string]any{"index": targetIndex, "op_type": "create"},
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal reindex body: %w", err)
	}

	resp, err := c.es.Reindex(
		bytes.NewReader(raw),
		c.es.Reindex.WithContext(ctx),
		c.es.Reindex.WithWaitForCompletion(false),
	)
	if err != nil {
		return nil, fmt.Errorf("start reindex %s -> %s: %w", sourceIndex, targetIndex, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read reindex response: %w", err)
	}

	if resp.IsError() {
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 408 {
			return nil, fmt.Errorf("%w (status %d): %s", ErrReindexRejected, resp.StatusCode, respBody)
		}
		return nil, fmt.Errorf("reindex start outcome unknown (status %d): %s", resp.StatusCode, respBody)
	}

	var result struct {
		Task string `json:"task"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse reindex response: %w", err)
	}

	if result.Task == "" {
		return nil, fmt.Errorf("reindex response missing task ID")
	}

	return &MigrationTask{
		ID:          result.Task,
		SourceIndex: sourceIndex,
		TargetIndex: targetIndex,
		Status:      "running",
	}, nil
}

func (c *Client) GetTaskStatus(ctx context.Context, taskID string) (*MigrationTask, error) {
	resp, err := c.es.Tasks.Get(
		taskID,
		c.es.Tasks.Get.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("get task %s: %w", taskID, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read task response: %w", err)
	}

	if resp.IsError() {
		return nil, fmt.Errorf("task status failed (status %d): %s", resp.StatusCode, string(body))
	}

	var taskResp struct {
		Completed *bool `json:"completed"`
		Task      struct {
			Status map[string]any `json:"status"`
		} `json:"task"`
		Error    json.RawMessage `json:"error"`
		Response *struct {
			Created  *int64 `json:"created"`
			Updated  *int64 `json:"updated"`
			Total    *int64 `json:"total"`
			Deleted  *int64 `json:"deleted"`
			Batches  *int64 `json:"batches"`
			Failures []any  `json:"failures"`
			TimedOut bool   `json:"timed_out"`
		} `json:"response"`
	}
	if err := json.Unmarshal(body, &taskResp); err != nil {
		return nil, fmt.Errorf("parse task response: %w", err)
	}

	if taskResp.Completed == nil {
		return nil, fmt.Errorf("task response missing completed flag")
	}
	task := &MigrationTask{ID: taskID}

	if v, ok := taskResp.Task.Status["created"]; ok {
		task.Created = toInt64(v)
	}
	if v, ok := taskResp.Task.Status["updated"]; ok {
		task.Updated = toInt64(v)
	}
	if v, ok := taskResp.Task.Status["total"]; ok {
		task.Total = toInt64(v)
	}
	if v, ok := taskResp.Task.Status["deleted"]; ok {
		task.Deleted = toInt64(v)
	}
	if v, ok := taskResp.Task.Status["batches"]; ok {
		task.Batches = toInt64(v)
	}

	if *taskResp.Completed {
		if len(taskResp.Error) > 0 && string(taskResp.Error) != "null" {
			task.Status = "failed"
			task.Error = string(taskResp.Error)
		} else if taskResp.Response == nil {
			return nil, fmt.Errorf("completed task response missing result")
		} else if len(taskResp.Response.Failures) > 0 || taskResp.Response.TimedOut {
			task.Status = "failed"
			task.Error = fmt.Sprintf("reindex failed: timeout=%t, failures=%v", taskResp.Response.TimedOut, taskResp.Response.Failures)
		} else {
			task.Status = "completed"
		}
	} else {
		task.Status = "running"
	}

	if *taskResp.Completed && taskResp.Response != nil {
		result := taskResp.Response
		if result.Created != nil {
			task.Created = *result.Created
		}
		if result.Updated != nil {
			task.Updated = *result.Updated
		}
		if result.Total != nil {
			task.Total = *result.Total
		}
		if result.Deleted != nil {
			task.Deleted = *result.Deleted
		}
		if result.Batches != nil {
			task.Batches = *result.Batches
		}
	}
	return task, nil
}

func (c *Client) WaitForTask(ctx context.Context, taskID string, tick time.Duration, progress func(*MigrationTask)) (*MigrationTask, error) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			task, err := c.GetTaskStatus(ctx, taskID)
			if err != nil {
				return nil, err
			}
			if progress != nil {
				progress(task)
			}
			if task.Status != "running" {
				return task, nil
			}
		}
	}
}

func toInt64(v any) int64 {
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case json.Number:
		n, _ := val.Int64()
		return n
	}
	return 0
}
