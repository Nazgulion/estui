package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
)

type IndexInfo struct {
	Name      string
	Health    string
	Status    string
	DocCount  int64
	SizeBytes *int64
	CreatedAt *int64
	SizeHuman string
	Primary   int
	Replica   int
	Shards    int
	UUID      string
}

type IndexDetail struct {
	Info     IndexInfo
	Settings map[string]any
	Mappings map[string]any
	Aliases  map[string]any
}

func (c *Client) ListIndices(ctx context.Context) ([]IndexInfo, error) {
	resp, err := c.es.Cat.Indices(
		c.es.Cat.Indices.WithContext(ctx),
		c.es.Cat.Indices.WithFormat("json"),
		c.es.Cat.Indices.WithH("health", "status", "index", "uuid", "pri", "rep", "docs.count", "store.size", "creation.date"),
		c.es.Cat.Indices.WithBytes("b"),
		c.es.Cat.Indices.WithS("index"),
	)
	if err != nil {
		return nil, fmt.Errorf("list indices: %w", err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return nil, fmt.Errorf("list indices failed (status %d)", resp.StatusCode)
	}

	type catIndex struct {
		Index     *string `json:"index"`
		Health    *string `json:"health"`
		Status    *string `json:"status"`
		UUID      *string `json:"uuid"`
		Pri       *string `json:"pri"`
		Rep       *string `json:"rep"`
		DocsCount *string `json:"docs.count"`
		StoreSize *string `json:"store.size"`
		CreatedAt *string `json:"creation.date"`
	}

	var raw []catIndex
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode cat indices: %w", err)
	}

	var indices []IndexInfo
	for _, ci := range raw {
		if ci.Index == nil || strings.HasPrefix(*ci.Index, ".") {
			continue
		}
		info := IndexInfo{
			Name:      *ci.Index,
			Health:    strVal(ci.Health),
			Status:    strVal(ci.Status),
			UUID:      strVal(ci.UUID),
			Primary:   parseInt(strVal(ci.Pri, "0")),
			Replica:   parseInt(strVal(ci.Rep, "0")),
			Shards:    parseInt(strVal(ci.Pri, "0")) + parseInt(strVal(ci.Rep, "0")),
			CreatedAt: optionalCatNumber(ci.CreatedAt),
			SizeBytes: optionalCatNumber(ci.StoreSize),
		}
		if ci.DocsCount != nil {
			info.DocCount = parseInt64(*ci.DocsCount)
		}
		if info.SizeBytes != nil {
			info.SizeHuman = formatBytes(*info.SizeBytes)
		}
		indices = append(indices, info)
	}
	return indices, nil
}

// CAT omits some metrics for closed indices. Preserve unknown values rather
// than sorting them as zero-byte indices or as Unix-epoch creation dates.
func optionalCatNumber(value *string) *int64 {
	if value == nil {
		return nil
	}
	number, err := strconv.ParseInt(*value, 10, 64)
	if err != nil || number < 0 {
		return nil
	}
	return &number
}

func (c *Client) GetIndexDetail(ctx context.Context, name string) (*IndexDetail, error) {
	resp, err := c.es.Indices.Get(
		[]string{name},
		c.es.Indices.Get.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("get index %s: %w", name, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read index detail: %w", err)
	}

	if resp.IsError() {
		return nil, fmt.Errorf("get index %s failed (status %d): %s", name, resp.StatusCode, string(body))
	}

	var raw map[string]struct {
		Settings map[string]any `json:"settings"`
		Mappings map[string]any `json:"mappings"`
		Aliases  map[string]any `json:"aliases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse index detail: %w", err)
	}

	idxData, ok := raw[name]
	if !ok {
		return nil, fmt.Errorf("index %s not found", name)
	}

	return &IndexDetail{
		Settings: idxData.Settings,
		Mappings: idxData.Mappings,
		Aliases:  idxData.Aliases,
	}, nil
}

func (c *Client) Count(ctx context.Context, name string) (int64, error) {
	resp, err := c.es.Count(
		c.es.Count.WithContext(ctx),
		c.es.Count.WithIndex(name),
	)
	if err != nil {
		return 0, fmt.Errorf("count %s: %w", name, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read count: %w", err)
	}

	var result struct {
		Count int64 `json:"count"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("parse count: %w", err)
	}
	return result.Count, nil
}

func (c *Client) OpenIndex(ctx context.Context, name string) error {
	resp, err := c.es.Indices.Open([]string{name}, c.es.Indices.Open.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("open index %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("open index %s failed (status %d): %s", name, resp.StatusCode, body)
	}
	var result struct {
		Acknowledged       bool  `json:"acknowledged"`
		ShardsAcknowledged *bool `json:"shards_acknowledged"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("invalid acknowledgement: %w", err)
	}
	if !result.Acknowledged || (result.ShardsAcknowledged != nil && !*result.ShardsAcknowledged) {
		return fmt.Errorf("index operation was not fully acknowledged")
	}
	return nil
}

func (c *Client) CloseIndex(ctx context.Context, name string) error {
	resp, err := c.es.Indices.Close([]string{name}, c.es.Indices.Close.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("close index %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("close index %s failed (status %d): %s", name, resp.StatusCode, body)
	}
	var result struct {
		Acknowledged       bool                       `json:"acknowledged"`
		ShardsAcknowledged *bool                      `json:"shards_acknowledged"`
		Indices            map[string]json.RawMessage `json:"indices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("invalid acknowledgement: %w", err)
	}
	if !result.Acknowledged || (result.ShardsAcknowledged != nil && !*result.ShardsAcknowledged) {
		// Closing an already-closed index is an acknowledged no-op with no
		// shard results. Confirm its state instead of reporting a false failure.
		if result.Acknowledged && result.Indices != nil && len(result.Indices) == 0 {
			var indices []struct {
				Name   string `json:"index"`
				Status string `json:"status"`
			}
			if err := c.jsonRequest(ctx, "GET", "/_cat/indices/"+url.PathEscape(name)+"?format=json&h=index,status&expand_wildcards=all", nil, &indices); err != nil {
				return err
			}
			if len(indices) == 1 && indices[0].Name == name && indices[0].Status == "close" {
				return nil
			}
		}
		return fmt.Errorf("index operation was not fully acknowledged")
	}
	return nil
}

func (c *Client) DeleteIndex(ctx context.Context, name string) error {
	resp, err := c.es.Indices.Delete([]string{name}, c.es.Indices.Delete.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("delete index %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete index %s failed (status %d): %s", name, resp.StatusCode, body)
	}
	var result struct {
		Acknowledged       bool  `json:"acknowledged"`
		ShardsAcknowledged *bool `json:"shards_acknowledged"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("invalid acknowledgement: %w", err)
	}
	if !result.Acknowledged || (result.ShardsAcknowledged != nil && !*result.ShardsAcknowledged) {
		return fmt.Errorf("index operation was not fully acknowledged")
	}
	return nil
}

type DiskStats struct {
	TotalBytes     int64  `json:"total_bytes"`
	AvailableBytes int64  `json:"available_bytes"`
	UsedPercent    int    `json:"used_percent"`
	TotalHuman     string `json:"total_human"`
	AvailableHuman string `json:"available_human"`
	WatermarkLow   string `json:"watermark_low"`
	WatermarkHigh  string `json:"watermark_high"`
	WatermarkFlood string `json:"watermark_flood"`
}

func (c *Client) GetDiskStats(ctx context.Context) (*DiskStats, error) {
	resp, err := c.es.Cluster.Stats(
		c.es.Cluster.Stats.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("cluster stats: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read cluster stats: %w", err)
	}

	var stats struct {
		Nodes struct {
			FS struct {
				TotalInBytes     int64 `json:"total_in_bytes"`
				AvailableInBytes int64 `json:"available_in_bytes"`
			} `json:"fs"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		return nil, fmt.Errorf("parse cluster stats: %w", err)
	}

	ds := &DiskStats{
		TotalBytes:     stats.Nodes.FS.TotalInBytes,
		AvailableBytes: stats.Nodes.FS.AvailableInBytes,
		TotalHuman:     formatBytes(stats.Nodes.FS.TotalInBytes),
		AvailableHuman: formatBytes(stats.Nodes.FS.AvailableInBytes),
	}
	if ds.TotalBytes > 0 {
		ds.UsedPercent = int(float64(ds.TotalBytes-ds.AvailableBytes) / float64(ds.TotalBytes) * 100)
	}

	settingsResp, err := c.es.Cluster.GetSettings(
		c.es.Cluster.GetSettings.WithContext(ctx),
		c.es.Cluster.GetSettings.WithIncludeDefaults(true),
	)
	if err == nil {
		defer settingsResp.Body.Close()
		settingsBody, _ := io.ReadAll(settingsResp.Body)
		var s struct {
			Defaults struct {
				Cluster struct {
					Routing struct {
						Allocation struct {
							Disk struct {
								Watermark struct {
									Low        string `json:"low"`
									High       string `json:"high"`
									FloodStage string `json:"flood_stage"`
								} `json:"watermark"`
							} `json:"disk"`
						} `json:"allocation"`
					} `json:"routing"`
				} `json:"cluster"`
			} `json:"defaults"`
		}
		json.Unmarshal(settingsBody, &s)
		ds.WatermarkLow = s.Defaults.Cluster.Routing.Allocation.Disk.Watermark.Low
		ds.WatermarkHigh = s.Defaults.Cluster.Routing.Allocation.Disk.Watermark.High
		ds.WatermarkFlood = s.Defaults.Cluster.Routing.Allocation.Disk.Watermark.FloodStage
	}

	return ds, nil
}

func strVal(s *string, fallback ...string) string {
	if s == nil {
		if len(fallback) > 0 {
			return fallback[0]
		}
		return ""
	}
	return *s
}

func parseInt(s string) int {
	var v int
	fmt.Sscanf(s, "%d", &v)
	return v
}

func parseInt64(s string) int64 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}

func parseSizeString(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	var value float64
	var unit string
	fmt.Sscanf(s, "%f%s", &value, &unit)
	unit = strings.ToLower(unit)
	switch unit {
	case "b", "":
		return int64(value)
	case "kb":
		return int64(value * 1024)
	case "mb":
		return int64(value * 1024 * 1024)
	case "gb":
		return int64(value * 1024 * 1024 * 1024)
	case "tb":
		return int64(value * 1024 * 1024 * 1024 * 1024)
	case "pb":
		return int64(value * 1024 * 1024 * 1024 * 1024 * 1024)
	default:
		// Try parsing as bytes
		var v int64
		fmt.Sscanf(s, "%d", &v)
		return v
	}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
