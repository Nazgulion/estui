package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

type ClusterInfo struct {
	ClusterName string     `json:"cluster_name"`
	ClusterUUID string     `json:"cluster_uuid"`
	Version     string     `json:"version"`
	NodeCount   int        `json:"node_count"`
	Host        string     `json:"host"`
	Disk        *DiskStats `json:"disk,omitempty"`
}

func (c *Client) GetClusterInfo(ctx context.Context, host string) (*ClusterInfo, error) {
	info := &ClusterInfo{Host: host}

	resp, err := c.es.Info(c.es.Info.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("cluster info: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read info: %w", err)
	}

	if resp.IsError() {
		return nil, fmt.Errorf("cluster info failed (status %d): %s", resp.StatusCode, body)
	}

	var infoResp struct {
		ClusterName string `json:"cluster_name"`
		ClusterUUID string `json:"cluster_uuid"`
		Version     struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := json.Unmarshal(body, &infoResp); err != nil {
		return nil, fmt.Errorf("parse info: %w", err)
	}

	info.ClusterName = infoResp.ClusterName
	info.ClusterUUID = infoResp.ClusterUUID
	info.Version = infoResp.Version.Number

	ds, err := c.GetDiskStats(ctx)
	if err == nil {
		info.Disk = ds
	}

	nsResp, err := c.es.Nodes.Stats(
		c.es.Nodes.Stats.WithContext(ctx),
	)
	if err == nil {
		defer nsResp.Body.Close()
		body, _ := io.ReadAll(nsResp.Body)
		var ns struct {
			Nodes map[string]any `json:"nodes"`
		}
		if json.Unmarshal(body, &ns) == nil {
			info.NodeCount = len(ns.Nodes)
		}
	}

	return info, nil
}
