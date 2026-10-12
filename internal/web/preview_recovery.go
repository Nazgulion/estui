package web

import (
	"context"
	"fmt"
	"time"
)

type PreviewRecovery struct {
	Side        string `json:"side"`
	ClusterUUID string `json:"cluster_uuid"`
	Index       string `json:"index"`
	IndexUUID   string `json:"index_uuid"`
}

func setPreviewRecovery(value *PreviewRecovery) {
	stateMu.Lock()
	defer stateMu.Unlock()
	state.Preview = value
}

// Record the restoration obligation before opening a closed production index.
func (s *Server) acquirePreview(side, index string) (func() error, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, cfg := s.Source, s.SourceCfg
	if side == "target" {
		client, cfg = s.Target, s.TargetCfg
	}
	if len(cfg.Hosts) != 1 {
		return nil, fmt.Errorf("preview requires a configured cluster")
	}
	info, err := client.GetClusterInfo(ctx, cfg.Hosts[0])
	if err != nil {
		return nil, err
	}
	if info.ClusterUUID == "" || info.ClusterUUID == "_na_" {
		return nil, fmt.Errorf("preview cluster UUID is unknown")
	}
	uuid, err := migrationIndexUUID(ctx, client, index)
	if err != nil {
		return nil, err
	}
	pending := &PreviewRecovery{Side: side, ClusterUUID: info.ClusterUUID, Index: index, IndexUUID: uuid}
	setPreviewRecovery(pending)
	if err := SaveState(s.StatePath); err != nil {
		return nil, err
	}
	return s.recoverPreview, nil
}

func (s *Server) recoverPreview() error {
	pending := GetState().Preview
	if pending == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, cfg := s.Source, s.SourceCfg
	if pending.Side == "target" {
		client, cfg = s.Target, s.TargetCfg
	}
	if len(cfg.Hosts) != 1 {
		return fmt.Errorf("cannot restore preview without its configured cluster")
	}
	info, err := client.GetClusterInfo(ctx, cfg.Hosts[0])
	if err != nil {
		return err
	}
	if info.ClusterUUID != pending.ClusterUUID {
		return fmt.Errorf("pending preview belongs to a different cluster; reconnect using the original configuration")
	}
	if err := restoreClosed(ctx, client, pending.Index, pending.IndexUUID); err != nil {
		return err
	}
	setPreviewRecovery(nil)
	if err := SaveState(s.StatePath); err != nil {
		setPreviewRecovery(pending)
		return err
	}
	return nil
}
