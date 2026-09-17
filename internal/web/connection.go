package web

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"estui/internal/config"
	"estui/internal/elastic"
)

type connectionHandler struct {
	s      *Server
	srcCfg *config.ESConnection
	tgtCfg *config.ESConnection
}

func (s *Server) handleTestSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		Host string `json:"host"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.Host == "" {
		httpJSON(w, 400, map[string]string{"error": "host required"})
		return
	}

	cfg := s.SourceCfg
	if len(cfg.Hosts) == 0 || cfg.Hosts[0] != req.Host {
		// Credentials and remote host overrides belong to the configured cluster.
		cfg = config.ESConnection{Hosts: []string{req.Host}}
	}
	client, err := elastic.NewClient(cfg, s.Log)
	if err != nil {
		httpJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	info, err := client.GetClusterInfo(ctx, req.Host)
	if err != nil {
		httpJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	s.Source = client
	s.SourceCfg = cfg
	s.SourceURL = cfg.ReindexURL()

	httpJSON(w, 200, map[string]any{"ok": true, "info": info})
}

func (s *Server) handleTestDest(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		Host string `json:"host"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.Host == "" {
		httpJSON(w, 400, map[string]string{"error": "host required"})
		return
	}

	cfg := s.TargetCfg
	if len(cfg.Hosts) == 0 || cfg.Hosts[0] != req.Host {
		// Credentials and remote host overrides belong to the configured cluster.
		cfg = config.ESConnection{Hosts: []string{req.Host}}
	}
	client, err := elastic.NewClient(cfg, s.Log)
	if err != nil {
		httpJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	info, err := client.GetClusterInfo(ctx, req.Host)
	if err != nil {
		httpJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
		return
	}

	s.Target = client
	s.TargetCfg = cfg
	s.TargetURL = cfg.ReindexURL()

	httpJSON(w, 200, map[string]any{"ok": true, "info": info})
}

func (s *Server) handleCheckCompat(w http.ResponseWriter, r *http.Request) {
	st := GetState()
	if st.Active == nil {
		httpJSON(w, 404, map[string]string{"error": "no active migration"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	srcInfo, _ := s.Source.GetClusterInfo(ctx, s.SourceCfg.Hosts[0])
	tgtInfo, _ := s.Target.GetClusterInfo(ctx, s.TargetCfg.Hosts[0])

	result := map[string]any{}

	srcMatch := false
	if srcInfo != nil {
		srcMatch = srcInfo.ClusterUUID == st.Active.Source.ClusterUUID &&
			srcInfo.ClusterName == st.Active.Source.ClusterName
	}
	result["source"] = map[string]any{
		"match": srcMatch,
		"role":  "source",
		"saved": map[string]string{"uuid": st.Active.Source.ClusterUUID, "name": st.Active.Source.ClusterName, "host": st.Active.Source.Host},
	}
	if srcInfo != nil {
		result["source"].(map[string]any)["current"] = map[string]string{"uuid": srcInfo.ClusterUUID, "name": srcInfo.ClusterName, "host": srcInfo.Host}
	}

	tgtMatch := false
	if tgtInfo != nil {
		tgtMatch = tgtInfo.ClusterUUID == st.Active.Dest.ClusterUUID &&
			tgtInfo.ClusterName == st.Active.Dest.ClusterName
	}
	result["dest"] = map[string]any{
		"match": tgtMatch,
		"role":  "destination",
		"saved": map[string]string{"uuid": st.Active.Dest.ClusterUUID, "name": st.Active.Dest.ClusterName, "host": st.Active.Dest.Host},
	}
	if tgtInfo != nil {
		result["dest"].(map[string]any)["current"] = map[string]string{"uuid": tgtInfo.ClusterUUID, "name": tgtInfo.ClusterName, "host": tgtInfo.Host}
	}

	result["compatible"] = srcMatch && tgtMatch

	// Detect swapped roles
	if !srcMatch && !tgtMatch && srcInfo != nil && tgtInfo != nil {
		srcMatchesDest := srcInfo.ClusterUUID == st.Active.Dest.ClusterUUID &&
			srcInfo.ClusterName == st.Active.Dest.ClusterName
		tgtMatchesSrc := tgtInfo.ClusterUUID == st.Active.Source.ClusterUUID &&
			tgtInfo.ClusterName == st.Active.Source.ClusterName
		if srcMatchesDest && tgtMatchesSrc {
			result["swapped"] = true
			result["swap_suggestion"] = "Source and destination roles appear to be swapped. Click 'Swap & Continue' to fix."
		}
	}

	httpJSON(w, 200, result)
}
