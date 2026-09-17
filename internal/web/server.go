package web

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"

	"estui/internal/config"
	"estui/internal/elastic"
	"estui/internal/logging"
)

//go:embed all:static
var staticFiles embed.FS

type indexJSON struct {
	Name      string `json:"name"`
	Health    string `json:"health"`
	Status    string `json:"status"`
	DocCount  int64  `json:"doc_count"`
	SizeHuman string `json:"size_human"`
	SizeBytes int64  `json:"size_bytes"`
	Primary   int    `json:"primary"`
	Replica   int    `json:"replica"`
	UUID      string `json:"uuid"`
}

type detailJSON struct {
	Settings map[string]any `json:"settings"`
	Mappings map[string]any `json:"mappings"`
	Aliases  map[string]any `json:"aliases"`
}

type clusterInfo struct {
	Name    string `json:"name"`
	Healthy bool   `json:"healthy"`
}

type migrateRequest struct {
	Indices     []string `json:"indices"`
	Overwrite   *bool    `json:"overwrite,omitempty"`
	Description string   `json:"description,omitempty"`
	Query       string   `json:"query,omitempty"`
	Direction   string   `json:"direction,omitempty"`
	ForceScroll bool     `json:"force_scroll,omitempty"`
}

type migrateJob struct {
	Resume           bool     `json:"-"`
	TaskID           string   `json:"task_id,omitempty"`
	ReindexUncertain bool     `json:"reindex_uncertain,omitempty"`
	SourceIndex      string   `json:"source_index"`
	TargetIndex      string   `json:"target_index"`
	Status           string   `json:"status"`
	Progress         int64    `json:"progress"`
	Total            int64    `json:"total"`
	Error            string   `json:"error,omitempty"`
	Logs             []string `json:"logs,omitempty"`
	Overwrite        bool     `json:"-"`
	Query            string   `json:"-"`
	Direction        string   `json:"-"`
	ForceScroll      bool     `json:"-"`
}

type bulkRequest struct {
	Indices []string `json:"indices"`
}

type actionResult struct {
	Index string `json:"index"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

var (
	migrateJobs   []migrateJob
	migrateActive bool
	migrateMu     sync.Mutex
)

type Server struct {
	connectionMu sync.RWMutex
	Source       *elastic.Client
	Target       *elastic.Client
	SourceURL    string
	TargetURL    string
	SourceCfg    config.ESConnection
	TargetCfg    config.ESConnection
	ConfigPath   string
	StatePath    string
	Log          *logging.Logger
	Port         int
}

func New(source, target *elastic.Client, sourceURL, targetURL string, sourceCfg, targetCfg config.ESConnection, configPath string, log *logging.Logger, port int) *Server {
	return &Server{
		Source:     source,
		Target:     target,
		SourceURL:  sourceURL,
		TargetURL:  targetURL,
		SourceCfg:  sourceCfg,
		TargetCfg:  targetCfg,
		ConfigPath: configPath,
		StatePath:  "migration_state.json",
		Log:        log,
		Port:       port,
	}
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/indices/source", s.handleSourceIndices)
	mux.HandleFunc("/api/indices/target", s.handleTargetIndices)
	mux.HandleFunc("/api/indices/detail/source/", s.handleSourceDetail)
	mux.HandleFunc("/api/indices/detail/target/", s.handleTargetDetail)
	mux.HandleFunc("/api/indices/delete", s.handleIndexDelete)
	mux.HandleFunc("/api/indices/open", s.handleIndexOpen)
	mux.HandleFunc("/api/indices/close", s.handleIndexClose)
	mux.HandleFunc("/api/clusters", s.handleClusters)
	mux.HandleFunc("/api/connect", s.handleConnect)
	mux.HandleFunc("/api/disk", s.handleDisk)
	mux.HandleFunc("/api/migrate", s.handleMigrate)
	mux.HandleFunc("/api/migrate/status", s.handleMigrateStatus)
	mux.HandleFunc("/api/migrate/continue", s.handleMigrateContinue)
	mux.HandleFunc("/api/migrate/abandon", s.handleMigrateAbandon)
	mux.HandleFunc("/api/migrate/state", s.handleMigrateState)
	mux.HandleFunc("/api/preview-query", s.handlePreviewQuery)
	mux.HandleFunc("/api/test-source", s.handleTestSource)
	mux.HandleFunc("/api/test-dest", s.handleTestDest)
	mux.HandleFunc("/api/check-compat", s.handleCheckCompat)

	staticFS, _ := fs.Sub(staticFiles, "static")
	mux.Handle("/", http.FileServer(http.FS(staticFS)))

	return localRequests(s.Port, s.connectionRequests(mux))
}

func (s *Server) listenAddress() string { return fmt.Sprintf("127.0.0.1:%d", s.Port) }

func (s *Server) Start() error {
	if err := LoadState(s.StatePath); err != nil {
		return fmt.Errorf("load migration state: %w", err)
	}
	s.Log.Info("web", "starting server on http://%s", s.listenAddress())
	server := &http.Server{Addr: s.listenAddress(), Handler: s.handler(), ReadHeaderTimeout: 10 * time.Second}
	return server.ListenAndServe()
}

func (s *Server) handleSourceIndices(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	indices, err := s.Source.ListIndices(ctx)
	if err != nil {
		httpJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpJSON(w, 200, toIndexJSON(indices))
}

func (s *Server) handleTargetIndices(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	indices, err := s.Target.ListIndices(ctx)
	if err != nil {
		httpJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpJSON(w, 200, toIndexJSON(indices))
}

func (s *Server) handleSourceDetail(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/indices/detail/source/")
	if name == "" {
		httpJSON(w, 400, map[string]string{"error": "missing index name"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	detail, err := s.Source.GetIndexDetail(ctx, name)
	if err != nil {
		httpJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	httpJSON(w, 200, detailJSON{Settings: detail.Settings, Mappings: detail.Mappings, Aliases: detail.Aliases})
}

func (s *Server) handleTargetDetail(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/indices/detail/target/")
	if name == "" {
		httpJSON(w, 400, map[string]string{"error": "missing index name"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	detail, err := s.Target.GetIndexDetail(ctx, name)
	if err != nil {
		httpJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	httpJSON(w, 200, detailJSON{Settings: detail.Settings, Mappings: detail.Mappings, Aliases: detail.Aliases})
}

func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	httpJSON(w, 200, map[string]any{
		"source": clusterInfo{
			Name:    s.Source.Name(),
			Healthy: true,
		},
		"target": clusterInfo{
			Name:    s.Target.Name(),
			Healthy: true,
		},
	})
}

func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		Source string `json:"source"`
		Target string `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}

	errs := map[string]string{}

	if req.Source != "" && strings.TrimSpace(req.Source) != strings.TrimSpace(s.SourceCfg.Hosts[0]) {
		cfg := s.SourceCfg
		cfg.Hosts = []string{req.Source}
		cfg.RemoteReindexHost = ""
		client, err := elastic.NewClient(cfg, s.Log)
		if err != nil {
			errs["source"] = err.Error()
			s.Log.Error("web", "source reconnect failed: %v", err)
		} else {
			s.Source = client
			s.SourceCfg = cfg
			s.SourceURL = cfg.ReindexURL()
			s.Log.Info("web", "reconnected to source: %s", req.Source)
		}
	}

	if req.Target != "" && strings.TrimSpace(req.Target) != strings.TrimSpace(s.TargetCfg.Hosts[0]) {
		cfg := s.TargetCfg
		cfg.Hosts = []string{req.Target}
		cfg.RemoteReindexHost = ""
		client, err := elastic.NewClient(cfg, s.Log)
		if err != nil {
			errs["target"] = err.Error()
			s.Log.Error("web", "target reconnect failed: %v", err)
		} else {
			s.Target = client
			s.TargetCfg = cfg
			s.TargetURL = cfg.ReindexURL()
			s.Log.Info("web", "reconnected to target: %s", req.Target)
		}
	}

	resp := map[string]any{
		"source": clusterInfo{Name: s.Source.Name(), Healthy: len(errs["source"]) == 0},
		"target": clusterInfo{Name: s.Target.Name(), Healthy: len(errs["target"]) == 0},
	}
	if len(errs) > 0 {
		resp["errors"] = errs
	}
	httpJSON(w, 200, resp)
}

func (s *Server) handleDisk(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	srcDisk, err := s.Source.GetDiskStats(ctx)
	if err != nil {
		httpJSON(w, 500, map[string]string{"error": "source: " + err.Error()})
		return
	}
	tgtDisk, err := s.Target.GetDiskStats(ctx)
	if err != nil {
		httpJSON(w, 500, map[string]string{"error": "target: " + err.Error()})
		return
	}
	httpJSON(w, 200, map[string]any{"source": srcDisk, "target": tgtDisk})
}

func (s *Server) handleIndexDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var req bulkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	results := make([]actionResult, len(req.Indices))
	for i, name := range req.Indices {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		err := s.Source.DeleteIndex(ctx, name)
		cancel()
		if err != nil {
			results[i] = actionResult{Index: name, OK: false, Error: err.Error()}
		} else {
			results[i] = actionResult{Index: name, OK: true}
		}
	}
	httpJSON(w, 200, map[string]any{"results": results})
}

func (s *Server) handleIndexOpen(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var req bulkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	results := make([]actionResult, len(req.Indices))
	for i, name := range req.Indices {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		err := s.Source.OpenIndex(ctx, name)
		cancel()
		if err != nil {
			results[i] = actionResult{Index: name, OK: false, Error: err.Error()}
		} else {
			results[i] = actionResult{Index: name, OK: true}
		}
	}
	httpJSON(w, 200, map[string]any{"results": results})
}

func (s *Server) handleIndexClose(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var req bulkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	results := make([]actionResult, len(req.Indices))
	for i, name := range req.Indices {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		err := s.Source.CloseIndex(ctx, name)
		cancel()
		if err != nil {
			results[i] = actionResult{Index: name, OK: false, Error: err.Error()}
		} else {
			results[i] = actionResult{Index: name, OK: true}
		}
	}
	httpJSON(w, 200, map[string]any{"results": results})
}

func (s *Server) handlePreviewQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		Host  string `json:"host"`
		Index string `json:"index"`
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}

	client := s.Source
	if req.Host == "target" {
		client = s.Target
	}

	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()

	// Auto-open if closed
	indices, _ := client.ListIndices(ctx)
	wasClosed := false
	for _, idx := range indices {
		if idx.Name == req.Index && idx.Status == "close" {
			wasClosed = true
			break
		}
	}
	if wasClosed {
		if err := client.OpenIndex(ctx, req.Index); err != nil {
			httpJSON(w, 500, map[string]string{"error": "failed to open closed index: " + err.Error()})
			return
		}
		for attempt := 0; attempt < 30; attempt++ {
			time.Sleep(2 * time.Second)
			indices, _ := client.ListIndices(ctx)
			ready := false
			for _, idx := range indices {
				if idx.Name == req.Index && idx.Status == "open" {
					ready = true
					break
				}
			}
			if ready {
				break
			}
		}
		defer func() { _ = client.CloseIndex(ctx, req.Index) }()
	}

	q := map[string]any{"match_all": map[string]any{}}
	if req.Query != "" && req.Query != "*" {
		q = map[string]any{"query_string": map[string]any{"query": req.Query}}
	}
	body, _ := json.Marshal(map[string]any{"query": q})

	resp, err := client.ES().Count(
		client.ES().Count.WithContext(ctx),
		client.ES().Count.WithIndex(req.Index),
		client.ES().Count.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		httpJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var c struct {
		Count int64 `json:"count"`
	}
	json.Unmarshal(respBody, &c)
	httpJSON(w, 200, map[string]any{"index": req.Index, "count": c.Count, "query": req.Query})
}

func toIndexJSON(indices []elastic.IndexInfo) []indexJSON {
	result := make([]indexJSON, len(indices))
	for i, idx := range indices {
		result[i] = indexJSON{
			Name:      idx.Name,
			Health:    idx.Health,
			Status:    idx.Status,
			DocCount:  idx.DocCount,
			SizeHuman: idx.SizeHuman,
			SizeBytes: idx.SizeBytes,
			Primary:   idx.Primary,
			Replica:   idx.Replica,
			UUID:      idx.UUID,
		}
	}
	return result
}

func httpJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
