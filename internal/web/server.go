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
	SourceIndex string   `json:"source_index"`
	TargetIndex string   `json:"target_index"`
	Status      string   `json:"status"`
	Progress    int64    `json:"progress"`
	Total       int64    `json:"total"`
	Error       string   `json:"error,omitempty"`
	Logs        []string `json:"logs,omitempty"`
	Overwrite   bool     `json:"-"`
	Query       string   `json:"-"`
	Direction   string   `json:"-"`
	ForceScroll bool     `json:"-"`
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
	Source     *elastic.Client
	Target     *elastic.Client
	SourceURL  string
	TargetURL  string
	SourceCfg  config.ESConnection
	TargetCfg  config.ESConnection
	ConfigPath string
	StatePath  string
	Log        *logging.Logger
	Port       int
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
		Port:      port,
	}
}

func (s *Server) Start() error {
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

	handler := corsMiddleware(mux)

	if err := LoadState(s.StatePath); err != nil {
		s.Log.Warn("web", "failed to load migration state: %v", err)
	}

	s.Log.Info("web", "starting server on http://localhost:%d", s.Port)
	return http.ListenAndServe(fmt.Sprintf(":%d", s.Port), handler)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		next.ServeHTTP(w, r)
	})
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
		s.SourceCfg.Hosts = []string{req.Source}
		client, err := elastic.NewClient(s.SourceCfg, s.Log)
		if err != nil {
			errs["source"] = err.Error()
			s.Log.Error("web", "source reconnect failed: %v", err)
		} else {
			s.Source = client
			s.Log.Info("web", "reconnected to source: %s", req.Source)
		}
	}

	if req.Target != "" && strings.TrimSpace(req.Target) != strings.TrimSpace(s.TargetCfg.Hosts[0]) {
		s.TargetCfg.Hosts = []string{req.Target}
		client, err := elastic.NewClient(s.TargetCfg, s.Log)
		if err != nil {
			errs["target"] = err.Error()
			s.Log.Error("web", "target reconnect failed: %v", err)
		} else {
			s.Target = client
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

func (s *Server) handleMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}

	var req migrateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	if len(req.Indices) == 0 {
		httpJSON(w, 400, map[string]string{"error": "no indices provided"})
		return
	}

	// Pre-flight: check all selected indices exist on the source side
	ctx := context.Background()
	srcClient := s.Source
	if req.Direction == "target->source" {
		srcClient = s.Target
	}
	srcIndices, listErr := srcClient.ListIndices(ctx)
	if listErr != nil {
		httpJSON(w, 500, map[string]string{"error": "failed to list source indices: " + listErr.Error()})
		return
	}
	srcIndexMap := make(map[string]bool)
	for _, idx := range srcIndices {
		srcIndexMap[idx.Name] = true
	}
	var missing []string
	for _, name := range req.Indices {
		if !srcIndexMap[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		httpJSON(w, 400, map[string]any{
			"error":   "some indices do not exist on source cluster",
			"missing": missing,
			"hint":    "These indices may be on the destination cluster. Flip direction or select different indices.",
		})
		return
	}

	migrateMu.Lock()
	if migrateActive {
		migrateMu.Unlock()
		httpJSON(w, 409, map[string]string{"error": "migration in progress"})
		return
	}
	migrateActive = true
	jobs := make([]migrateJob, len(req.Indices))
	overwrite := true
	if req.Overwrite != nil {
		overwrite = *req.Overwrite
	}
	for i, idx := range req.Indices {
		jobs[i] = migrateJob{
			SourceIndex: idx,
			TargetIndex: idx,
			Status:      "queued",
			Overwrite:   overwrite,
			Query:       req.Query,
			Direction:   req.Direction,
			ForceScroll: req.ForceScroll,
		}
	}
	migrateJobs = jobs
	migrateMu.Unlock()

	// Create state entry
	srcInfo, _ := s.Source.GetClusterInfo(ctx, s.SourceCfg.Hosts[0])
	tgtInfo, _ := s.Target.GetClusterInfo(ctx, s.TargetCfg.Hosts[0])
	active := &ActiveMigration{
		Description: req.Description,
		Query:       req.Query,
		Direction:   req.Direction,
		ForceScroll: req.ForceScroll,
		Source:      ConnectionState{Host: s.SourceCfg.Hosts[0]},
		Dest:        ConnectionState{Host: s.TargetCfg.Hosts[0]},
		Indices:     make([]IndexState, len(req.Indices)),
		StartedAt:   time.Now().Format(time.RFC3339),
	}
	if srcInfo != nil {
		active.Source.ClusterName = srcInfo.ClusterName
		active.Source.ClusterUUID = srcInfo.ClusterUUID
	}
	if tgtInfo != nil {
		active.Dest.ClusterName = tgtInfo.ClusterName
		active.Dest.ClusterUUID = tgtInfo.ClusterUUID
	}
	for i, idx := range req.Indices {
		active.Indices[i] = IndexState{Name: idx, Status: "queued"}
		count, _ := s.Source.Count(ctx, idx)
		if count > 0 {
			active.Indices[i].DocsTotal = count
		}
	}
	SetActiveMigration(active)
	_ = SaveState(s.StatePath)

	go s.runMigration(jobs)

	httpJSON(w, 200, map[string]string{"status": "started", "count": fmt.Sprintf("%d", len(jobs))})
}

func (s *Server) runMigration(jobs []migrateJob) {
	// Helper: pick source/target based on direction
	getSrc := func(dir string) *elastic.Client { if dir == "target->source" { return s.Target }; return s.Source }
	getDst := func(dir string) *elastic.Client { if dir == "target->source" { return s.Source }; return s.Target }
	getSrcURL := func(dir string) string { if dir == "target->source" { return s.TargetURL }; return s.SourceURL }

	defer func() {
		migrateMu.Lock()
		migrateActive = false
		allDone := true
		for _, j := range migrateJobs {
			if j.Status != "completed" {
				allDone = false
				break
			}
		}
		migrateMu.Unlock()
		if allDone {
			CompleteMigration()
			_ = SaveState(s.StatePath)
		}
	}()

	for i := range jobs {
		j := &jobs[i]

		migrateMu.Lock()
		j.Status = "running"
		j.Logs = append(j.Logs, fmt.Sprintf("[%s] starting migration", time.Now().Format("15:04:05")))
		if j.Query != "" && j.Query != "*" {
			migrateMu.Lock()
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] query: %s", time.Now().Format("15:04:05"), j.Query))
			migrateMu.Unlock()
		}
		migrateMu.Unlock()

		ctx := context.Background()
		src := getSrc(j.Direction)
		dst := getDst(j.Direction)
		srcURL := getSrcURL(j.Direction)

		// Auto-open closed indices, restore original state after migration
		srcIndices, _ := src.ListIndices(ctx)
		wasClosed := false
		for _, idx := range srcIndices {
			if idx.Name == j.SourceIndex && idx.Status == "close" {
				wasClosed = true
				break
			}
		}
		if wasClosed {
			migrateMu.Lock()
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] index is closed, auto-opening...", time.Now().Format("15:04:05")))
			migrateMu.Unlock()
			if err := src.OpenIndex(ctx, j.SourceIndex); err != nil {
				migrateMu.Lock()
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] WARN: failed to auto-open: %v", time.Now().Format("15:04:05"), err))
				migrateMu.Unlock()
			} else {
				// Wait for index to become searchable
				migrateMu.Lock()
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] waiting for index to become ready...", time.Now().Format("15:04:05")))
				migrateMu.Unlock()
				for attempt := 0; attempt < 30; attempt++ {
					time.Sleep(2 * time.Second)
					indices, _ := src.ListIndices(ctx)
					ready := false
					for _, idx := range indices {
						if idx.Name == j.SourceIndex && idx.Status == "open" {
							ready = true
							break
						}
					}
					if ready {
						migrateMu.Lock()
						j.Logs = append(j.Logs, fmt.Sprintf("[%s] index ready (opened after %ds)", time.Now().Format("15:04:05"), (attempt+1)*2))
						migrateMu.Unlock()
						break
					}
				}
			}
		}
		defer func() {
			if wasClosed {
				if err := src.CloseIndex(ctx, j.SourceIndex); err != nil {
					migrateMu.Lock()
					j.Logs = append(j.Logs, fmt.Sprintf("[%s] WARN: failed to auto-close after migration: %v", time.Now().Format("15:04:05"), err))
					migrateMu.Unlock()
				} else {
					migrateMu.Lock()
					j.Logs = append(j.Logs, fmt.Sprintf("[%s] closed — restored original state", time.Now().Format("15:04:05")))
					migrateMu.Unlock()
				}
			}
			// Always close the target index after successful migration
			if j.Status == "completed" {
				if err := dst.CloseIndex(ctx, j.TargetIndex); err != nil {
					migrateMu.Lock()
					j.Logs = append(j.Logs, fmt.Sprintf("[%s] WARN: failed to close target index: %v", time.Now().Format("15:04:05"), err))
					migrateMu.Unlock()
				} else {
					migrateMu.Lock()
					j.Logs = append(j.Logs, fmt.Sprintf("[%s] closed target index", time.Now().Format("15:04:05")))
					migrateMu.Unlock()
				}
			}
		}()

		detail, err := src.GetIndexDetail(ctx, j.SourceIndex)
		if err != nil {
			migrateMu.Lock()
			j.Status = "failed"
			j.Error = err.Error()
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] ERROR: read settings: %v", time.Now().Format("15:04:05"), err))
			migrateMu.Unlock()
			s.Log.Error("web-migrate", "detail failed for %s: %v", j.SourceIndex, err)
			continue
		}

		migrateMu.Lock()
		j.Logs = append(j.Logs, fmt.Sprintf("[%s] read settings and mappings", time.Now().Format("15:04:05")))
		migrateMu.Unlock()

		tgtIndices, checkErr := dst.ListIndices(ctx)
		exists := false
		if checkErr == nil {
			for _, ti := range tgtIndices {
				if ti.Name == j.TargetIndex {
					exists = true
					break
				}
			}
		}
		if exists {
			if !j.Overwrite {
				migrateMu.Lock()
				j.Status = "skipped"
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] ⏭ skipped — already exists on target (overwrite disabled)", time.Now().Format("15:04:05")))
				migrateMu.Unlock()
				s.Log.Info("web-migrate", "skipping %s — exists on target", j.SourceIndex)
				continue
			}
			migrateMu.Lock()
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] index already exists on target, deleting...", time.Now().Format("15:04:05")))
			migrateMu.Unlock()
			if delErr := dst.DeleteIndex(ctx, j.TargetIndex); delErr != nil {
				migrateMu.Lock()
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] WARN: could not delete existing index: %v", time.Now().Format("15:04:05"), delErr))
				migrateMu.Unlock()
			}
		}

		err = dst.CreateIndexFromSource(ctx, j.TargetIndex, detail)
		if err != nil {
			migrateMu.Lock()
			j.Status = "failed"
			j.Error = err.Error()
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] ERROR: create index: %v", time.Now().Format("15:04:05"), err))
			migrateMu.Unlock()
			s.Log.Error("web-migrate", "create index failed for %s: %v", j.TargetIndex, err)
			continue
		}

		migrateMu.Lock()
		j.Logs = append(j.Logs, fmt.Sprintf("[%s] created target index", time.Now().Format("15:04:05")))
		migrateMu.Unlock()

		if j.ForceScroll {
			migrateMu.Lock()
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] proxy mode — using scroll+bulk", time.Now().Format("15:04:05")))
			migrateMu.Unlock()
			s.Log.Info("web-migrate", "proxy mode for %s, using scroll+bulk", j.SourceIndex)

			err = s.runScrollMigration(ctx, j)
			if err != nil {
				migrateMu.Lock()
				j.Status = "failed"
				j.Error = err.Error()
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] ERROR: scroll+bulk: %v", time.Now().Format("15:04:05"), err))
				migrateMu.Unlock()
				s.Log.Error("web-migrate", "scroll+bulk failed for %s: %v", j.SourceIndex, err)
				continue
			}

			migrateMu.Lock()
			j.Status = "completed"
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] completed: %d/%d docs (scroll+bulk)", time.Now().Format("15:04:05"), j.Progress, j.Total))
			migrateMu.Unlock()
			continue
		}

		task, err := dst.ReindexAsync(ctx, srcURL, j.SourceIndex, j.TargetIndex, j.Query)
		if err != nil {
			migrateMu.Lock()
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] native reindex unavailable: %v", time.Now().Format("15:04:05"), err))
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] falling back to scroll+bulk...", time.Now().Format("15:04:05")))
			migrateMu.Unlock()
			s.Log.Info("web-migrate", "native reindex failed for %s, trying scroll+bulk", j.SourceIndex)

			err = s.runScrollMigration(ctx, j)
			if err != nil {
				migrateMu.Lock()
				j.Status = "failed"
				j.Error = err.Error()
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] ERROR: scroll+bulk: %v", time.Now().Format("15:04:05"), err))
				migrateMu.Unlock()
				s.Log.Error("web-migrate", "scroll+bulk failed for %s: %v", j.SourceIndex, err)
				continue
			}

			migrateMu.Lock()
			j.Status = "completed"
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] completed: %d/%d docs (scroll+bulk)", time.Now().Format("15:04:05"), j.Progress, j.Total))
			migrateMu.Unlock()
			continue
		}

		migrateMu.Lock()
		j.Logs = append(j.Logs, fmt.Sprintf("[%s] reindex task %s started", time.Now().Format("15:04:05"), task.ID))
		migrateMu.Unlock()

		s.Log.Info("web-migrate", "task %s started for %s", task.ID, j.SourceIndex)

		for {
			status, err := dst.GetTaskStatus(ctx, task.ID)
			if err != nil {
				migrateMu.Lock()
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] task poll error, falling back to scroll+bulk...", time.Now().Format("15:04:05")))
				migrateMu.Unlock()
				s.runScrollFallback(ctx, j)
				break
			}

			migrateMu.Lock()
			j.Progress = status.Created + status.Updated + status.Deleted
			j.Total = status.Total
			if status.Status == "failed" {
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] native reindex failed, falling back to scroll+bulk...", time.Now().Format("15:04:05")))
				migrateMu.Unlock()
				s.Log.Info("web-migrate", "native reindex task failed for %s, trying scroll+bulk", j.SourceIndex)
				s.runScrollFallback(ctx, j)
				break
			}
			if status.Status != "running" {
				j.Status = status.Status
			}
			migrateMu.Unlock()

			if status.Status != "running" {
				migrateMu.Lock()
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] native reindex: %d/%d docs", time.Now().Format("15:04:05"), j.Progress, j.Total))
				if status.Status == "completed" && j.Progress == 0 && j.Total == 0 {
					j.Logs = append(j.Logs, fmt.Sprintf("[%s] 0 docs transferred, falling back to scroll+bulk...", time.Now().Format("15:04:05")))
					migrateMu.Unlock()
					s.Log.Info("web-migrate", "native reindex got 0 docs for %s, trying scroll+bulk", j.SourceIndex)
					s.runScrollFallback(ctx, j)
					break
				}
				j.Status = status.Status
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] completed: %d/%d docs", time.Now().Format("15:04:05"), j.Progress, j.Total))
				migrateMu.Unlock()
				break
			}
			time.Sleep(1 * time.Second)
		}

		// Save state after each index
		migrateMu.Lock()
		_ = s.updateStateFromJob(j)
		migrateMu.Unlock()
	}
}

func (s *Server) updateStateFromJob(j *migrateJob) error {
	UpdateIndexState(j.SourceIndex, j.Status, j.Progress, "")
	if j.Status == "failed" && j.Error != "" {
		SetIndexError(j.SourceIndex, j.Error)
	}
	if err := SaveState(s.StatePath); err != nil {
		s.Log.Error("web", "save state failed: %v", err)
		return err
	}
	return nil
}

func (s *Server) handleMigrateContinue(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}

	st := GetState()
	if st.Active == nil {
		httpJSON(w, 404, map[string]string{"error": "no active migration"})
		return
	}

	migrateMu.Lock()
	if migrateActive {
		migrateMu.Unlock()
		httpJSON(w, 409, map[string]string{"error": "migration in progress"})
		return
	}
	migrateActive = true

	// Build jobs from remaining indices
	var jobs []migrateJob
	for _, idx := range st.Active.Indices {
		if idx.Status == "completed" {
			continue
		}
		jobs = append(jobs, migrateJob{
			SourceIndex: idx.Name,
			TargetIndex: idx.Name,
			Status:      "queued",
			Overwrite:   true,
			Query:       st.Active.Query,
			Direction:   st.Active.Direction,
			ForceScroll: st.Active.ForceScroll,
		})
	}
	migrateJobs = jobs
	migrateMu.Unlock()

	if len(jobs) == 0 {
		migrateActive = false
		CompleteMigration()
		_ = SaveState(s.StatePath)
		httpJSON(w, 200, map[string]string{"status": "completed", "message": "all indices already migrated"})
		return
	}

	go s.runMigration(jobs)

	httpJSON(w, 200, map[string]string{"status": "started", "count": fmt.Sprintf("%d", len(jobs))})
}

func (s *Server) handleMigrateAbandon(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}

	migrateMu.Lock()
	if migrateActive {
		migrateMu.Unlock()
		httpJSON(w, 409, map[string]string{"error": "migration in progress"})
		return
	}
	migrateMu.Unlock()

	AbandonMigration()
	_ = SaveState(s.StatePath)

	httpJSON(w, 200, map[string]string{"status": "abandoned"})
}

func (s *Server) handleMigrateState(w http.ResponseWriter, r *http.Request) {
	st := GetState()
	httpJSON(w, 200, st)
}

func (s *Server) handleMigrateStatus(w http.ResponseWriter, r *http.Request) {
	migrateMu.Lock()
	defer migrateMu.Unlock()
	jobs := migrateJobs
	if !migrateActive {
		jobs = nil
		migrateJobs = nil
	}
	httpJSON(w, 200, map[string]any{
		"active": migrateActive,
		"jobs":   jobs,
	})
}

func (s *Server) runScrollFallback(ctx context.Context, j *migrateJob) {
	err := s.runScrollMigration(ctx, j)
	migrateMu.Lock()
	defer migrateMu.Unlock()
	if err != nil {
		j.Status = "failed"
		j.Error = err.Error()
		j.Logs = append(j.Logs, fmt.Sprintf("[%s] ERROR: scroll+bulk: %v", time.Now().Format("15:04:05"), err))
		s.Log.Error("web-migrate", "scroll+bulk failed for %s: %v", j.SourceIndex, err)
	} else {
		j.Status = "completed"
		j.Logs = append(j.Logs, fmt.Sprintf("[%s] completed: %d docs (scroll+bulk)", time.Now().Format("15:04:05"), j.Progress))
	}
}

func (s *Server) runScrollMigration(ctx context.Context, j *migrateJob) error {
	src := s.Source
	dst := s.Target
	if j.Direction == "target->source" {
		src, dst = s.Target, s.Source
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	docCh, errCh := src.ScrollAll(ctx, j.SourceIndex, 1000, j.Query)

	total := int64(0)
	for batch := range docCh {
		created, err := dst.BulkInsert(ctx, j.TargetIndex, batch)
		if err != nil {
			return fmt.Errorf("bulk insert: %w", err)
		}
		total += int64(created)
		migrateMu.Lock()
		j.Progress = total
		j.Logs = append(j.Logs, fmt.Sprintf("[%s] transferred %d docs...", time.Now().Format("15:04:05"), total))
		migrateMu.Unlock()
	}

	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	default:
	}

	migrateMu.Lock()
	j.Progress = total
	j.Total = total
	migrateMu.Unlock()

	return nil
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
