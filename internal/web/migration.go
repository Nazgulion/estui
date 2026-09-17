package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"estui/internal/elastic"
)

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

	if req.Direction != "" && req.Direction != "source->target" && req.Direction != "target->source" {
		httpJSON(w, 400, map[string]string{"error": "invalid migration direction"})
		return
	}
	migrateMu.Lock()
	defer migrateMu.Unlock()
	if migrateActive || GetState().Active != nil {
		httpJSON(w, 409, map[string]string{"error": "continue or abandon the existing migration first"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	srcInfo, tgtInfo, err := s.validateClusters(ctx, nil)
	if err != nil {
		httpJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}

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
	seen := make(map[string]bool)
	for _, name := range req.Indices {
		if seen[name] {
			httpJSON(w, 400, map[string]string{"error": "duplicate index: " + name})
			return
		}
		seen[name] = true
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

	// Create state entry
	active := &ActiveMigration{
		Description: req.Description,
		Query:       req.Query,
		Direction:   req.Direction,
		ForceScroll: req.ForceScroll,
		Overwrite:   &overwrite,
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
		count, _ := srcClient.Count(ctx, idx)
		if count > 0 {
			active.Indices[i].DocsTotal = count
		}
	}
	SetActiveMigration(active)
	if err := SaveState(s.StatePath); err != nil {
		SetActiveMigration(nil)
		httpJSON(w, 500, map[string]string{"error": "save migration: " + err.Error()})
		return
	}
	migrateActive = true

	go s.runMigration(jobs)

	httpJSON(w, 200, map[string]string{"status": "started", "count": fmt.Sprintf("%d", len(jobs))})
}

func (s *Server) runMigration(jobs []migrateJob) {
	// Helper: pick source/target based on direction
	getSrc := func(dir string) *elastic.Client {
		if dir == "target->source" {
			return s.Target
		}
		return s.Source
	}
	getDst := func(dir string) *elastic.Client {
		if dir == "target->source" {
			return s.Source
		}
		return s.Target
	}
	getSrcURL := func(dir string) string {
		if dir == "target->source" {
			return s.TargetURL
		}
		return s.SourceURL
	}

	defer func() {
		migrateMu.Lock()
		defer migrateMu.Unlock()
		allDone := true
		for _, j := range jobs {
			if !indexFinished(j.Status) {
				allDone = false
				break
			}
		}
		if allDone {
			previous := GetState()
			CompleteMigration()
			if err := SaveState(s.StatePath); err != nil {
				restoreState(previous)
				s.Log.Error("web-migrate", "save completion: %v", err)
			}
		}
		migrateActive = false
	}()
	persistFailed := false

	for i := range jobs {
		j := &jobs[i]
		func() {
			// This runs after index cleanup and on every success, skip, or error path.
			defer func() {
				migrateMu.Lock()
				defer migrateMu.Unlock()
				if err := s.updateStateFromJob(j); err != nil {
					j.Status, j.Error = "failed", "save migration state: "+err.Error()
					storeJob(*j)
					persistFailed = true
				}
			}()

			migrateMu.Lock()
			j.Status = "running"
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] starting migration", time.Now().Format("15:04:05")))
			if j.Query != "" && j.Query != "*" {
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] query: %s", time.Now().Format("15:04:05"), j.Query))
			}
			migrateMu.Unlock()

			if err := s.updateStateFromJob(j); err != nil {
				failJob(j, err)
				return
			}
			ctx := context.Background()
			src := getSrc(j.Direction)
			dst := getDst(j.Direction)
			srcURL := getSrcURL(j.Direction)

			// Auto-open closed indices, restore original state after migration
			srcIndices, listErr := src.ListIndices(ctx)
			if listErr != nil {
				failJob(j, listErr)
				return
			}
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
				return
			}

			migrateMu.Lock()
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] read settings and mappings", time.Now().Format("15:04:05")))
			migrateMu.Unlock()

			tgtIndices, checkErr := dst.ListIndices(ctx)
			if checkErr != nil {
				failJob(j, checkErr)
				return
			}
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
					if j.Resume {
						failJob(j, fmt.Errorf("destination %s exists for an unfinished index and overwrite is disabled; inspect the partial destination before starting a new migration", j.TargetIndex))
						return
					}
					migrateMu.Lock()
					j.Status = "skipped"
					j.Logs = append(j.Logs, fmt.Sprintf("[%s] ⏭ skipped — already exists on target (overwrite disabled)", time.Now().Format("15:04:05")))
					migrateMu.Unlock()
					s.Log.Info("web-migrate", "skipping %s — exists on target", j.SourceIndex)
					return
				}
				migrateMu.Lock()
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] index already exists on target, deleting...", time.Now().Format("15:04:05")))
				migrateMu.Unlock()
				if delErr := dst.DeleteIndex(ctx, j.TargetIndex); delErr != nil {
					migrateMu.Lock()
					j.Logs = append(j.Logs, fmt.Sprintf("[%s] ERROR: could not delete existing index: %v", time.Now().Format("15:04:05"), delErr))
					j.Status, j.Error = "failed", delErr.Error()
					migrateMu.Unlock()
					return
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
				return
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
					return
				}

				migrateMu.Lock()
				j.Status = "completed"
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] completed: %d/%d docs (scroll+bulk)", time.Now().Format("15:04:05"), j.Progress, j.Total))
				migrateMu.Unlock()
				return
			}

			// Persist uncertainty before sending: a crash can happen after ES accepts
			// the task but before its ID reaches us.
			migrateMu.Lock()
			j.ReindexUncertain = true
			migrateMu.Unlock()
			if err := s.updateStateFromJob(j); err != nil {
				failJob(j, err)
				return
			}
			task, err := dst.ReindexAsync(ctx, srcURL, j.SourceIndex, j.TargetIndex, j.Query)
			if err != nil {
				if !errors.Is(err, elastic.ErrReindexRejected) {
					migrateMu.Lock()
					j.ReindexUncertain = true
					migrateMu.Unlock()
					failJob(j, fmt.Errorf("native task may still be running; inspect Elasticsearch before abandoning or retrying: %w", err))
					return
				}
				migrateMu.Lock()
				j.ReindexUncertain = false
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
					return
				}

				migrateMu.Lock()
				j.Status = "completed"
				j.Logs = append(j.Logs, fmt.Sprintf("[%s] completed: %d/%d docs (scroll+bulk)", time.Now().Format("15:04:05"), j.Progress, j.Total))
				migrateMu.Unlock()
				return
			}

			migrateMu.Lock()
			j.TaskID = task.ID
			j.ReindexUncertain = false
			j.Logs = append(j.Logs, fmt.Sprintf("[%s] reindex task %s started", time.Now().Format("15:04:05"), task.ID))
			migrateMu.Unlock()

			s.Log.Info("web-migrate", "task %s started for %s", task.ID, j.SourceIndex)
			if err := s.updateStateFromJob(j); err != nil {
				failJob(j, err)
				return
			}

			for {
				status, err := dst.GetTaskStatus(ctx, task.ID)
				if err != nil {
					failJob(j, fmt.Errorf("cannot confirm native task %s stopped; retry Continue after checking the cluster: %w", task.ID, err))
					return
				}

				migrateMu.Lock()
				j.Progress = status.Created + status.Updated + status.Deleted
				j.Total = status.Total
				if status.Status != "running" {
					j.TaskID = ""
				}
				if status.Status == "failed" {
					j.Logs = append(j.Logs, fmt.Sprintf("[%s] native reindex failed: %s; falling back to scroll+bulk...", time.Now().Format("15:04:05"), status.Error))
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
					j.Status = status.Status
					j.Logs = append(j.Logs, fmt.Sprintf("[%s] completed: %d/%d docs", time.Now().Format("15:04:05"), j.Progress, j.Total))
					migrateMu.Unlock()
					break
				}
				time.Sleep(1 * time.Second)
			}

		}()
		if persistFailed {
			return
		}
	}
}

func (s *Server) updateStateFromJob(j *migrateJob) error {
	storeJob(*j)
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
	migrateMu.Lock()
	defer migrateMu.Unlock()
	if migrateActive {
		httpJSON(w, 409, map[string]string{"error": "migration in progress"})
		return
	}
	st := GetState()
	if st.Active == nil {
		httpJSON(w, 404, map[string]string{"error": "no active migration"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if _, _, err := s.validateClusters(ctx, st.Active); err != nil {
		httpJSON(w, 409, map[string]string{"error": err.Error()})
		return
	}
	var jobs []migrateJob
	for _, idx := range st.Active.Indices {
		if indexFinished(idx.Status) {
			continue
		}
		if idx.ReindexUncertain {
			httpJSON(w, 409, map[string]string{"error": "native task outcome unknown for " + idx.Name + "; inspect Elasticsearch before abandoning this migration"})
			return
		}
		if idx.TaskID != "" {
			dst := s.Target
			if st.Active.Direction == "target->source" {
				dst = s.Source
			}
			task, err := dst.GetTaskStatus(ctx, idx.TaskID)
			if err != nil || task.Status == "running" {
				httpJSON(w, 409, map[string]string{"error": "native task " + idx.TaskID + " is still running or cannot be verified"})
				return
			}
		}
		// Old checkpoints without an overwrite choice must not authorize deletion.
		overwrite := st.Active.Overwrite != nil && *st.Active.Overwrite
		jobs = append(jobs, migrateJob{SourceIndex: idx.Name, TargetIndex: idx.Name, Status: "queued", Resume: idx.Status == "failed" || idx.Status == "running", Overwrite: overwrite, Query: st.Active.Query, Direction: st.Active.Direction, ForceScroll: st.Active.ForceScroll})
	}
	if len(jobs) == 0 {
		CompleteMigration()
		if err := SaveState(s.StatePath); err != nil {
			restoreState(st)
			httpJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		httpJSON(w, 200, map[string]string{"status": "completed"})
		return
	}
	migrateJobs = jobs
	migrateActive = true
	go s.runMigration(jobs)
	httpJSON(w, 200, map[string]string{"status": "started", "count": fmt.Sprint(len(jobs))})
}

func (s *Server) handleMigrateAbandon(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}

	migrateMu.Lock()
	defer migrateMu.Unlock()
	if migrateActive {
		httpJSON(w, 409, map[string]string{"error": "migration in progress"})
		return
	}

	previous := GetState()
	AbandonMigration()
	if err := SaveState(s.StatePath); err != nil {
		restoreState(previous)
		httpJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}

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
	docCh, errCh := src.ScrollAll(ctx, j.SourceIndex, 1000, j.Query)
	defer func() {
		cancel()
		// Wait for the producer to release its cursor, even when a bulk write fails.
		for range docCh {
		}
		for range errCh {
		}
	}()

	total := int64(0)
	for batch := range docCh {
		created, err := dst.BulkInsert(ctx, j.TargetIndex, batch)
		total += int64(created)
		migrateMu.Lock()
		j.Progress = total
		j.Logs = append(j.Logs, fmt.Sprintf("[%s] transferred %d docs...", time.Now().Format("15:04:05"), total))
		migrateMu.Unlock()
		if err != nil {
			return fmt.Errorf("bulk insert: %w", err)
		}
	}

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	migrateMu.Lock()
	j.Progress = total
	j.Total = total
	migrateMu.Unlock()

	return nil
}

func failJob(j *migrateJob, err error) {
	migrateMu.Lock()
	defer migrateMu.Unlock()
	j.Status, j.Error = "failed", err.Error()
	j.Logs = append(j.Logs, fmt.Sprintf("[%s] ERROR: %v", time.Now().Format("15:04:05"), err))
}

// Comparing UUIDs catches aliases and different nodes of the same cluster.
func (s *Server) validateClusters(ctx context.Context, saved *ActiveMigration) (*elastic.ClusterInfo, *elastic.ClusterInfo, error) {
	if s.Source == nil || s.Target == nil || len(s.SourceCfg.Hosts) == 0 || len(s.TargetCfg.Hosts) == 0 {
		return nil, nil, fmt.Errorf("both cluster connections must be configured")
	}
	src, err := s.Source.GetClusterInfo(ctx, s.SourceCfg.Hosts[0])
	if err != nil {
		return nil, nil, fmt.Errorf("verify source cluster: %w", err)
	}
	dst, err := s.Target.GetClusterInfo(ctx, s.TargetCfg.Hosts[0])
	if err != nil {
		return nil, nil, fmt.Errorf("verify destination cluster: %w", err)
	}
	if src.ClusterUUID == "" || dst.ClusterUUID == "" || src.ClusterUUID == "_na_" || dst.ClusterUUID == "_na_" {
		return nil, nil, fmt.Errorf("both cluster UUIDs must be known before migrating")
	}
	if src.ClusterUUID == dst.ClusterUUID {
		return nil, nil, fmt.Errorf("source and destination are the same cluster; migration would delete source data")
	}
	if saved != nil && (src.ClusterUUID != saved.Source.ClusterUUID || dst.ClusterUUID != saved.Dest.ClusterUUID) {
		return nil, nil, fmt.Errorf("connected clusters do not match the saved migration; reconnect the original source and destination")
	}
	return src, dst, nil
}
