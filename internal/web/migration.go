package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"estui/internal/elastic"
)

func (s *Server) handleMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpJSON(w, 405, map[string]string{"error": "POST required"})
		return
	}

	var req migrateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
		httpJSON(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	if len(req.Indices) == 0 {
		httpJSON(w, 400, map[string]string{"error": "no indices provided"})
		return
	}
	if req.AllowLiveSource {
		if req.CollisionAction == "overwrite" {
			httpJSON(w, 400, map[string]string{"error": "live copy cannot overwrite existing destinations; choose a separate copy"})
			return
		}
		for _, plan := range req.Remaps {
			if plan.OriginalAction != "" && plan.OriginalAction != "keep" {
				httpJSON(w, 400, map[string]string{"error": "live copy requires keeping the original; source backup/removal is not permitted"})
				return
			}
		}
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

	if (req.SourceUUID != "" && req.SourceUUID != srcInfo.ClusterUUID) || (req.TargetUUID != "" && req.TargetUUID != tgtInfo.ClusterUUID) {
		httpJSON(w, 409, map[string]string{"error": "cluster connection changed since confirmation; reload before migrating"})
		return
	}
	if err := validateIndexNames(req.Indices); err != nil {
		httpJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}

	srcClient, dstClient := s.Source, s.Target
	if req.Direction == "target->source" {
		srcClient, dstClient = s.Target, s.Source
	}
	srcIndices, listErr := srcClient.ListIndices(ctx)
	if listErr != nil {
		httpJSON(w, 500, map[string]string{"error": "failed to list source indices: " + listErr.Error()})
		return
	}
	srcIndexMap := make(map[string]string)
	for _, idx := range srcIndices {
		srcIndexMap[idx.Name] = idx.UUID
	}
	var missing []string
	seen := make(map[string]bool)
	for _, name := range req.Indices {
		if seen[name] {
			httpJSON(w, 400, map[string]string{"error": "duplicate index: " + name})
			return
		}
		seen[name] = true
		if uuid, exists := srcIndexMap[name]; exists && uuid == "" {
			httpJSON(w, 400, map[string]string{"error": "source index UUID is unavailable: " + name})
			return
		}
		if _, exists := srcIndexMap[name]; !exists {
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

	if len(req.Remaps) > 0 {
		if len(req.Remaps) != len(req.Indices) || req.SourceUUID != srcInfo.ClusterUUID || req.TargetUUID != tgtInfo.ClusterUUID || req.CollisionAction == "overwrite" {
			httpJSON(w, 400, map[string]string{"error": "mapping plans require every selected index, confirmed cluster identities, and new destination names"})
			return
		}
		for name, options := range req.Remaps {
			if !seen[name] {
				httpJSON(w, 400, map[string]string{"error": "mapping plan contains an unselected index"})
				return
			}
			options, err = validateRemapOptions(name, options, req, srcIndexMap[name])
			if err != nil {
				httpJSON(w, 400, map[string]string{"error": name + ": " + err.Error()})
				return
			}
			req.Remaps[name] = options
		}
	}
	local := srcInfo.ClusterUUID == tgtInfo.ClusterUUID
	action := req.CollisionAction
	// Legacy true only authorized a separate copy, never destructive replacement.
	if action == "" && req.Overwrite != nil && *req.Overwrite {
		action = "rename"
	}
	if action != "" && action != "rename" && action != "overwrite" && action != "skip" {
		httpJSON(w, 400, map[string]string{"error": "invalid collision action"})
		return
	}
	if local && action == "overwrite" {
		httpJSON(w, 400, map[string]string{"error": "same-cluster migration always uses a new prefixed index; overwrite is not allowed"})
		return
	}
	destinations, err := dstClient.ListIndices(ctx)
	if err != nil {
		httpJSON(w, 502, map[string]string{"error": err.Error()})
		return
	}
	destUUIDs := map[string]string{}
	for _, idx := range destinations {
		destUUIDs[idx.Name] = idx.UUID
	}
	collisions := []string{}
	for _, name := range req.Indices {
		if _, ok := destUUIDs[name]; ok {
			collisions = append(collisions, name)
		}
	}
	if len(req.Remaps) == 0 && !local && len(collisions) > 0 && action == "" && req.Overwrite == nil {
		httpJSON(w, 409, map[string]any{"error": "destination indices already exist; choose overwrite or rename", "collisions": collisions})
		return
	}
	reservedDest, reservedBackup := map[string]bool{}, map[string]bool{}
	for _, o := range req.Remaps {
		_, exists := destUUIDs[o.Destination]
		if exists || reservedDest[o.Destination] || (local && seen[o.Destination]) {
			httpJSON(w, 409, map[string]string{"error": "destination must be a new, distinct index: " + o.Destination})
			return
		}
		reservedDest[o.Destination] = true
		if strings.HasPrefix(o.OriginalAction, "backup") {
			_, exists := srcIndexMap[o.BackupIndex]
			if exists || reservedBackup[o.BackupIndex] {
				httpJSON(w, 409, map[string]string{"error": "backup must be a new, distinct index: " + o.BackupIndex})
				return
			}
			reservedBackup[o.BackupIndex] = true
		}
	}
	if local {
		for name := range reservedBackup {
			if reservedDest[name] {
				httpJSON(w, 409, map[string]string{"error": "backup and destination names must differ"})
				return
			}
		}
	}
	jobs := make([]migrateJob, len(req.Indices))
	overwrite := action == "rename" // Internal permission to allocate a separate copy.
	for i, name := range req.Indices {
		if options, ok := req.Remaps[name]; ok {
			jobs[i] = migrateJob{AllowLiveSource: req.AllowLiveSource, SourceIndex: name, SourceUUID: srcIndexMap[name], TargetIndex: options.Destination, Status: "queued", Query: req.Query, Direction: req.Direction, Local: local, Remap: RemapState{Options: options, Phase: "copying"}}
			continue
		}
		target := name
		_, exists := destUUIDs[name]
		if local || (exists && (action == "rename" || action == "overwrite")) {
			target, err = newCopyName(name, local)
			if err != nil {
				httpJSON(w, 500, map[string]string{"error": err.Error()})
				return
			}
		}
		job := migrateJob{AllowLiveSource: req.AllowLiveSource, SourceIndex: name, SourceUUID: srcIndexMap[name], TargetIndex: target, Status: "queued", Overwrite: overwrite, Query: req.Query, Direction: req.Direction, ForceScroll: req.ForceScroll, Local: local}
		if !local && exists && action == "overwrite" {
			expected := req.OverwriteUUIDs[name]
			if expected == "" || expected != destUUIDs[name] || req.SourceUUID != srcInfo.ClusterUUID || req.TargetUUID != tgtInfo.ClusterUUID {
				httpJSON(w, 409, map[string]string{"error": "overwrite requires confirmation of the current clusters and destination index UUIDs; reload the dialog"})
				return
			}
			original, err := dstClient.GetIndexDetail(ctx, name)
			if err != nil || !elastic.WriteBlocked(original) {
				httpJSON(w, 409, map[string]string{"error": "stop destination writers and write-block " + name + " before confirming overwrite"})
				return
			}
			job.FinalIndex = name
			job.ReplaceUUID = expected
			job.ReplacePhase = "staging"
		}
		jobs[i] = job
	}
	migrateJobs = jobs

	// Create state entry
	active := &ActiveMigration{
		AllowLiveSource: req.AllowLiveSource,
		Description:     req.Description,
		Query:           req.Query,
		Direction:       req.Direction,
		ForceScroll:     req.ForceScroll,
		Overwrite:       &overwrite,
		Source:          ConnectionState{Host: s.SourceCfg.Hosts[0]},
		Dest:            ConnectionState{Host: s.TargetCfg.Hosts[0]},
		Indices:         make([]IndexState, len(req.Indices)),
		StartedAt:       time.Now().Format(time.RFC3339),
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
		active.Indices[i] = IndexState{AllowLiveSource: req.AllowLiveSource, Remap: jobs[i].Remap, Name: idx, SourceUUID: srcIndexMap[idx], TargetIndex: jobs[i].TargetIndex, FinalIndex: jobs[i].FinalIndex, ReplaceUUID: jobs[i].ReplaceUUID, ReplacePhase: jobs[i].ReplacePhase, Local: local, Status: "queued"}
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
	defer func() {
		migrateMu.Lock()
		defer migrateMu.Unlock()
		allDone := true
		for _, j := range jobs {
			if !indexFinished(j.Status) {
				allDone = false
			}
		}
		if allDone {
			previous := GetState()
			CompleteMigration()
			if err := SaveState(s.StatePath); err != nil {
				restoreState(previous)
				s.Log.Error("migration", "save completion: %v", err)
			}
		}
		migrateActive = false
	}()
	for i := range jobs {
		j := &jobs[i]
		migrateMu.Lock()
		j.Status = "running"
		j.Error = ""
		migrateMu.Unlock()
		jobLog(j, "starting migration")
		if err := s.updateStateFromJob(j); err != nil {
			failJob(j, err)
			return
		}
		err := s.runIndexMigration(context.Background(), j)
		if err != nil {
			failJob(j, err)
		}
		if err := s.updateStateFromJob(j); err != nil {
			failJob(j, err)
			return
		}
		// Stop after the first failure; do not amplify an outage across other indices.
		if err != nil {
			return
		}
	}
}

func (s *Server) runIndexMigration(ctx context.Context, j *migrateJob) (err error) {
	src, dst := s.Source, s.Target
	if j.Direction == "target->source" {
		src, dst = s.Target, s.Source
	}
	if j.Local && (j.TargetIndex == j.SourceIndex || j.FinalIndex != "") {
		return fmt.Errorf("same-cluster destination must differ from source and cannot overwrite")
	}
	defer func() {
		if j.TaskID != "" || j.ReindexUncertain {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		closeOwned := func(c *elastic.Client, name, uuid string) error {
			actual, e := migrationIndexUUID(cleanup, c, name)
			if e != nil {
				return e
			}
			if uuid == "" || actual != uuid {
				return fmt.Errorf("index %s replaced; refusing to close", name)
			}
			return c.CloseIndex(cleanup, name)
		}
		if !j.Remap.SourceDeleted && !j.Remap.uncertain() && j.SourceWasClosed != nil && *j.SourceWasClosed && j.SourceUUID != "" {
			err = errors.Join(err, closeOwned(src, j.SourceIndex, j.SourceUUID))
		}
		if err == nil && (j.Status == "completed" || j.Status == "copied_live") {
			err = closeOwned(dst, j.TargetIndex, j.TargetUUID)
			if err == nil {
				err = restoreDestinationWriteBlock(cleanup, dst, j.TargetIndex, j.TargetUUID, j.SourceWriteBlocked)
			}
		}
	}()
	if j.AllowLiveSource {
		if err := validateLiveJob(j); err != nil {
			return err
		}
	}
	if j.Remap.enabled() && j.Remap.uncertain() {
		return fmt.Errorf("original deletion outcome is unknown; manual reconciliation required")
	}
	if j.Remap.enabled() && j.Remap.SourceDeleted {
		return s.completeDeletedRemap(ctx, src, dst, j)
	}
	// Validate the source before any destination mutation, including recovery.
	if j.TaskID != "" && j.SourceUUID == "" {
		return fmt.Errorf("legacy native task lacks source identity; manual reconciliation is required")
	}
	detail, err := src.GetIndexDetail(ctx, j.SourceIndex)
	if err != nil {
		return err
	}
	identity := elastic.SourceIdentity
	if j.AllowLiveSource {
		identity = elastic.ReadableSourceIdentity
	}
	sourceUUID, err := identity(detail)
	if err != nil {
		return err
	}
	if j.SourceUUID != "" && j.SourceUUID != sourceUUID {
		return fmt.Errorf("source index %s was replaced", j.SourceIndex)
	}
	status, err := previewIndexStatus(ctx, src, j.SourceIndex)
	if err != nil {
		return err
	}
	migrateMu.Lock()
	j.SourceUUID = sourceUUID
	if j.SourceWriteBlocked == nil {
		blocked := elastic.WriteBlocked(detail)
		j.SourceWriteBlocked = &blocked
	}
	if j.SourceWasClosed == nil {
		closed := status == "close"
		j.SourceWasClosed = &closed
	}
	migrateMu.Unlock()
	if err := s.updateStateFromJob(j); err != nil {
		return err
	}
	if status == "close" {
		jobLog(j, "temporarily opening source; its original closed state is checkpointed and will be restored")
		if err := src.OpenIndex(ctx, j.SourceIndex); err != nil {
			return err
		}
		if err := waitPreviewIndexOpen(ctx, src, j.SourceIndex); err != nil {
			return err
		}
	}
	if !j.AllowLiveSource {
		// The explicit block API waits for in-flight writes to drain.
		if err := src.BlockWrites(ctx, j.SourceIndex); err != nil {
			return err
		}
		if err := src.RefreshIndex(ctx, j.SourceIndex); err != nil {
			return err
		}
	} else {
		jobLog(j, "live copy authorized: no source write block or settings changes; temporarily opened sources will be closed afterward; later writes are not synchronized")
	}
	if !j.AllowLiveSource && j.Remap.enabled() && j.Remap.Phase != "copying" {
		return s.finishRemap(ctx, src, dst, j)
	}
	if j.FinalIndex != "" && j.ReplacePhase != "staging" {
		return s.publishReplacement(ctx, src, dst, j)
	}
	if j.TaskID != "" {
		actual, e := migrationIndexUUID(ctx, dst, j.TargetIndex)
		if e != nil {
			return e
		}
		if actual != j.TargetUUID || j.TargetUUID == "" {
			return fmt.Errorf("destination %s was replaced since this migration", j.TargetIndex)
		}
		task, e := pollMigrationTask(ctx, dst, j)
		if e != nil {
			return e
		}
		if task.Status == "completed" {
			return s.completeTransfer(ctx, src, dst, j)
		}
		jobLog(j, "saved native task failed; retaining partial index and starting a fresh copy")
		if err := s.updateStateFromJob(j); err != nil {
			return err
		}
	}
	if j.Remap.enabled() {
		mapping, e := decodeMapping(j.Remap.Options.Mapping, j.SourceIndex)
		if e != nil {
			return e
		}
		copyDetail := *detail
		copyDetail.Mappings = mapping
		detail = &copyDetail
	}
	if err := s.prepareDestination(ctx, dst, j, detail); err != nil {
		return err
	}
	if j.Remap.enabled() {
		if err := checkRemapMapping(ctx, dst, j); err != nil {
			return err
		}
	}
	if j.Status == "skipped" {
		return nil
	}
	// New transfers always use the configured client. A destination-side remote
	// address cannot be proven to refer to that same source from this process.
	jobLog(j, "copying through the configured source connection")
	if err := s.runScrollMigration(ctx, j); err != nil {
		return err
	}
	return s.completeTransfer(ctx, src, dst, j)
}

func (s *Server) prepareDestination(ctx context.Context, dst *elastic.Client, j *migrateJob, detail *elastic.IndexDetail) error {
	indices, err := dst.ListIndices(ctx)
	if err != nil {
		return err
	}
	exists := false
	for _, idx := range indices {
		if idx.Name == j.TargetIndex {
			exists = true
		}
	}
	if exists {
		if j.TargetUUID != "" {
			actual, err := migrationIndexUUID(ctx, dst, j.TargetIndex)
			if err != nil {
				return err
			}
			if actual != j.TargetUUID {
				return fmt.Errorf("destination %s was replaced since this migration", j.TargetIndex)
			}
		}
		if j.Remap.enabled() && !j.Resume {
			return fmt.Errorf("planned destination appeared after confirmation; refusing to reuse it")
		}
		if !j.Overwrite && !j.Resume && !j.Local && j.FinalIndex == "" {
			migrateMu.Lock()
			j.Status = "skipped"
			migrateMu.Unlock()
			jobLog(j, "skipped existing destination; no index was changed")
			return nil
		}
		previous := j.TargetIndex
		next, err := newCopyName(j.SourceIndex, j.Local)
		if err != nil {
			return err
		}
		migrateMu.Lock()
		if j.TargetUUID != "" {
			j.RetainedTargets = append(j.RetainedTargets, previous)
		}
		j.TargetIndex = next
		j.TargetUUID = ""
		j.Progress = 0
		j.Total = 0
		j.VerifiedHash = ""
		j.VerifiedCount = 0
		j.CapturedHash, j.CapturedCount = "", 0
		migrateMu.Unlock()
		jobLog(j, "preserved %s; new destination is %s", previous, j.TargetIndex)
	}
	// Persist the intended name before submitting a non-retried create request.
	if err := s.updateStateFromJob(j); err != nil {
		return err
	}
	if err := dst.CheckTemplates(ctx, j.TargetIndex); err != nil {
		return err
	}
	if err := dst.CreateIndexFromSource(ctx, j.TargetIndex, detail); err != nil {
		return err
	}
	actual, err := dst.GetIndexDetail(ctx, j.TargetIndex)
	if err != nil {
		return err
	}
	uuid, ok := elastic.Setting(actual, "uuid").(string)
	if !ok || uuid == "" {
		return fmt.Errorf("created index has no UUID")
	}
	migrateMu.Lock()
	j.TargetUUID = uuid
	migrateMu.Unlock()
	if err := s.updateStateFromJob(j); err != nil {
		return err
	}
	if len(actual.Aliases) != 0 || elastic.Setting(actual, "lifecycle.name") != nil {
		return fmt.Errorf("destination unexpectedly has aliases or lifecycle settings; copy stopped")
	}
	for _, key := range []string{"default_pipeline", "final_pipeline"} {
		v := elastic.Setting(actual, key)
		if v != nil && v != "_none" {
			return fmt.Errorf("destination unexpectedly has %s; copy stopped", key)
		}
	}
	return nil
}

func (s *Server) finishCopy(ctx context.Context, src, dst *elastic.Client, j *migrateJob) error {
	actual, err := migrationIndexUUID(ctx, dst, j.TargetIndex)
	if err != nil {
		return err
	}
	if j.TargetUUID == "" || actual != j.TargetUUID {
		return fmt.Errorf("destination identity changed before verification; refusing to block writes")
	}
	// Block before reading back so no outside writer can race the verification.
	if err := dst.BlockWrites(ctx, j.TargetIndex); err != nil {
		return err
	}
	jobLog(j, "verifying source and destination document IDs, routing, and contents")
	fingerprint, err := verifyCopy(ctx, src, dst, j.SourceIndex, j.TargetIndex, j.SourceUUID, j.TargetUUID, j.Query)
	if err != nil {
		return err
	}
	migrateMu.Lock()
	j.VerifiedHash = fingerprint.Hash
	j.VerifiedCount = fingerprint.Count
	j.Progress = fingerprint.Count
	j.Total = fingerprint.Count
	j.Status = "completed"
	if j.Remap.enabled() {
		j.Status = "verifying"
	}
	if j.FinalIndex != "" && j.ReplacePhase != "finalizing" && j.ReplacePhase != "complete" {
		j.Status = "verified"
	}
	migrateMu.Unlock()
	jobLog(j, "verified %d documents; destination %s remains write-blocked", fingerprint.Count, j.TargetIndex)
	return nil
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
	var req struct {
		AllowLiveSource bool `json:"allow_live_source"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil && err != io.EOF {
		httpJSON(w, 400, map[string]string{"error": "invalid Continue request"})
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
	if req.AllowLiveSource && !st.Active.AllowLiveSource {
		// Only convert an unfinished, non-destructive plan. A mixed plan could
		// otherwise modify a source while revalidating an older completed copy.
		for _, idx := range st.Active.Indices {
			if !idx.AllowLiveSource && indexFinished(idx.Status) {
				httpJSON(w, 409, map[string]string{"error": "cannot change a completed strict checkpoint to live copy; start a separate new migration"})
				return
			}
			if err := validateLiveJob(liveCheckpointJob(idx)); err != nil {
				httpJSON(w, 409, map[string]string{"error": err.Error()})
				return
			}
		}
		previous := GetState()
		st.Active.AllowLiveSource = true
		for i := range st.Active.Indices {
			st.Active.Indices[i].AllowLiveSource = true
		}
		SetActiveMigration(st.Active)
		if err := SaveState(s.StatePath); err != nil {
			restoreState(previous)
			httpJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
	}
	var jobs []migrateJob
	for _, idx := range st.Active.Indices {
		if indexFinished(idx.Status) {
			if err := s.verifyFinished(r.Context(), st.Active, idx); err != nil {
				httpJSON(w, 409, map[string]string{"error": err.Error()})
				return
			}
			continue
		}
		if idx.Remap.uncertain() {
			httpJSON(w, 409, map[string]string{"error": "original deletion outcome unknown for " + idx.Name + "; manual reconciliation required; destination: " + idx.TargetIndex + "; backup: " + idx.Remap.BackupIndex})
			return
		}
		if idx.ReplacePhase == "delete_pending" || idx.ReplacePhase == "clone_pending" {
			httpJSON(w, 409, map[string]string{"error": "replacement outcome unknown for " + idx.Name + "; manual reconciliation required; verified copy: " + idx.StageIndex})
			return
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
			taskCtx, taskCancel := context.WithTimeout(r.Context(), 30*time.Second)
			_, err := dst.GetTaskStatus(taskCtx, idx.TaskID)
			taskCancel()
			if err != nil {
				httpJSON(w, 409, map[string]string{"error": "native task " + idx.TaskID + " cannot be verified"})
				return
			}
		}
		// Old checkpoints without a choice must default to skipping existing destinations.
		overwrite := st.Active.Overwrite != nil && *st.Active.Overwrite
		jobs = append(jobs, migrateJob{
			AllowLiveSource: idx.AllowLiveSource, CapturedHash: idx.CapturedHash, CapturedCount: idx.CapturedCount,
			Remap: idx.Remap, SourceIndex: idx.Name, TargetIndex: checkpointTarget(idx), Status: "queued",
			SourceUUID: idx.SourceUUID, VerifiedHash: idx.VerifiedHash, VerifiedCount: idx.VerifiedCount, RetainedTargets: append([]string(nil), idx.RetainedTargets...),
			Resume:     idx.Status == "failed" || idx.Status == "running" || idx.Status == "verified" || idx.Status == "verifying",
			FinalIndex: idx.FinalIndex, ReplaceUUID: idx.ReplaceUUID, ReplacePhase: idx.ReplacePhase, StageIndex: idx.StageIndex, StageUUID: idx.StageUUID, Local: st.Active.Source.ClusterUUID == st.Active.Dest.ClusterUUID,
			Overwrite: overwrite, Query: st.Active.Query, Direction: st.Active.Direction,
			ForceScroll: st.Active.ForceScroll, TaskID: idx.TaskID,
			TargetUUID: idx.TargetUUID, SourceWasClosed: idx.SourceWasClosed, SourceWriteBlocked: idx.SourceWriteBlocked,
			Progress: idx.Transferred, Total: idx.DocsTotal, Logs: append([]string(nil), idx.Logs...),
		})
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
	if previous.Active != nil {
		for _, idx := range previous.Active.Indices {
			if idx.Remap.uncertain() || idx.ReindexUncertain || idx.TaskID != "" || idx.ReplacePhase == "delete_pending" || idx.ReplacePhase == "clone_pending" {
				httpJSON(w, 409, map[string]string{"error": "cannot abandon an unresolved native task or replacement request; reconcile its outcome first"})
				return
			}
		}
		if err := s.restoreAbandonedSources(r.Context(), previous.Active); err != nil {
			httpJSON(w, 409, map[string]string{"error": "cannot abandon before restoring source state: " + err.Error()})
			return
		}
	}
	AbandonMigration()
	if err := SaveState(s.StatePath); err != nil {
		restoreState(previous)
		httpJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}

	httpJSON(w, 200, map[string]string{"status": "abandoned"})
}

// Abandon must not erase the only record that a source needs to be closed.
// Restore only the recorded cluster/index identities; retain the checkpoint on
// any failure so the operator can reconnect and retry safely.
func (s *Server) restoreAbandonedSources(ctx context.Context, a *ActiveMigration) error {
	src, cfg, saved := s.Source, s.SourceCfg, a.Source
	if a.Direction == "target->source" {
		src, cfg, saved = s.Target, s.TargetCfg, a.Dest
	}
	checked := false
	for _, idx := range a.Indices {
		if idx.Remap.SourceDeleted || idx.SourceWasClosed == nil || !*idx.SourceWasClosed {
			continue
		}
		if !checked {
			if len(cfg.Hosts) != 1 || saved.ClusterUUID == "" {
				return fmt.Errorf("original source cluster identity is unavailable")
			}
			info, err := src.GetClusterInfo(ctx, cfg.Hosts[0])
			if err != nil {
				return err
			}
			if info.ClusterUUID != saved.ClusterUUID {
				return fmt.Errorf("reconnect the original source cluster")
			}
			checked = true
		}
		if err := restoreClosed(ctx, src, idx.Name, idx.SourceUUID); err != nil {
			return err
		}
	}
	return nil
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

func (s *Server) runScrollMigration(ctx context.Context, j *migrateJob) error {
	src := s.Source
	dst := s.Target
	if j.Direction == "target->source" {
		src, dst = s.Target, s.Source
	}
	return copyDocuments(ctx, src, dst, j)
}

func copyDocuments(ctx context.Context, src, dst *elastic.Client, j *migrateJob) error {
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
	var captured elastic.FingerprintAccumulator
	for batch := range docCh {
		if j.AllowLiveSource {
			if err := captured.Add(batch); err != nil {
				return err
			}
		}
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
	if j.AllowLiveSource {
		fp := captured.Result()
		j.CapturedHash, j.CapturedCount = fp.Hash, fp.Count
	}
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

	if err := elastic.ValidateMigrationVersions(src.Version, dst.Version); err != nil {
		return nil, nil, err
	}
	if saved != nil && (src.ClusterUUID != saved.Source.ClusterUUID || dst.ClusterUUID != saved.Dest.ClusterUUID) {
		return nil, nil, fmt.Errorf("connected clusters do not match the saved migration; reconnect the original source and destination")
	}
	return src, dst, nil
}

func jobLog(j *migrateJob, format string, args ...any) {
	migrateMu.Lock()
	defer migrateMu.Unlock()
	j.Logs = append(j.Logs, fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...)))
}

func migrationIndexUUID(ctx context.Context, client *elastic.Client, name string) (string, error) {
	detail, err := client.GetIndexDetail(ctx, name)
	if err != nil {
		return "", err
	}
	settings, _ := detail.Settings["index"].(map[string]any)
	uuid, _ := settings["uuid"].(string)
	if uuid == "" {
		return "", fmt.Errorf("destination %s has no index UUID; cannot checkpoint ownership", name)
	}
	return uuid, nil
}

func pollMigrationTask(ctx context.Context, dst *elastic.Client, j *migrateJob) (*elastic.MigrationTask, error) {
	taskID := j.TaskID
	for {
		task, err := dst.GetTaskStatus(ctx, taskID)
		if err != nil {
			return nil, fmt.Errorf("cannot verify native task %s; Continue will reconnect when the cluster is available: %w", taskID, err)
		}
		migrateMu.Lock()
		j.Progress, j.Total = task.Created+task.Updated+task.Deleted, task.Total
		if task.Status != "running" {
			j.TaskID = ""
		}
		// Completion is only recorded after independent verification.
		migrateMu.Unlock()
		if task.Status != "running" {
			jobLog(j, "native reindex %s: %d/%d docs", task.Status, j.Progress, j.Total)
			return task, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func checkpointTarget(idx IndexState) string {
	if idx.TargetIndex != "" {
		return idx.TargetIndex
	}
	return idx.Name
}
