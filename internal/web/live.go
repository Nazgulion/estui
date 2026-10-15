package web

import (
	"context"
	"errors"
	"fmt"
	"time"

	"estui/internal/elastic"
)

const liveCopyNotice = "Captured documents verified against destination; original source retained and its initial open/closed state restored on exit. Later source additions, updates and deletions are not synchronized. Current-source equality is not verified."

func validateLiveJob(j *migrateJob) error {
	if j.FinalIndex != "" || j.ReplacePhase != "" || j.ReplaceUUID != "" || j.StageIndex != "" || j.TaskID != "" || j.ReindexUncertain {
		return fmt.Errorf("live copy cannot overwrite a destination or resume a replacement/native task; use a separate new copy")
	}
	if j.Remap.enabled() && (j.Remap.Options.OriginalAction != "keep" || j.Remap.SourceDeleted || j.Remap.BackupIndex != "" || (j.Remap.Phase != "copying" && j.Remap.Phase != "complete")) {
		return fmt.Errorf("live copy requires keeping the original; source backup/removal is not permitted")
	}
	if j.SourceIndex == j.TargetIndex && j.Local {
		return fmt.Errorf("live copy requires a separate destination index")
	}
	return nil
}

func liveCheckpointJob(idx IndexState) *migrateJob {
	return &migrateJob{AllowLiveSource: idx.AllowLiveSource, SourceIndex: idx.Name, SourceUUID: idx.SourceUUID,
		TargetIndex: idx.TargetIndex, TargetUUID: idx.TargetUUID, Local: idx.Local, SourceWriteBlocked: idx.SourceWriteBlocked,
		FinalIndex: idx.FinalIndex, ReplacePhase: idx.ReplacePhase, ReplaceUUID: idx.ReplaceUUID, StageIndex: idx.StageIndex,
		TaskID: idx.TaskID, ReindexUncertain: idx.ReindexUncertain, Remap: idx.Remap,
		CapturedHash: idx.CapturedHash, CapturedCount: idx.CapturedCount}
}

func verifyLiveSourceIdentity(ctx context.Context, src *elastic.Client, j *migrateJob) error {
	detail, err := src.GetIndexDetail(ctx, j.SourceIndex)
	if err != nil {
		return err
	}
	uuid, err := elastic.ReadableSourceIdentity(detail)
	if err != nil {
		return err
	}
	if j.SourceUUID == "" || uuid != j.SourceUUID {
		return fmt.Errorf("source identity changed during live copy")
	}
	return nil
}

func verifyCapturedDestination(ctx context.Context, dst *elastic.Client, j *migrateJob) (elastic.Fingerprint, error) {
	if j.CapturedHash == "" || j.CapturedCount < 0 {
		return elastic.Fingerprint{}, fmt.Errorf("live copy has no complete captured-document fingerprint; retry into a new destination")
	}
	if err := verifyIdentity(ctx, dst, j.TargetIndex, j.TargetUUID, false); err != nil {
		return elastic.Fingerprint{}, err
	}
	if err := dst.RefreshIndex(ctx, j.TargetIndex); err != nil {
		return elastic.Fingerprint{}, err
	}
	fp, err := dst.Fingerprint(ctx, j.TargetIndex, "")
	if err != nil {
		return fp, err
	}
	if fp.Hash != j.CapturedHash || fp.Count != j.CapturedCount {
		return fp, fmt.Errorf("captured-copy verification failed: captured count=%d hash=%s, destination count=%d hash=%s", j.CapturedCount, j.CapturedHash, fp.Count, fp.Hash)
	}
	if err := verifyIdentity(ctx, dst, j.TargetIndex, j.TargetUUID, false); err != nil {
		return fp, err
	}
	if j.Remap.enabled() {
		if err := checkRemapMapping(ctx, dst, j); err != nil {
			return fp, err
		}
	}
	return fp, nil
}

func (s *Server) finishLiveCopy(ctx context.Context, src, dst *elastic.Client, j *migrateJob) error {
	if err := validateLiveJob(j); err != nil {
		return err
	}
	uuid, err := migrationIndexUUID(ctx, dst, j.TargetIndex)
	if err != nil {
		return err
	}
	if j.TargetUUID == "" || uuid != j.TargetUUID {
		return fmt.Errorf("live-copy destination identity changed")
	}
	if err := dst.BlockWrites(ctx, j.TargetIndex); err != nil {
		return err
	}
	jobLog(j, "verifying destination IDs, routing and contents against the documents captured during copying; source writes are not frozen")
	fp, err := verifyCapturedDestination(ctx, dst, j)
	if err != nil {
		return err
	}
	if err := verifyLiveSourceIdentity(ctx, src, j); err != nil {
		return err
	}
	migrateMu.Lock()
	j.VerifiedHash, j.VerifiedCount = fp.Hash, fp.Count
	j.Progress, j.Total, j.Status = fp.Count, fp.Count, "copied_live"
	if j.Remap.enabled() {
		j.Remap.Phase = "complete"
	}
	migrateMu.Unlock()
	jobLog(j, "%s", liveCopyNotice)
	return nil
}

func (s *Server) verifyFinishedLive(ctx context.Context, a *ActiveMigration, idx IndexState) (err error) {
	if !idx.AllowLiveSource || idx.Status != "copied_live" || idx.VerifiedHash == "" || idx.VerifiedHash != idx.CapturedHash || idx.VerifiedCount != idx.CapturedCount {
		return fmt.Errorf("invalid live-copy verification checkpoint")
	}
	j := liveCheckpointJob(idx)
	if err := validateLiveJob(j); err != nil {
		return err
	}
	src, dst := s.Source, s.Target
	if a.Direction == "target->source" {
		src, dst = dst, src
	}
	// A previous process may have stopped after opening the source. Restore
	// its checkpointed state even when completed-copy revalidation fails.
	defer func() {
		if idx.SourceWasClosed != nil && *idx.SourceWasClosed {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err = errors.Join(err, restoreClosed(cleanup, src, idx.Name, idx.SourceUUID))
		}
	}()
	if err := verifyLiveSourceIdentity(ctx, src, j); err != nil {
		return err
	}
	// Pin ownership before changing the destination. Legacy checkpoints without
	// the captured source setting retain the old requirement and blocked state.
	if err := verifyDestinationOwnership(ctx, dst, j.TargetIndex, j.TargetUUID); err != nil {
		return err
	}
	if j.SourceWriteBlocked == nil {
		if err := verifyIdentity(ctx, dst, j.TargetIndex, j.TargetUUID, false); err != nil {
			return err
		}
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err = errors.Join(err, restoreClosed(cleanup, dst, j.TargetIndex, j.TargetUUID))
		if err == nil {
			err = restoreDestinationWriteBlock(cleanup, dst, j.TargetIndex, j.TargetUUID, j.SourceWriteBlocked)
		}
	}()
	// Install the block before opening a closed destination, then drain writes
	// with the block API before verification begins.
	status, err := previewIndexStatus(ctx, dst, j.TargetIndex)
	if err != nil {
		return err
	}
	if status == "close" {
		if err := dst.SetWriteBlock(ctx, j.TargetIndex, true); err != nil {
			return err
		}
		if err := dst.OpenIndex(ctx, j.TargetIndex); err != nil {
			return err
		}
		if err := waitPreviewIndexOpen(ctx, dst, j.TargetIndex); err != nil {
			return err
		}
	}
	if err := dst.BlockWrites(ctx, j.TargetIndex); err != nil {
		return err
	}
	_, err = verifyCapturedDestination(ctx, dst, j)
	return err
}
