package web

import (
	"context"
	"crypto/rand"
	"fmt"
	"unicode/utf8"

	"estui/internal/elastic"
)

func newCopyName(source string, local bool) (string, error) {
	base := source
	for len(base) > 180 {
		_, size := utf8.DecodeLastRuneInString(base)
		base = base[:len(base)-size]
	}
	token := make([]byte, 8)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	if local {
		return fmt.Sprintf("reindex_%s_%x", base, token), nil
	}
	return fmt.Sprintf("%s_reindex_%x", base, token), nil
}

func (s *Server) completeTransfer(ctx context.Context, src, dst *elastic.Client, j *migrateJob) error {
	if j.AllowLiveSource {
		return s.finishLiveCopy(ctx, src, dst, j)
	}
	if j.Remap.enabled() {
		return s.finishRemap(ctx, src, dst, j)
	}
	if err := s.finishCopy(ctx, src, dst, j); err != nil {
		return err
	}
	if j.FinalIndex == "" {
		return nil
	}
	migrateMu.Lock()
	j.StageIndex, j.StageUUID = j.TargetIndex, j.TargetUUID
	j.ReplacePhase = "verified"
	migrateMu.Unlock()
	if err := s.updateStateFromJob(j); err != nil {
		return err
	}
	return s.publishReplacement(ctx, src, dst, j)
}

// Destruction is a single explicitly authorized transition. An ambiguous delete
// or clone is never reissued by Continue, even after a process crash.
func (s *Server) publishReplacement(ctx context.Context, src, dst *elastic.Client, j *migrateJob) error {
	if j.Local || j.FinalIndex != j.SourceIndex || j.StageIndex == j.FinalIndex || j.StageUUID == "" || j.ReplaceUUID == "" {
		return fmt.Errorf("invalid replacement checkpoint; refusing mutation")
	}
	if j.ReplacePhase == "delete_pending" || j.ReplacePhase == "clone_pending" {
		return fmt.Errorf("replacement %s outcome is unknown; manual reconciliation required (verified copy: %s)", j.ReplacePhase, j.StageIndex)
	}
	fingerprint, err := verifyCopy(ctx, src, dst, j.SourceIndex, j.StageIndex, j.SourceUUID, j.StageUUID, j.Query)
	if err != nil {
		return err
	}
	if fingerprint.Hash != j.VerifiedHash || fingerprint.Count != j.VerifiedCount {
		return fmt.Errorf("staging copy no longer matches verified checkpoint")
	}
	if j.ReplacePhase == "verified" {
		// Clone's green-health prerequisite must be satisfied before deletion.
		if err := dst.WaitIndexGreen(ctx, j.StageIndex); err != nil {
			return err
		}
		original, err := dst.GetIndexDetail(ctx, j.FinalIndex)
		if err != nil {
			return err
		}
		if elastic.Setting(original, "uuid") != j.ReplaceUUID || !elastic.WriteBlocked(original) {
			return fmt.Errorf("confirmed destination changed or is writable; refusing overwrite")
		}
		// Pin the live clusters again before the only destructive migration operation.
		if _, _, err := s.validateClusters(ctx, GetState().Active); err != nil {
			return err
		}
		migrateMu.Lock()
		j.ReplacePhase = "delete_pending"
		migrateMu.Unlock()
		if err := s.updateStateFromJob(j); err != nil {
			return err
		}
		jobLog(j, "overwriting confirmed destination %s; verified copy retained at %s", j.FinalIndex, j.StageIndex)
		if err := dst.DeleteIndex(ctx, j.FinalIndex); err != nil {
			return err
		}
		migrateMu.Lock()
		j.ReplacePhase = "deleted"
		migrateMu.Unlock()
		if err := s.updateStateFromJob(j); err != nil {
			return err
		}
	}
	if j.ReplacePhase == "deleted" {
		// Clone creates the final name with a write block already installed; there is
		// no writable empty-index interval in which application writes can race copy.
		migrateMu.Lock()
		j.ReplacePhase = "clone_pending"
		migrateMu.Unlock()
		if err := s.updateStateFromJob(j); err != nil {
			return err
		}
		if err := dst.CloneBlocked(ctx, j.StageIndex, j.FinalIndex); err != nil {
			return err
		}
		uuid, err := migrationIndexUUID(ctx, dst, j.FinalIndex)
		if err != nil {
			return err
		}
		migrateMu.Lock()
		j.RetainedTargets = append(j.RetainedTargets, j.StageIndex)
		j.TargetIndex, j.TargetUUID = j.FinalIndex, uuid
		j.ReplacePhase = "finalizing"
		migrateMu.Unlock()
		if err := s.updateStateFromJob(j); err != nil {
			return err
		}
	}
	if j.ReplacePhase != "finalizing" && j.ReplacePhase != "complete" {
		return fmt.Errorf("unrecognized replacement phase %q", j.ReplacePhase)
	}
	if err := dst.WaitIndexGreen(ctx, j.TargetIndex); err != nil {
		return err
	}
	if err := s.finishCopy(ctx, src, dst, j); err != nil {
		return err
	}
	migrateMu.Lock()
	j.ReplacePhase = "complete"
	migrateMu.Unlock()
	return nil
}
