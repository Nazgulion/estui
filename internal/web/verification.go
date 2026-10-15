package web

import (
	"context"
	"errors"
	"fmt"
	"time"

	"estui/internal/elastic"
)

func verifyIdentity(ctx context.Context, c *elastic.Client, name, uuid string, source bool) error {
	detail, err := c.GetIndexDetail(ctx, name)
	if err != nil {
		return err
	}
	actual := fmt.Sprint(elastic.Setting(detail, "uuid"))
	if uuid == "" || actual != uuid {
		return fmt.Errorf("index %s identity changed; refusing to continue", name)
	}
	if !elastic.WriteBlocked(detail) {
		return fmt.Errorf("index %s must remain write-blocked for verification", name)
	}
	if source {
		_, err = elastic.SourceIdentity(detail)
		return err
	}
	if len(detail.Aliases) > 0 {
		return fmt.Errorf("destination %s unexpectedly has aliases", name)
	}
	return nil
}

// verifyCopy reads both indices independently and compares complete identities
// and canonical contents. It rechecks UUIDs and write blocks after both scans.
func verifyCopy(ctx context.Context, src, dst *elastic.Client, source, target, sourceUUID, targetUUID, query string) (fingerprint elastic.Fingerprint, err error) {
	for _, item := range []struct {
		client     *elastic.Client
		name, uuid string
		source     bool
	}{{src, source, sourceUUID, true}, {dst, target, targetUUID, false}} {
		if err := verifyIdentity(ctx, item.client, item.name, item.uuid, item.source); err != nil {
			return fingerprint, err
		}
		status, e := previewIndexStatus(ctx, item.client, item.name)
		if e != nil {
			return fingerprint, e
		}
		if status == "close" {
			client, name, uuid := item.client, item.name, item.uuid
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				current, e := migrationIndexUUID(cleanup, client, name)
				if e == nil && current != uuid {
					e = fmt.Errorf("index %s replaced during verification; refusing to close", name)
				}
				if e == nil {
					e = client.CloseIndex(cleanup, name)
				}
				err = errors.Join(err, e)
			}()
			if e := client.OpenIndex(ctx, name); e != nil {
				return fingerprint, e
			}
			if e := waitPreviewIndexOpen(ctx, client, name); e != nil {
				return fingerprint, e
			}
		}
		if err := item.client.RefreshIndex(ctx, item.name); err != nil {
			return fingerprint, err
		}
	}
	fingerprint, err = src.Fingerprint(ctx, source, query)
	if err != nil {
		return fingerprint, err
	}
	actual, err := dst.Fingerprint(ctx, target, "")
	if err != nil {
		return fingerprint, err
	}
	if fingerprint != actual {
		return fingerprint, fmt.Errorf("verification failed for %s: source count=%d hash=%s, destination count=%d hash=%s", source, fingerprint.Count, fingerprint.Hash, actual.Count, actual.Hash)
	}
	if err = verifyIdentity(ctx, src, source, sourceUUID, true); err != nil {
		return fingerprint, err
	}
	err = verifyIdentity(ctx, dst, target, targetUUID, false)
	return fingerprint, err
}

func (s *Server) verifyFinished(ctx context.Context, a *ActiveMigration, idx IndexState) (err error) {
	if idx.AllowLiveSource || idx.Status == "copied_live" {
		if idx.Status == "skipped" {
			return nil
		}
		return s.verifyFinishedLive(ctx, a, idx)
	}
	if idx.Status != "completed" {
		return nil
	}
	if idx.SourceUUID == "" || idx.TargetUUID == "" || idx.VerifiedHash == "" {
		return fmt.Errorf("completed index %s has no verification checkpoint; manual verification is required", idx.Name)
	}
	src, dst := s.Source, s.Target
	if a.Direction == "target->source" {
		src, dst = dst, src
	}
	if idx.Remap.enabled() && idx.Remap.SourceDeleted {
		j := &migrateJob{SourceIndex: idx.Name, SourceUUID: idx.SourceUUID, TargetIndex: idx.TargetIndex, TargetUUID: idx.TargetUUID, VerifiedHash: idx.VerifiedHash, VerifiedCount: idx.VerifiedCount, Remap: idx.Remap}
		return s.verifyRemapAfterDeletion(ctx, src, dst, j)
	}
	if idx.Remap.enabled() {
		j := &migrateJob{SourceIndex: idx.Name, TargetIndex: idx.TargetIndex, TargetUUID: idx.TargetUUID, Remap: idx.Remap}
		if err := checkRemapMapping(ctx, dst, j); err != nil {
			return err
		}
	}
	target := idx.TargetIndex
	if target == "" {
		target = idx.Name
	}
	// Completed checkpoints also describe the intended closed state after a crash
	// during revalidation, when the previous process could not execute its defers.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if idx.SourceWasClosed != nil && *idx.SourceWasClosed {
			err = errors.Join(err, restoreClosed(cleanup, src, idx.Name, idx.SourceUUID))
		}
		err = errors.Join(err, restoreClosed(cleanup, dst, target, idx.TargetUUID))
	}()

	actual, err := verifyCopy(ctx, src, dst, idx.Name, target, idx.SourceUUID, idx.TargetUUID, a.Query)
	if err != nil {
		return err
	}
	if actual.Hash != idx.VerifiedHash || actual.Count != idx.VerifiedCount {
		return fmt.Errorf("completed index %s differs from its saved verification", idx.Name)
	}
	return nil
}

func restoreClosed(ctx context.Context, client *elastic.Client, name, uuid string) error {
	actual, err := migrationIndexUUID(ctx, client, name)
	if err != nil {
		return err
	}
	if uuid == "" || actual != uuid {
		return fmt.Errorf("index %s was replaced; refusing to restore its state", name)
	}
	return client.CloseIndex(ctx, name)
}

func verifyDestinationOwnership(ctx context.Context, dst *elastic.Client, name, uuid string) error {
	detail, err := dst.GetIndexDetail(ctx, name)
	if err != nil {
		return err
	}
	if uuid == "" || elastic.Setting(detail, "uuid") != uuid {
		return fmt.Errorf("destination %s identity changed; refusing to change write block", name)
	}
	if len(detail.Aliases) != 0 {
		return fmt.Errorf("destination %s unexpectedly has aliases", name)
	}
	return nil
}

// Only release the verification block after successful verification and closure.
// Missing metadata in older checkpoints conservatively retains the block.
func restoreDestinationWriteBlock(ctx context.Context, dst *elastic.Client, name, uuid string, sourceBlocked *bool) error {
	if sourceBlocked == nil || *sourceBlocked {
		return nil
	}
	if err := verifyDestinationOwnership(ctx, dst, name, uuid); err != nil {
		return err
	}
	if err := dst.SetWriteBlock(ctx, name, false); err != nil {
		return fmt.Errorf("restore destination write setting: %w", err)
	}
	detail, err := dst.GetIndexDetail(ctx, name)
	if err != nil {
		return err
	}
	if elastic.Setting(detail, "uuid") != uuid || elastic.WriteBlocked(detail) {
		return fmt.Errorf("destination %s write setting could not be restored", name)
	}
	return nil
}
