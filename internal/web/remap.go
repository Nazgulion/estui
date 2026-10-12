package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"

	"estui/internal/elastic"
)

// RemapOptions is the operator's immutable plan, persisted before any mutation.
type RemapOptions struct {
	Mapping        json.RawMessage `json:"mapping,omitempty"`
	Destination    string          `json:"destination"`
	OriginalAction string          `json:"original_action"`
	BackupIndex    string          `json:"backup_index,omitempty"`
	SourceUUID     string          `json:"source_uuid"`
	ConfirmRemoval bool            `json:"confirm_removal,omitempty"`
}
type RemapState struct {
	Options         RemapOptions    `json:"options"`
	Phase           string          `json:"phase,omitempty"`
	BackupIndex     string          `json:"backup_index,omitempty"`
	BackupUUID      string          `json:"backup_uuid,omitempty"`
	BackupHash      string          `json:"backup_hash,omitempty"`
	BackupCount     int64           `json:"backup_count,omitempty"`
	OriginalMapping json.RawMessage `json:"original_mapping,omitempty"`
	SourceDeleted   bool            `json:"source_deleted,omitempty"`
}

func (r RemapState) enabled() bool {
	return len(r.Options.Mapping) > 0 && !bytes.Equal(bytes.TrimSpace(r.Options.Mapping), []byte("null"))
}
func (r RemapState) uncertain() bool     { return r.Phase == "delete_pending" }
func removesOriginal(action string) bool { return action == "delete" || action == "backup" }

func decodeMapping(raw json.RawMessage, index string) (map[string]any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, err
		}
		raw = []byte(text)
	}
	if len(raw) > 1<<20 {
		return nil, fmt.Errorf("mapping exceeds 1 MiB")
	}
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil || m == nil {
		return nil, fmt.Errorf("mapping must be a JSON object")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("mapping must contain one JSON object")
	}
	if len(m) == 1 {
		if wrapper, ok := m[index].(map[string]any); ok {
			m = wrapper
		}
		if wrapper, ok := m["mappings"].(map[string]any); ok && len(m) == 1 {
			m = wrapper
		}
	}
	allowed := map[string]bool{"properties": true, "dynamic": true, "dynamic_templates": true, "date_detection": true, "numeric_detection": true, "dynamic_date_formats": true, "_source": true, "_meta": true, "_routing": true, "_field_names": true, "runtime": true, "enabled": true, "subobjects": true}
	for k := range m {
		if !allowed[k] {
			return nil, fmt.Errorf("unsupported mapping root %q; supply mappings only, without settings or aliases", k)
		}
	}
	if v, ok := m["_source"]; ok {
		source, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("_source must be an object")
		}
		if source["enabled"] == false || source["includes"] != nil || source["excludes"] != nil || (source["mode"] != nil && source["mode"] != "stored") {
			return nil, fmt.Errorf("custom mapping must preserve complete stored _source for verification")
		}
	}
	return m, nil
}
func validateNewIndexName(name string) error {
	if err := validateIndexNames([]string{name}); err != nil {
		return err
	}
	if len(name) > 255 || name != strings.ToLower(name) || strings.ContainsAny(name, ":#\"|+") || strings.ContainsAny(name[:1], "._-+") || strings.IndexFunc(name, unicode.IsSpace) >= 0 || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return fmt.Errorf("invalid new index name %q: use a lowercase, non-system Elasticsearch index name (at most 255 bytes)", name)
	}
	return nil
}
func validateRemapOptions(name string, o RemapOptions, req migrateRequest, sourceUUID string) (RemapOptions, error) {
	m, err := decodeMapping(o.Mapping, name)
	if err != nil {
		return o, err
	}
	o.Mapping, err = json.Marshal(m)
	if err != nil {
		return o, err
	}
	if o.OriginalAction == "" {
		o.OriginalAction = "keep"
	}
	if o.OriginalAction != "keep" && o.OriginalAction != "backup" && o.OriginalAction != "delete" {
		return o, fmt.Errorf("invalid original action")
	}
	if err := validateNewIndexName(o.Destination); err != nil {
		return o, err
	}
	if o.Destination == name {
		return o, fmt.Errorf("mapping reindex requires a new destination name")
	}
	if o.SourceUUID != sourceUUID {
		return o, fmt.Errorf("source index changed since confirmation")
	}
	if removesOriginal(o.OriginalAction) && !o.ConfirmRemoval {
		return o, fmt.Errorf("explicit acknowledgement is required to remove the original index")
	}
	if o.OriginalAction == "delete" && req.Query != "" && req.Query != "*" {
		return o, fmt.Errorf("filtered reindex cannot delete the original without a full backup")
	}
	if strings.HasPrefix(o.OriginalAction, "backup") {
		if err := validateNewIndexName(o.BackupIndex); err != nil {
			return o, err
		}
		if o.BackupIndex == name || o.BackupIndex == o.Destination {
			return o, fmt.Errorf("backup, source, and destination names must differ")
		}
	}
	return o, nil
}

// Elasticsearch may add defaults or dynamically discovered fields. Every supplied
// mapping option must still be present after creation and before source removal.
func mappingContains(actual, expected any) bool {
	if want, ok := expected.(map[string]any); ok {
		got, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range want {
			if !mappingContains(got[k], v) {
				return false
			}
		}
		return true
	}
	a, _ := json.Marshal(actual)
	b, _ := json.Marshal(expected)
	return bytes.Equal(a, b)
}
func checkRemapMapping(ctx context.Context, c *elastic.Client, j *migrateJob) error {
	expected, err := decodeMapping(j.Remap.Options.Mapping, j.SourceIndex)
	if err != nil {
		return err
	}
	d, err := c.GetIndexDetail(ctx, j.TargetIndex)
	if err != nil {
		return err
	}
	if elastic.Setting(d, "uuid") != j.TargetUUID || !mappingContains(d.Mappings, expected) {
		return fmt.Errorf("destination identity or supplied mapping changed")
	}
	return nil
}
func (s *Server) remapCheckpoint(j *migrateJob, phase string) error {
	migrateMu.Lock()
	j.Remap.Phase = phase
	j.Status = "verifying"
	migrateMu.Unlock()
	return s.updateStateFromJob(j)
}

func (s *Server) finishRemap(ctx context.Context, src, dst *elastic.Client, j *migrateJob) error {
	if j.Remap.uncertain() {
		return fmt.Errorf("original deletion outcome is unknown; manual reconciliation required")
	}
	if j.Remap.SourceDeleted {
		return s.completeDeletedRemap(ctx, src, dst, j)
	}
	if err := checkRemapMapping(ctx, dst, j); err != nil {
		return err
	}
	if err := s.finishCopy(ctx, src, dst, j); err != nil {
		return err
	}
	if j.Remap.Phase == "copying" {
		if err := s.remapCheckpoint(j, "copied"); err != nil {
			return err
		}
	}
	action := j.Remap.Options.OriginalAction
	if strings.HasPrefix(action, "backup") {
		if err := s.backupOriginal(ctx, src, j); err != nil {
			return err
		}
	}
	if removesOriginal(action) {
		if !j.Remap.Options.ConfirmRemoval {
			return fmt.Errorf("source removal was not explicitly confirmed")
		}
		// Recheck the new copy after the full backup, immediately before removal.
		if err := checkRemapMapping(ctx, dst, j); err != nil {
			return err
		}
		if err := s.finishCopy(ctx, src, dst, j); err != nil {
			return err
		}
		if _, _, err := s.validateClusters(ctx, GetState().Active); err != nil {
			return err
		}
		if err := verifyIdentity(ctx, src, j.SourceIndex, j.SourceUUID, true); err != nil {
			return err
		}
		// Close verified output before deletion; failure cannot strand an unfinished copy.
		if err := dst.CloseIndex(ctx, j.TargetIndex); err != nil {
			return err
		}
		if err := s.remapCheckpoint(j, "delete_pending"); err != nil {
			return err
		}
		jobLog(j, "removing confirmed original %s after destination verification", j.SourceIndex)
		if err := src.DeleteIndex(ctx, j.SourceIndex); err != nil {
			return err
		}
		migrateMu.Lock()
		j.Remap.SourceDeleted = true
		migrateMu.Unlock()
		if err := s.remapCheckpoint(j, "deleted"); err != nil {
			return err
		}
	}
	migrateMu.Lock()
	j.Remap.Phase = "complete"
	j.Status = "completed"
	migrateMu.Unlock()
	return nil
}

func (s *Server) backupOriginal(ctx context.Context, src *elastic.Client, j *migrateJob) error {
	r := j.Remap
	if r.Phase != "backup_verified" {
		name := r.Options.BackupIndex
		if r.Phase == "backup_copying" {
			// Retain any interrupted backup. Never refill an existing partially copied index.
			var err error
			name, err = newCopyName(r.BackupIndex, false)
			if err != nil {
				return err
			}
			migrateMu.Lock()
			j.RetainedTargets = append(j.RetainedTargets, r.BackupIndex)
			migrateMu.Unlock()
		}
		detail, err := src.GetIndexDetail(ctx, j.SourceIndex)
		if err != nil {
			return err
		}
		if _, err := elastic.SourceIdentity(detail); err != nil {
			return err
		}
		mapping, err := json.Marshal(detail.Mappings)
		if err != nil {
			return err
		}
		migrateMu.Lock()
		j.Remap.BackupIndex = name
		j.Remap.BackupUUID = ""
		j.Remap.OriginalMapping = mapping
		migrateMu.Unlock()
		if err := s.remapCheckpoint(j, "backup_copying"); err != nil {
			return err
		}
		if err := src.CheckTemplates(ctx, name); err != nil {
			return err
		}
		if err := src.CreateIndexFromSource(ctx, name, detail); err != nil {
			return err
		}
		uuid, err := migrationIndexUUID(ctx, src, name)
		if err != nil {
			return err
		}
		migrateMu.Lock()
		j.Remap.BackupUUID = uuid
		migrateMu.Unlock()
		if err := s.updateStateFromJob(j); err != nil {
			return err
		}
		jobLog(j, "backing up all original documents and mapping to source cluster index %s", name)
		copyJob := &migrateJob{SourceIndex: j.SourceIndex, TargetIndex: name}
		if err := copyDocuments(ctx, src, src, copyJob); err != nil {
			return err
		}
		if err := src.BlockWrites(ctx, name); err != nil {
			return err
		}
	}
	r = j.Remap
	fingerprint, err := verifyCopy(ctx, src, src, j.SourceIndex, r.BackupIndex, j.SourceUUID, r.BackupUUID, "")
	if err != nil {
		return err
	}
	detail, err := src.GetIndexDetail(ctx, r.BackupIndex)
	if err != nil {
		return err
	}
	mapping, err := json.Marshal(detail.Mappings)
	if err != nil {
		return err
	}
	if !bytes.Equal(mapping, r.OriginalMapping) {
		return fmt.Errorf("backup mapping does not match original mapping")
	}
	currentSource, err := src.GetIndexDetail(ctx, j.SourceIndex)
	if err != nil {
		return err
	}
	currentMapping, err := json.Marshal(currentSource.Mappings)
	if err != nil {
		return err
	}
	if !bytes.Equal(currentMapping, r.OriginalMapping) {
		return fmt.Errorf("source mapping changed while backing up")
	}
	if r.Phase == "backup_verified" && (fingerprint.Hash != r.BackupHash || fingerprint.Count != r.BackupCount) {
		return fmt.Errorf("backup differs from its checkpoint")
	}
	if err := src.CloseIndex(ctx, r.BackupIndex); err != nil {
		return err
	}
	migrateMu.Lock()
	j.Remap.BackupHash = fingerprint.Hash
	j.Remap.BackupCount = fingerprint.Count
	migrateMu.Unlock()
	return s.remapCheckpoint(j, "backup_verified")
}

func (s *Server) completeDeletedRemap(ctx context.Context, src, dst *elastic.Client, j *migrateJob) error {
	if err := s.verifyRemapAfterDeletion(ctx, src, dst, j); err != nil {
		return err
	}
	migrateMu.Lock()
	j.Remap.Phase = "complete"
	j.Status = "completed"
	migrateMu.Unlock()
	return nil
}

func (s *Server) verifyRemapAfterDeletion(ctx context.Context, src, dst *elastic.Client, j *migrateJob) (err error) {
	// Saved successful outputs must return to their closed state even if a prior
	// process crashed after opening them during revalidation.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err = errors.Join(err, restoreClosed(cleanup, dst, j.TargetIndex, j.TargetUUID))
		if j.Remap.BackupUUID != "" {
			err = errors.Join(err, restoreClosed(cleanup, src, j.Remap.BackupIndex, j.Remap.BackupUUID))
		}
	}()
	if !j.Remap.SourceDeleted || !removesOriginal(j.Remap.Options.OriginalAction) || j.VerifiedHash == "" {
		return fmt.Errorf("invalid source deletion checkpoint")
	}
	if err := checkRemapMapping(ctx, dst, j); err != nil {
		return err
	}
	fp, err := fingerprintSavedIndex(ctx, dst, j.TargetIndex, j.TargetUUID)
	if err != nil {
		return err
	}
	if fp.Hash != j.VerifiedHash || fp.Count != j.VerifiedCount {
		return fmt.Errorf("destination differs from verified checkpoint after source removal")
	}
	if strings.HasPrefix(j.Remap.Options.OriginalAction, "backup") {
		r := j.Remap
		fp, err := fingerprintSavedIndex(ctx, src, r.BackupIndex, r.BackupUUID)
		if err != nil {
			return err
		}
		if fp.Hash != r.BackupHash || fp.Count != r.BackupCount {
			return fmt.Errorf("original backup differs from checkpoint")
		}
		d, err := src.GetIndexDetail(ctx, r.BackupIndex)
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(d.Mappings)
		if !bytes.Equal(raw, r.OriginalMapping) {
			return fmt.Errorf("original backup mapping changed")
		}
	}
	return nil
}
func fingerprintSavedIndex(ctx context.Context, c *elastic.Client, name, uuid string) (fp elastic.Fingerprint, err error) {
	if err = verifyIdentity(ctx, c, name, uuid, false); err != nil {
		return
	}
	status, e := previewIndexStatus(ctx, c, name)
	if e != nil {
		return fp, e
	}
	if status == "close" {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if e := restoreClosed(cleanup, c, name, uuid); e != nil {
				err = fmt.Errorf("restore closed index: %w (verification: %v)", e, err)
			}
		}()
		if err = c.OpenIndex(ctx, name); err != nil {
			return
		}
		if err = waitPreviewIndexOpen(ctx, c, name); err != nil {
			return
		}
	}
	if err = c.RefreshIndex(ctx, name); err != nil {
		return
	}
	fp, err = c.Fingerprint(ctx, name, "")
	if err != nil {
		return
	}
	err = verifyIdentity(ctx, c, name, uuid, false)
	return
}
