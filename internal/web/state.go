package web

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type IndexState struct {
	Remap        RemapState `json:"remap,omitempty"`
	FinalIndex   string     `json:"final_index,omitempty"`
	ReplaceUUID  string     `json:"replace_uuid,omitempty"`
	ReplacePhase string     `json:"replace_phase,omitempty"`
	StageIndex   string     `json:"stage_index,omitempty"`
	StageUUID    string     `json:"stage_uuid,omitempty"`
	Local        bool       `json:"local,omitempty"`

	TargetIndex      string   `json:"target_index,omitempty"`
	SourceUUID       string   `json:"source_uuid,omitempty"`
	VerifiedHash     string   `json:"verified_hash,omitempty"`
	VerifiedCount    int64    `json:"verified_count"`
	RetainedTargets  []string `json:"retained_targets,omitempty"`
	TargetUUID       string   `json:"target_uuid,omitempty"`
	SourceWasClosed  *bool    `json:"source_was_closed,omitempty"`
	TaskID           string   `json:"task_id,omitempty"`
	ReindexUncertain bool     `json:"reindex_uncertain,omitempty"`
	Name             string   `json:"name"`
	DocsTotal        int64    `json:"docs_total"`
	Status           string   `json:"status"`
	Transferred      int64    `json:"transferred"`
	Logs             []string `json:"logs,omitempty"`
	Error            string   `json:"error,omitempty"`
}

type ConnectionState struct {
	Host        string `json:"host"`
	ClusterName string `json:"cluster_name"`
	ClusterUUID string `json:"cluster_uuid"`
}

type MigrationState struct {
	Preview *PreviewRecovery `json:"preview_recovery,omitempty"`
	Active  *ActiveMigration `json:"active,omitempty"`
	History []HistoryEntry   `json:"history,omitempty"`
}

type ActiveMigration struct {
	Description string          `json:"description"`
	Query       string          `json:"query,omitempty"`
	Direction   string          `json:"direction,omitempty"`
	ForceScroll bool            `json:"force_scroll,omitempty"`
	Overwrite   *bool           `json:"overwrite,omitempty"`
	Source      ConnectionState `json:"source"`
	Dest        ConnectionState `json:"dest"`
	Indices     []IndexState    `json:"indices"`
	StartedAt   string          `json:"started_at"`
	UpdatedAt   string          `json:"updated_at"`
}

type HistoryEntry struct {
	Overwrite   *bool           `json:"overwrite,omitempty"`
	Description string          `json:"description"`
	Query       string          `json:"query,omitempty"`
	Direction   string          `json:"direction,omitempty"`
	Source      ConnectionState `json:"source"`
	Dest        ConnectionState `json:"dest"`
	Indices     []IndexState    `json:"indices"`
	StartedAt   string          `json:"started_at"`
	CompletedAt string          `json:"completed_at"`
}

var (
	stateMu sync.Mutex
	state   MigrationState
)

func LoadState(path string) error {
	stateMu.Lock()
	defer stateMu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			state = MigrationState{}
			return nil
		}
		return err
	}
	var loaded MigrationState
	if err := json.Unmarshal(data, &loaded); err != nil {
		return err
	}
	state = loaded
	return nil
}

func SaveState(path string) error {
	stateMu.Lock()
	defer stateMu.Unlock()
	if state.Active != nil {
		state.Active.UpdatedAt = time.Now().Format(time.RFC3339)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	// Replace atomically so interruption cannot leave a truncated checkpoint.
	f, err := os.CreateTemp(filepath.Dir(path), ".migration-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	// Persist the directory entry as well as the checkpoint contents.
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func GetState() MigrationState {
	stateMu.Lock()
	defer stateMu.Unlock()
	return cloneState(state)
}

func cloneState(value MigrationState) MigrationState {
	// All fields are JSON data; cloning prevents readers from racing with updates.
	data, _ := json.Marshal(value)
	var copy MigrationState
	_ = json.Unmarshal(data, &copy)
	return copy
}

func SetActiveMigration(active *ActiveMigration) {
	stateMu.Lock()
	defer stateMu.Unlock()
	state.Active = cloneState(MigrationState{Active: active}).Active
}

func UpdateIndexState(name string, status string, transferred int64, log string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	if state.Active == nil {
		return
	}
	for i := range state.Active.Indices {
		if state.Active.Indices[i].Name == name {
			state.Active.Indices[i].Status = status
			if transferred >= 0 {
				state.Active.Indices[i].Transferred = transferred
			}
			if log != "" {
				state.Active.Indices[i].Logs = append(state.Active.Indices[i].Logs, log)
			}
			return
		}
	}
}

func SetIndexError(name string, errMsg string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	if state.Active == nil {
		return
	}
	for i := range state.Active.Indices {
		if state.Active.Indices[i].Name == name {
			state.Active.Indices[i].Status = "failed"
			state.Active.Indices[i].Error = errMsg
			return
		}
	}
}

func CompleteMigration() {
	stateMu.Lock()
	defer stateMu.Unlock()
	if state.Active == nil {
		return
	}
	h := HistoryEntry{
		Overwrite:   state.Active.Overwrite,
		Description: state.Active.Description,
		Query:       state.Active.Query,
		Direction:   state.Active.Direction,
		Source:      state.Active.Source,
		Dest:        state.Active.Dest,
		Indices:     state.Active.Indices,
		StartedAt:   state.Active.StartedAt,
		CompletedAt: time.Now().Format(time.RFC3339),
	}
	state.History = append([]HistoryEntry{h}, state.History...)
	if len(state.History) > 10 {
		state.History = state.History[:10]
	}
	state.Active = nil
}

func AbandonMigration() {
	stateMu.Lock()
	defer stateMu.Unlock()
	if state.Active == nil {
		return
	}
	for i := range state.Active.Indices {
		if !indexFinished(state.Active.Indices[i].Status) {
			state.Active.Indices[i].Status = "abandoned"
		}
	}
	h := HistoryEntry{
		Overwrite:   state.Active.Overwrite,
		Description: state.Active.Description + " (abandoned)",
		Query:       state.Active.Query,
		Direction:   state.Active.Direction,
		Source:      state.Active.Source,
		Dest:        state.Active.Dest,
		Indices:     state.Active.Indices,
		StartedAt:   state.Active.StartedAt,
		CompletedAt: time.Now().Format(time.RFC3339),
	}
	state.History = append([]HistoryEntry{h}, state.History...)
	if len(state.History) > 10 {
		state.History = state.History[:10]
	}
	state.Active = nil
}

func indexFinished(status string) bool { return status == "completed" || status == "skipped" }

func storeJob(j migrateJob) {
	stateMu.Lock()
	defer stateMu.Unlock()
	if state.Active == nil {
		return
	}
	for i := range state.Active.Indices {
		idx := &state.Active.Indices[i]
		if idx.Name == j.SourceIndex {
			idx.Remap = j.Remap
			idx.Status, idx.Transferred, idx.Error = j.Status, j.Progress, j.Error
			idx.TaskID, idx.ReindexUncertain = j.TaskID, j.ReindexUncertain
			idx.TargetUUID, idx.SourceWasClosed = j.TargetUUID, j.SourceWasClosed
			idx.TargetIndex, idx.SourceUUID = j.TargetIndex, j.SourceUUID
			idx.FinalIndex, idx.ReplaceUUID, idx.ReplacePhase = j.FinalIndex, j.ReplaceUUID, j.ReplacePhase
			idx.StageIndex, idx.StageUUID, idx.Local = j.StageIndex, j.StageUUID, j.Local
			idx.VerifiedHash, idx.VerifiedCount = j.VerifiedHash, j.VerifiedCount
			idx.RetainedTargets = append([]string(nil), j.RetainedTargets...)
			idx.Logs = append([]string(nil), j.Logs...)
			if j.Total > 0 {
				idx.DocsTotal = j.Total
			}
			return
		}
	}
}

func restoreState(saved MigrationState) {
	stateMu.Lock()
	defer stateMu.Unlock()
	state = cloneState(saved)
}
