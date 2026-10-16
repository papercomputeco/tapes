package derive

import (
	"errors"
	"time"
)

// RunState is the lifecycle state of an asynchronous derive run.
type RunState string

const (
	// RunPending is a run that has been created and snapshotted but
	// that no process has started executing yet.
	RunPending RunState = "pending"

	// RunRunning is a run an executor has started. It stays running
	// across a restart of that executor until another process resumes it.
	RunRunning RunState = "running"

	// RunCompleted is a run whose every snapshotted session finished.
	// Sessions that failed are counted in SessionsFailed and sampled in
	// Errors; completion does not mean every session succeeded.
	RunCompleted RunState = "completed"

	// RunCanceled is a run an operator stopped before it completed.
	RunCanceled RunState = "canceled"
)

// Active reports whether the state is one a run can still advance from.
func (s RunState) Active() bool {
	return s == RunPending || s == RunRunning
}

// Run is the durable record of one asynchronous whole-store re-derive.
//
// The session set is fixed when the run is created. Sessions captured after
// that are derived by the derive worker as they arrive, so a run converges
// the projection that existed when it started.
type Run struct {
	// ID identifies the run.
	ID string `json:"id"`

	// Project restricts the derive to one project; empty means all.
	Project string `json:"project,omitempty"`

	// State is the run's lifecycle state.
	State RunState `json:"state"`

	// SessionsTotal is the size of the snapshotted session set.
	SessionsTotal int `json:"sessions_total"`

	// SessionsDone counts finished sessions, failed ones included.
	SessionsDone int `json:"sessions_done"`

	// SessionsFailed counts finished sessions whose derive returned an error.
	SessionsFailed int `json:"sessions_failed"`

	// Report aggregates the derive reports of every successful session.
	Report *RederiveReport `json:"report,omitempty"`

	// Errors is a bounded sample of per-session failures.
	Errors []string `json:"errors"`

	// Worker is the host name of the process that last advanced the run.
	Worker string `json:"worker,omitempty"`

	// CreatedAt is when the run was created.
	CreatedAt time.Time `json:"created_at"`

	// StartedAt is when an executor first started the run.
	StartedAt *time.Time `json:"started_at,omitempty"`

	// UpdatedAt is when the run last advanced. A running run whose UpdatedAt
	// stops moving has lost its executor and is waiting to be resumed.
	UpdatedAt time.Time `json:"updated_at"`

	// FinishedAt is when the run completed or was canceled.
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

var (
	// ErrRunNotFound means no derive run has the requested id.
	ErrRunNotFound = errors.New("derive run not found")

	// ErrRunBusy means another process holds the run's execution lock.
	ErrRunBusy = errors.New("derive run is being executed by another process")
)
