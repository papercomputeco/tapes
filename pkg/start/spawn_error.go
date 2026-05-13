package start

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const spawnErrorFileName = "last-spawn-error.json"

// SpawnErrorReason classifies why the most recent daemon spawn exited before
// becoming healthy. The bootstrap path in `tapes deck` reads this to decide
// whether it can recover (e.g. by bringing up the local Postgres container)
// without itself opening a database connection.
type SpawnErrorReason string

const (
	// ReasonPostgresUnreachable means the daemon could not connect to the
	// configured Postgres DSN. Recoverable by `tapes local up` when the DSN
	// points at the local default and Docker is available.
	ReasonPostgresUnreachable SpawnErrorReason = "postgres_unreachable"
	// ReasonOther covers everything else: listener binds, embedder failures,
	// vector store failures, etc. Bootstrap surfaces these as-is.
	ReasonOther SpawnErrorReason = "other"
)

// SpawnError is written by the daemon when its startup fails and read by
// bootstrap callers (`tapes deck`) to recover or surface a styled message.
// It is a transient record — the daemon clears it on a successful start, and
// bootstrap clears it before each attempt.
type SpawnError struct {
	Reason SpawnErrorReason `json:"reason"`
	// DSN is the Postgres DSN the daemon attempted, if relevant.
	DSN string `json:"dsn,omitempty"`
	// Detail is the underlying error message for surfacing in CLI output.
	Detail string    `json:"detail,omitempty"`
	At     time.Time `json:"at"`
}

func (m *Manager) spawnErrorPath() string {
	return filepath.Join(m.Dir, spawnErrorFileName)
}

// RecordSpawnError persists a SpawnError to the manager's directory. The
// caller (the daemon) should set Reason and any relevant fields; At is
// stamped here. Errors writing the sentinel are returned but typically
// ignored by callers — failing to record is strictly less useful than
// failing to start, which is the actual problem.
func (m *Manager) RecordSpawnError(spawnErr SpawnError) error {
	if m == nil {
		return errors.New("nil start manager")
	}
	spawnErr.At = time.Now()
	data, err := json.MarshalIndent(spawnErr, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling spawn error: %w", err)
	}
	if err := os.WriteFile(m.spawnErrorPath(), data, 0o600); err != nil {
		return fmt.Errorf("writing spawn error: %w", err)
	}
	return nil
}

// LoadSpawnError reads the last recorded SpawnError. Returns (nil, nil) when
// no sentinel is present. Corrupt sentinels are cleared and treated as
// missing rather than surfaced as an error.
func (m *Manager) LoadSpawnError() (*SpawnError, error) {
	if m == nil {
		return nil, errors.New("nil start manager")
	}
	data, err := os.ReadFile(m.spawnErrorPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading spawn error: %w", err)
	}
	spawnErr := &SpawnError{}
	if err := json.Unmarshal(data, spawnErr); err != nil {
		_ = m.ClearSpawnError()
		return nil, nil
	}
	return spawnErr, nil
}

// ClearSpawnError removes any persisted SpawnError. A missing file is not
// an error.
func (m *Manager) ClearSpawnError() error {
	if m == nil {
		return errors.New("nil start manager")
	}
	if err := os.Remove(m.spawnErrorPath()); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("removing spawn error: %w", err)
	}
	return nil
}
