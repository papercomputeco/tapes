package start

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

const (
	stateFileName = "start.json"
	logFileName   = "start.log"
	lockFileName  = "start.lock"
	stateVersion  = 1
)

type AgentSession struct {
	Name      string    `json:"name"`
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
}

type State struct {
	Version          int            `json:"version"`
	DaemonPID        int            `json:"daemon_pid"`
	ProxyURL         string         `json:"proxy_url"`
	APIURL           string         `json:"api_url"`
	ShutdownWhenIdle bool           `json:"shutdown_when_idle"`
	Agents           []AgentSession `json:"agents"`
	LogPath          string         `json:"log_path"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

type Manager struct {
	Dir       string
	StatePath string
	LogPath   string
	LockPath  string
}

type Lock struct {
	file *os.File
}

func NewManager(configDir string) (*Manager, error) {
	manager := dotdir.NewManager()
	dir, err := manager.Target(configDir)
	if err != nil {
		return nil, err
	}

	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolving home dir: %w", err)
		}
		dir = filepath.Join(home, ".tapes")
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating tapes dir: %w", err)
	}

	return &Manager{
		Dir:       dir,
		StatePath: filepath.Join(dir, stateFileName),
		LogPath:   filepath.Join(dir, logFileName),
		LockPath:  filepath.Join(dir, lockFileName),
	}, nil
}

func (m *Manager) Lock() (*Lock, error) {
	file, err := os.OpenFile(m.LockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening lock file: %w", err)
	}

	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return nil, fmt.Errorf("locking start file: %w", err)
	}

	return &Lock{file: file}, nil
}

func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		_ = l.file.Close()
		return fmt.Errorf("unlocking start file: %w", err)
	}
	return l.file.Close()
}

func (m *Manager) LoadState() (*State, error) {
	data, err := os.ReadFile(m.StatePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading start state: %w", err)
	}

	state := &State{}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("parsing start state: %w", err)
	}

	return state, nil
}

func (m *Manager) SaveState(state *State) error {
	if state == nil {
		return errors.New("cannot save nil state")
	}
	if state.Version == 0 {
		state.Version = stateVersion
	}
	state.UpdatedAt = time.Now()
	if state.LogPath == "" {
		state.LogPath = m.LogPath
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling start state: %w", err)
	}

	tmpFile, err := os.CreateTemp(m.Dir, "start-state-*.json")
	if err != nil {
		return fmt.Errorf("creating temp state file: %w", err)
	}

	if err := tmpFile.Chmod(0o600); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("chmod temp state file: %w", err)
	}

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("writing temp state file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing temp state file: %w", err)
	}

	if err := os.Rename(tmpFile.Name(), m.StatePath); err != nil {
		return fmt.Errorf("persisting state file: %w", err)
	}

	return nil
}

func (m *Manager) ClearState() error {
	if err := os.Remove(m.StatePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("removing start state: %w", err)
	}
	return nil
}
