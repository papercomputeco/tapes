package start

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

const defaultWaitForDaemonTimeout = 30 * time.Second

// APIReachable returns true if a GET to apiURL+/ping responds 200 within 2s.
func APIReachable(ctx context.Context, apiURL string) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	url := strings.TrimRight(apiURL, "/") + "/ping"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// ProcessAlive reports whether a process with pid is responsive to signal 0.
func ProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

// StateHealthy returns true if state is non-nil, points at a live daemon
// process, and the daemon's API responds to /ping.
func StateHealthy(ctx context.Context, state *State) bool {
	if state == nil || state.DaemonPID == 0 || state.APIURL == "" {
		return false
	}
	if !ProcessAlive(state.DaemonPID) {
		return false
	}
	return APIReachable(ctx, state.APIURL)
}

// SpawnOptions configures a daemon spawn.
type SpawnOptions struct {
	// ExecPath is the tapes binary to launch. Defaults to os.Executable().
	ExecPath string
	// ConfigDir is forwarded as --config-dir if non-empty.
	ConfigDir string
	// Debug forwards --debug.
	Debug bool
	// PostgresDSN forwards --postgres if non-empty.
	PostgresDSN string
	// LogPath is the file the daemon's stdout+stderr is appended to. Defaults
	// to manager.LogPath.
	LogPath string
}

// Spawn launches `tapes start --daemon` as a backgrounded process. The
// returned channel closes when the child process exits, so callers can detect
// crashes during startup.
func Spawn(ctx context.Context, manager *Manager, opts SpawnOptions) (<-chan struct{}, error) {
	if manager == nil {
		return nil, errors.New("nil start manager")
	}

	execPath := opts.ExecPath
	if execPath == "" {
		var err error
		execPath, err = os.Executable()
		if err != nil {
			return nil, fmt.Errorf("resolving executable: %w", err)
		}
	}

	logPath := opts.LogPath
	if logPath == "" {
		logPath = manager.LogPath
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening log file: %w", err)
	}

	args := []string{"start", "--daemon"}
	if opts.Debug {
		args = append(args, "--debug")
	}
	if opts.ConfigDir != "" {
		args = append(args, "--config-dir", opts.ConfigDir)
	}
	if opts.PostgresDSN != "" {
		args = append(args, "--postgres", opts.PostgresDSN)
	}

	cmd := exec.CommandContext(ctx, execPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("starting daemon: %w", err)
	}

	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
		close(done)
	}()

	return done, nil
}

// WaitOptions configures WaitForDaemon.
type WaitOptions struct {
	// Timeout caps the wait. Defaults to 30s.
	Timeout time.Duration
	// Done, if non-nil, is observed alongside the timer. Closing it ends the
	// wait early with an error indicating the daemon process exited.
	Done <-chan struct{}
}

// WaitForDaemon polls manager state until a healthy daemon appears, opts.Done
// closes (child crashed), or opts.Timeout elapses.
func WaitForDaemon(ctx context.Context, manager *Manager, opts WaitOptions) (*State, error) {
	if manager == nil {
		return nil, errors.New("nil start manager")
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = defaultWaitForDaemonTimeout
	}
	deadline := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline:
			return nil, errors.New("timed out waiting for daemon: the daemon process did not become healthy within 30 seconds; check logs with 'tapes start --logs'")
		default:
		}

		if opts.Done != nil {
			select {
			case <-opts.Done:
				return nil, errors.New("daemon process exited during startup; check logs with 'tapes start --logs'")
			default:
			}
		}

		lock, err := manager.Lock()
		if err != nil {
			return nil, err
		}
		state, err := manager.LoadState()
		_ = lock.Release()
		if err != nil {
			return nil, err
		}
		if state != nil && StateHealthy(ctx, state) {
			return state, nil
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// RegisterAgent locks the manager, appends an AgentSession, and saves.
func RegisterAgent(manager *Manager, name string, pid int) error {
	if manager == nil {
		return errors.New("nil start manager")
	}
	lock, err := manager.Lock()
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release() }()

	state, err := manager.LoadState()
	if err != nil {
		return err
	}
	if state == nil {
		return errors.New("daemon state missing")
	}

	state.Agents = append(state.Agents, AgentSession{
		Name:      name,
		PID:       pid,
		StartedAt: time.Now(),
	})
	return manager.SaveState(state)
}

// UnregisterAgent locks the manager and removes the agent session for pid.
// Missing state is treated as a no-op.
func UnregisterAgent(manager *Manager, pid int) error {
	if manager == nil {
		return errors.New("nil start manager")
	}
	lock, err := manager.Lock()
	if err != nil {
		return err
	}
	defer func() { _ = lock.Release() }()

	state, err := manager.LoadState()
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}

	remaining := make([]AgentSession, 0, len(state.Agents))
	for _, session := range state.Agents {
		if session.PID != pid {
			remaining = append(remaining, session)
		}
	}
	state.Agents = remaining
	return manager.SaveState(state)
}

// DSNMismatchError signals that a healthy daemon is running but bound to a
// different Postgres DSN than the caller asked for. Callers should refuse to
// attach and instruct the user to stop the running daemon.
type DSNMismatchError struct {
	Running   string
	Requested string
}

func (e *DSNMismatchError) Error() string {
	return fmt.Sprintf("running daemon is bound to %q; you asked for %q", e.Running, e.Requested)
}

// LoadHealthyMatching wraps LoadHealthyOrClear and additionally rejects a
// healthy daemon whose recorded PostgresDSN does not match wantDSN. An empty
// wantDSN or an empty recorded DSN (legacy state files) skips the check.
func LoadHealthyMatching(ctx context.Context, manager *Manager, wantDSN string, warnOut io.Writer) (*State, error) {
	state, err := LoadHealthyOrClear(ctx, manager, warnOut)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, nil
	}
	if wantDSN == "" || state.PostgresDSN == "" {
		return state, nil
	}
	if state.PostgresDSN != wantDSN {
		return nil, &DSNMismatchError{Running: state.PostgresDSN, Requested: wantDSN}
	}
	return state, nil
}

// LoadHealthyOrClear acquires the manager lock, loads the daemon state, and
// returns it only if it points at a live, /ping-responsive daemon. If state is
// stale or the JSON is corrupted, it is cleared and the function returns
// (nil, nil) so callers can proceed to spawn a fresh daemon. Corrupted state
// emits a warning via warnOut (when non-nil) instead of returning an error.
func LoadHealthyOrClear(ctx context.Context, manager *Manager, warnOut io.Writer) (*State, error) {
	if manager == nil {
		return nil, errors.New("nil start manager")
	}
	lock, err := manager.Lock()
	if err != nil {
		return nil, fmt.Errorf("locking start manager: %w", err)
	}
	defer func() { _ = lock.Release() }()

	state, loadErr := manager.LoadState()
	if loadErr != nil {
		if warnOut != nil {
			fmt.Fprintf(warnOut, "warning: clearing corrupted daemon state: %v\n", loadErr)
		}
		_ = manager.ClearState()
		return nil, nil
	}

	if StateHealthy(ctx, state) {
		return state, nil
	}

	if state != nil {
		_ = manager.ClearState()
	}
	return nil, nil
}

// SpawnAndWait spawns a daemon and blocks until it becomes healthy. The
// returned channel closes when the spawned process exits and is the same
// channel passed into the underlying WaitForDaemon — callers that want to
// observe daemon liveness after this returns should retain it.
func SpawnAndWait(ctx context.Context, manager *Manager, spawnOpts SpawnOptions, waitOpts WaitOptions) (*State, <-chan struct{}, error) {
	done, err := Spawn(ctx, manager, spawnOpts)
	if err != nil {
		return nil, nil, err
	}
	waitOpts.Done = done
	state, err := WaitForDaemon(ctx, manager, waitOpts)
	if err != nil {
		return nil, done, err
	}
	return state, done, nil
}

// FilterActiveAgents returns the agent sessions whose PIDs are still alive.
// Used by the idle-monitor and by external tools that want to see what
// sessions are using a daemon.
func FilterActiveAgents(state *State) []AgentSession {
	if state == nil {
		return nil
	}
	active := make([]AgentSession, 0, len(state.Agents))
	for _, session := range state.Agents {
		if ProcessAlive(session.PID) {
			active = append(active, session)
		}
	}
	return active
}
