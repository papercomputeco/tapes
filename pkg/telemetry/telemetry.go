// Package telemetry provides anonymous usage tracking for the tapes CLI using
// PostHog. A persistent UUID is stored in ~/.tapes/telemetry.json and all
// events are anonymous by default. Telemetry can be disabled through the
// config.toml telemetry.disabled key, the --disable-telemetry flag, the
// TAPES_TELEMETRY_DISABLED environment variable (all handled by Viper), or
// by running in a detected CI environment.
package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/posthog/posthog-go"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

const (
	stateFileName = "telemetry.json"
)

// State is the persistent telemetry state stored in ~/.tapes/telemetry.json.
type State struct {
	UUID     string `json:"uuid"`
	FirstRun string `json:"first_run"`
}

// Manager manages the persistent telemetry identity stored in the .tapes/ directory.
type Manager struct {
	statePath string
}

// NewManager creates a new telemetry Manager. If configDir is non-empty it is
// used as the .tapes/ directory; otherwise standard dotdir resolution applies.
// When no .tapes/ directory is found, one is created at ~/.tapes/.
func NewManager(configDir string) (*Manager, error) {
	ddm := dotdir.NewManager()
	dir, err := ddm.Target(configDir)
	if err != nil {
		return nil, err
	}

	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolving home dir: %w", err)
		}
		dir = filepath.Join(home, ".tapes")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("creating tapes dir: %w", err)
		}
	}

	return &Manager{
		statePath: filepath.Join(dir, stateFileName),
	}, nil
}

// LoadOrCreate reads the existing telemetry state or creates a new one with a
// fresh UUID and the current time as first_run. Returns the state and a bool
// indicating whether this was a first run (i.e. the file was just created).
func (m *Manager) LoadOrCreate() (*State, bool, error) {
	data, err := os.ReadFile(m.statePath)
	if err == nil {
		state := &State{}
		if err := json.Unmarshal(data, state); err != nil {
			return nil, false, fmt.Errorf("parsing telemetry state: %w", err)
		}
		return state, false, nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return nil, false, fmt.Errorf("reading telemetry state: %w", err)
	}

	state := &State{
		UUID:     uuid.New().String(),
		FirstRun: time.Now().UTC().Format(time.RFC3339),
	}

	data, err = json.MarshalIndent(state, "", "  ")
	if err != nil {
		return nil, false, fmt.Errorf("marshaling telemetry state: %w", err)
	}

	if err := os.WriteFile(m.statePath, data, 0o600); err != nil {
		return nil, false, fmt.Errorf("writing telemetry state: %w", err)
	}

	return state, true, nil
}

// ciEnvVars is the list of environment variables used to detect CI environments.
var ciEnvVars = []string{
	"CI",
	"GITHUB_ACTIONS",
	"GITLAB_CI",
	"CIRCLECI",
	"TRAVIS",
	"JENKINS_URL",
	"BUILDKITE",
	"CODEBUILD_BUILD_ID",
}

// IsCI returns true if the process appears to be running in a CI environment.
func IsCI() bool {
	for _, env := range ciEnvVars {
		if os.Getenv(env) != "" {
			return true
		}
	}
	return false
}

// contextKey is an unexported type for context keys in this package.
type contextKey struct{}

// WithContext returns a copy of ctx with the telemetry Client attached.
func WithContext(ctx context.Context, c *Client) context.Context {
	return context.WithValue(ctx, contextKey{}, c)
}

// FromContext returns the telemetry Client from ctx, or nil if none is set.
func FromContext(ctx context.Context) *Client {
	c, _ := ctx.Value(contextKey{}).(*Client)
	return c
}

// CommonProperties returns properties that are included with every event.
func CommonProperties() posthog.Properties {
	return posthog.NewProperties().
		Set("os", runtime.GOOS).
		Set("arch", runtime.GOARCH)
}
