package backfill

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// SessionStub mirrors the per-session metadata files Claude Code writes
// to ~/.claude/sessions/<pid>.json. The files contain only process-level
// bookkeeping (pid, sessionId, cwd, kind), not conversation data — the
// transcripts themselves live under ~/.claude/projects/<encoded-cwd>/.
type SessionStub struct {
	PID             int    `json:"pid"`
	SessionID       string `json:"sessionId"`
	CWD             string `json:"cwd"`
	StartedAt       int64  `json:"startedAt"`
	Kind            string `json:"kind"`
	Entrypoint      string `json:"entrypoint"`
	BridgeSessionID string `json:"bridgeSessionId"`
	SourceFile      string `json:"-"`
}

// LoadSessionStubs reads every *.json file in dir and returns the stubs that
// successfully decode and carry a non-empty sessionId. A missing directory
// is not an error: callers may run on machines where Claude Code has never
// written a session stub.
func LoadSessionStubs(dir string) ([]SessionStub, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var stubs []SessionStub
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var s SessionStub
		if err := json.Unmarshal(data, &s); err != nil {
			continue
		}
		if s.SessionID == "" {
			continue
		}
		s.SourceFile = path
		stubs = append(stubs, s)
	}
	return stubs, nil
}

// EncodeCWD applies Claude Code's working-directory encoding: every character
// that isn't ASCII alphanumeric becomes a dash. /Users/me/proj therefore maps
// to -Users-me-proj, which is the directory name under ~/.claude/projects/.
func EncodeCWD(cwd string) string {
	var b strings.Builder
	b.Grow(len(cwd))
	for _, r := range cwd {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return b.String()
}
