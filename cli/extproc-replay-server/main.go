// replay-server reads paperd wire-trace bundles and serves the
// captured upstream responses back over HTTP, so extproc can be
// exercised against real upstream streaming bytes inside a cluster
// instead of against synthetic curls.
//
// Synthetic adversarial probes (reducer_adversarial_test.go) cover
// the shapes that can drive the reducer into the empty-Content
// surface — empty body, compressed payload, JSON error envelope,
// post-message_start truncation, out-of-shape chunks. Synthetic
// probes confirm the reducer would fail on any of them in
// isolation; only real captures answer which shape an upstream
// actually emitted on a given turn. This binary reads turn-* capture
// bundles produced by a proxy/extproc build with explicit wire-capture
// support and serves the captured response.sse files back as if it were
// the upstream LLM provider.
//
// # Layout expected
//
//	<captures-dir>/turn-<ns>-<seq>/
//	  request.json     { method, url, headers, body_b64, ... }
//	  response.sse     raw upstream bytes
//	  meta.json        { status, content_type, content_encoding, ... }
//
// # Wire behaviour
//
// On every request, the server picks one bundle and replays:
//
//   - The captured HTTP status from meta.json
//   - The captured Content-Type (and Content-Encoding, if any)
//   - The raw response.sse bytes verbatim
//
// Selection order, first match wins:
//
//  1. X-Replay-Turn: <turn-dir-name> header → that specific bundle
//  2. ?turn=<turn-dir-name> query param   → same as above
//  3. Round-robin over all loaded bundles
//
// # Endpoints
//
//   - GET /_replay/healthz   → 200 with bundle count
//   - GET /_replay/list      → JSON list of every bundle
//   - any other path         → replay one bundle as described
//
// Usage:
//
//	replay-server --captures-dir /path/to/wire-captures --listen :8080
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
)

func main() {
	var (
		capturesDir = flag.String("captures-dir", "", "directory of paperd wire-trace bundles (one turn-*/ subdir per turn)")
		listen      = flag.String("listen", ":8080", "HTTP listen address")
	)
	flag.Parse()

	if *capturesDir == "" {
		slog.Error("--captures-dir is required")
		os.Exit(2)
	}

	bundles, err := loadBundles(*capturesDir)
	if err != nil {
		slog.Error("load captures", "dir", *capturesDir, "error", err)
		os.Exit(1)
	}
	if len(bundles) == 0 {
		slog.Warn("no bundles loaded — every replay will 404 until you drop a turn-*/ directory under --captures-dir", "dir", *capturesDir)
	} else {
		slog.Info("captures loaded", "count", len(bundles), "dir", *capturesDir)
	}

	srv := &Server{bundles: bundles}
	mux := http.NewServeMux()
	mux.Handle("/_replay/healthz", http.HandlerFunc(srv.healthz))
	mux.Handle("/_replay/list", http.HandlerFunc(srv.list))
	mux.Handle("/", http.HandlerFunc(srv.replay))

	slog.Info("listening", "addr", *listen)
	if err := http.ListenAndServe(*listen, mux); err != nil {
		slog.Error("server exited", "error", err)
		os.Exit(1)
	}
}

// Server holds the loaded bundles and the round-robin cursor. Bundles
// are immutable after construction; cursor is the only mutable state.
type Server struct {
	bundles []*Bundle
	cursor  atomic.Uint64
}

// Bundle is one paperd wire-trace turn, loaded into memory at startup.
// We keep the response bytes resident because they're small (typical
// Anthropic streaming response is ~5-50 KiB) and serving from disk on
// every request adds latency that distorts whatever timing-sensitive
// behavior the reducer / dispatcher have.
type Bundle struct {
	// Name is the directory name (turn-<ns>-<seq>) — the stable handle
	// for operator-directed selection.
	Name string `json:"name"`
	// Meta is the parsed meta.json. Status + ContentType + ContentEncoding
	// are the fields the replay actually uses; the rest is included so
	// the /_replay/list endpoint is useful for operators picking a turn.
	Meta MetaRecord `json:"meta"`
	// Response is the raw response.sse bytes. Served verbatim.
	Response []byte `json:"-"`
}

// MetaRecord mirrors the fields paperd writes; only what we need to
// replay correctly is declared. Extra fields in the file are ignored.
type MetaRecord struct {
	Status          int             `json:"status"`
	ContentType     *string         `json:"content_type,omitempty"`
	ContentEncoding *string         `json:"content_encoding,omitempty"`
	ResponseBytes   int64           `json:"response_bytes"`
	DurationMs      uint64          `json:"duration_ms"`
	FinalizedBy     string          `json:"finalized_by"`
	ResponseHeaders [][]string      `json:"response_headers,omitempty"`
	Extra           json.RawMessage `json:"-"`
}

// loadBundles walks the captures directory non-recursively and parses
// every subdirectory that looks like a wire-trace turn. Subdirs missing
// either meta.json or response.sse are skipped with a warn — partial
// bundles are common when paperd was killed mid-stream and we don't
// want one bad turn to take down the whole replay server.
func loadBundles(dir string) ([]*Bundle, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir: %w", err)
	}
	var bundles []*Bundle
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		full := filepath.Join(dir, name)
		b, err := loadBundle(full)
		if err != nil {
			slog.Warn("skipping bundle", "dir", name, "error", err)
			continue
		}
		b.Name = name
		bundles = append(bundles, b)
	}
	sort.Slice(bundles, func(i, j int) bool { return bundles[i].Name < bundles[j].Name })
	return bundles, nil
}

func loadBundle(dir string) (*Bundle, error) {
	metaPath := filepath.Join(dir, "meta.json")
	respPath := filepath.Join(dir, "response.sse")

	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("read meta.json: %w", err)
	}
	var meta MetaRecord
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, fmt.Errorf("parse meta.json: %w", err)
	}
	if meta.Status == 0 {
		return nil, errors.New("meta.json: status missing or zero")
	}

	respBytes, err := os.ReadFile(respPath)
	if err != nil {
		// Empty body is a real signal (hypothesis A — empty upstream
		// body). Distinguish file-missing from file-present-but-empty:
		// missing is a malformed bundle, empty is a legitimate capture.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("response.sse missing")
		}
		return nil, fmt.Errorf("read response.sse: %w", err)
	}
	return &Bundle{Meta: meta, Response: respBytes}, nil
}

// pick returns the bundle to replay for this request. Operators can
// target a specific turn via the X-Replay-Turn header or ?turn= query
// param; otherwise the cursor advances round-robin.
func (s *Server) pick(r *http.Request) (*Bundle, error) {
	if len(s.bundles) == 0 {
		return nil, errors.New("no bundles loaded")
	}
	if turn := r.Header.Get("X-Replay-Turn"); turn != "" {
		if b := s.byName(turn); b != nil {
			return b, nil
		}
		return nil, fmt.Errorf("turn %q not found", turn)
	}
	if turn := r.URL.Query().Get("turn"); turn != "" {
		if b := s.byName(turn); b != nil {
			return b, nil
		}
		return nil, fmt.Errorf("turn %q not found", turn)
	}
	// Atomic round-robin: cursor.Add returns the post-increment value,
	// so mod len(bundles) lands on the right index without any locking.
	i := (s.cursor.Add(1) - 1) % uint64(len(s.bundles))
	return s.bundles[i], nil
}

func (s *Server) byName(name string) *Bundle {
	for _, b := range s.bundles {
		if b.Name == name {
			return b
		}
	}
	return nil
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	bundle, err := s.pick(r)
	if err != nil {
		slog.Warn("replay: no bundle available", "path", r.URL.Path, "error", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	if bundle.Meta.ContentType != nil {
		w.Header().Set("Content-Type", *bundle.Meta.ContentType)
	}
	if bundle.Meta.ContentEncoding != nil {
		w.Header().Set("Content-Encoding", *bundle.Meta.ContentEncoding)
	}
	// Hand back the captured turn name so test harnesses can correlate
	// the replay response with the source bundle.
	w.Header().Set("X-Replay-Turn", bundle.Name)
	w.WriteHeader(bundle.Meta.Status)
	// Write+Flush per slice so reverse proxies / extproc see the bytes
	// in one shot. We don't try to recreate the original timing —
	// chunk boundaries from real traffic aren't preserved in the
	// captured response.sse file anyway (paperd appends as chunks
	// arrive but readers see the concatenated bytes). For diagnostic
	// purposes the content is what matters, not the chunking.
	if _, err := w.Write(bundle.Response); err != nil {
		slog.Warn("replay: write failed", "turn", bundle.Name, "error", err)
		return
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	slog.Info("replay", "turn", bundle.Name, "status", bundle.Meta.Status, "bytes", len(bundle.Response), "client_path", r.URL.Path)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"bundles": len(s.bundles),
	})
}

func (s *Server) list(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	type Entry struct {
		Name            string  `json:"name"`
		Status          int     `json:"status"`
		ContentType     *string `json:"content_type,omitempty"`
		ContentEncoding *string `json:"content_encoding,omitempty"`
		ResponseBytes   int64   `json:"response_bytes"`
		FinalizedBy     string  `json:"finalized_by"`
		Preview         string  `json:"preview"`
	}
	entries := make([]Entry, 0, len(s.bundles))
	for _, b := range s.bundles {
		entries = append(entries, Entry{
			Name:            b.Name,
			Status:          b.Meta.Status,
			ContentType:     b.Meta.ContentType,
			ContentEncoding: b.Meta.ContentEncoding,
			ResponseBytes:   int64(len(b.Response)),
			FinalizedBy:     b.Meta.FinalizedBy,
			Preview:         preview(b.Response, 160),
		})
	}
	_ = json.NewEncoder(w).Encode(entries)
}

// preview matches respBodyPreview in tapes-extproc/processor.go so the
// list output looks the same shape an operator sees in extproc logs.
// Non-printable bytes are hex-escaped; longer bodies are truncated.
func preview(b []byte, max int) string {
	if len(b) == 0 {
		return "<empty>"
	}
	n := len(b)
	if n > max {
		n = max
	}
	var sb strings.Builder
	sb.Grow(n + 16)
	for i := 0; i < n; i++ {
		c := b[i]
		switch {
		case c == '\\':
			sb.WriteString(`\\`)
		case c == '"':
			sb.WriteString(`\"`)
		case c >= 0x20 && c < 0x7f:
			sb.WriteByte(c)
		case c == '\n':
			sb.WriteString(`\n`)
		case c == '\r':
			sb.WriteString(`\r`)
		case c == '\t':
			sb.WriteString(`\t`)
		default:
			fmt.Fprintf(&sb, `\x%02x`, c)
		}
	}
	if len(b) > max {
		fmt.Fprintf(&sb, "...(%dB total)", len(b))
	}
	return sb.String()
}
