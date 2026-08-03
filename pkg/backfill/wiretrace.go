package backfill

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// WireTraceOptions configures a wire-trace → ingest backfill run.
type WireTraceOptions struct {
	// CapturesDir is the paperd wire-trace root holding turn-* bundles
	// (request.json + response.sse + meta.json per turn).
	CapturesDir string

	// IngestURL is the base URL of a tapes-ingest server, e.g.
	// "http://127.0.0.1:8090". The backfill POSTs one envelope per
	// captured turn to {IngestURL}/v1/ingest.
	IngestURL string

	// SessionIDs filters replay to bundles whose captured
	// X-Tapes-Harness-Session-Id matches; empty replays everything.
	SessionIDs []string

	// DryRun parses and reduces every bundle but skips the POST.
	DryRun bool

	// Verbose logs each turn's outcome to Logf.
	Verbose bool

	// Logf receives progress output; defaults to a no-op.
	Logf func(format string, args ...any)
}

// WireTraceResult summarizes a wire-trace backfill run.
type WireTraceResult struct {
	Scanned  int      `json:"scanned"`
	Posted   int      `json:"posted"`
	RawOnly  int      `json:"raw_only"`
	Skipped  int      `json:"skipped"`
	Failed   int      `json:"failed"`
	Failures []string `json:"failures,omitempty"`
}

// wireTraceRequest mirrors the request.json record paperd writes per
// captured turn.
type wireTraceRequest struct {
	Method        string      `json:"method"`
	URL           string      `json:"url"`
	Headers       [][2]string `json:"headers"`
	BodyB64       string      `json:"body_b64"`
	BodyTruncated bool        `json:"body_truncated"`
	Timestamp     string      `json:"ts"`
}

// wireTraceMeta mirrors the meta.json record paperd writes per turn.
type wireTraceMeta struct {
	Status          int     `json:"status"`
	ContentType     string  `json:"content_type"`
	ContentEncoding string  `json:"content_encoding"`
	ResponseBytes   int     `json:"response_bytes"`
	DurationMs      float64 `json:"duration_ms"`
}

// wireTraceEnvelope is the TurnEnvelope JSON POSTed to ingest. It
// mirrors tapes-extproc's envelope shape: raw provider request bytes,
// reduced response, full meta block, optional session block.
type wireTraceEnvelope struct {
	Provider  string                   `json:"provider"`
	AgentName string                   `json:"agent_name,omitempty"`
	Request   json.RawMessage          `json:"request"`
	Response  any                      `json:"response"`
	Meta      wireTraceMetaBlock       `json:"meta"`
	Session   *sessions.IngestEnvelope `json:"session,omitempty"`
}

// wireTraceMetaBlock matches ingest.TurnMeta / extproc TurnMeta wire keys.
type wireTraceMetaBlock struct {
	RequestID       string  `json:"request_id,omitempty"`
	ContentType     string  `json:"content_type,omitempty"`
	ThreadID        string  `json:"thread_id,omitempty"`
	Method          string  `json:"method,omitempty"`
	Path            string  `json:"path,omitempty"`
	Endpoint        string  `json:"endpoint,omitempty"`
	Model           string  `json:"model,omitempty"`
	Stream          string  `json:"stream,omitempty"`
	ContentEncoding string  `json:"content_encoding,omitempty"`
	UpstreamStatus  int     `json:"upstream_status,omitempty"`
	RequestBytes    int     `json:"request_bytes,omitempty"`
	ResponseBytes   int     `json:"response_bytes,omitempty"`
	ElapsedSeconds  float64 `json:"elapsed_seconds,omitempty"`
	BackfillSource  string  `json:"backfill_source,omitempty"`

	// TsRequest preserves the bundle's original capture time so the
	// deriver can order backfilled turns by when they actually
	// happened, not when the backfill ran.
	TsRequest string `json:"ts_request,omitempty"`
}

// WireTrace replays paperd wire-trace capture bundles through a
// tapes-ingest server, reconstructing the envelope tapes-extproc would
// have dispatched live: verbatim request bytes, response reduced with
// the same shared capture reducer, meta rebuilt from the bundle, and
// the session block recovered from the captured X-Tapes-* headers.
//
// The whole flow is idempotent end to end: the raw layer dedupes on
// (org, request_id) — the turn directory name, stable across re-runs —
// and node inserts are content-addressed ON CONFLICT DO NOTHING, so
// turns that already landed via live capture reproduce their existing
// nodes byte for byte and only gain the previously-missing raw row.
//
// Only provider chat-completion calls are replayed (…/v1/messages);
// auxiliary traffic in the trace (count_tokens, tapes API reads) is
// skipped.
func WireTrace(ctx context.Context, opts WireTraceOptions) (*WireTraceResult, error) {
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	entries, err := os.ReadDir(opts.CapturesDir)
	if err != nil {
		return nil, fmt.Errorf("read captures dir: %w", err)
	}
	var turnDirs []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "turn-") {
			turnDirs = append(turnDirs, e.Name())
		}
	}
	// Chronological replay: the dir name embeds a nanosecond timestamp
	// + sequence, so a lexical sort is time order.
	sort.Strings(turnDirs)

	wantSession := map[string]bool{}
	for _, id := range opts.SessionIDs {
		wantSession[id] = true
	}

	reducer := capture.NewAnthropicReducer()
	client := &http.Client{}
	result := &WireTraceResult{}

	for _, turnDir := range turnDirs {
		result.Scanned++
		envelope, skipReason, err := buildWireTraceEnvelope(ctx, opts.CapturesDir, turnDir, reducer)
		if err == nil && envelope != nil && len(wantSession) > 0 &&
			(envelope.Session == nil || !wantSession[envelope.Session.HarnessSessionID]) {
			result.Skipped++
			continue
		}
		if err != nil {
			result.Failed++
			if len(result.Failures) < 25 {
				result.Failures = append(result.Failures, turnDir+": "+err.Error())
			}
			opts.Logf("FAIL %s: %v", turnDir, err)
			continue
		}
		if envelope == nil {
			result.Skipped++
			if opts.Verbose {
				opts.Logf("skip %s: %s", turnDir, skipReason)
			}
			continue
		}
		if opts.DryRun {
			result.Posted++
			if opts.Verbose {
				sid := "(no session envelope)"
				if envelope.Session != nil {
					sid = envelope.Session.HarnessSessionID
				}
				opts.Logf("dry-run %s: would post (session %s)", turnDir, sid)
			}
			continue
		}
		status, err := postWireTraceEnvelope(ctx, client, opts.IngestURL, envelope)
		switch {
		case err != nil && status == http.StatusUnprocessableEntity:
			// The node path rejected the turn (error capture, empty
			// response) but the raw layer persisted it first — the
			// outcome live capture of the same turn would have had.
			result.RawOnly++
			if opts.Verbose {
				opts.Logf("raw-only %s: %v", turnDir, err)
			}
		case err != nil:
			result.Failed++
			if len(result.Failures) < 25 {
				result.Failures = append(result.Failures, turnDir+": "+err.Error())
			}
			opts.Logf("FAIL %s: %v", turnDir, err)
		default:
			result.Posted++
			if opts.Verbose {
				opts.Logf("posted %s", turnDir)
			}
		}
	}
	return result, nil
}

// buildWireTraceEnvelope loads one capture bundle and reconstructs the
// ingest envelope. Returns (nil, reason, nil) for bundles that are not
// provider chat calls.
func buildWireTraceEnvelope(ctx context.Context, dir, turnDir string, reducer capture.Reducer) (*wireTraceEnvelope, string, error) {
	reqPath := filepath.Join(dir, turnDir, "request.json")
	reqRaw, err := os.ReadFile(reqPath)
	if err != nil {
		return nil, "", fmt.Errorf("read request.json: %w", err)
	}
	var req wireTraceRequest
	if err := json.Unmarshal(reqRaw, &req); err != nil {
		return nil, "", fmt.Errorf("decode request.json: %w", err)
	}

	parsedURL, err := url.Parse(req.URL)
	if err != nil {
		return nil, "", fmt.Errorf("parse url %q: %w", req.URL, err)
	}
	if !strings.HasSuffix(parsedURL.Path, "/v1/messages") {
		return nil, "not a /v1/messages call: " + parsedURL.Path, nil
	}
	provider := providerFromPath(parsedURL.Path)
	if provider == "" {
		return nil, "no provider segment in path: " + parsedURL.Path, nil
	}
	if req.BodyTruncated {
		return nil, "", errors.New("request body truncated at capture; raw replay would be lossy")
	}

	body, err := base64.StdEncoding.DecodeString(req.BodyB64)
	if err != nil {
		return nil, "", fmt.Errorf("decode body_b64: %w", err)
	}
	if len(body) == 0 {
		// Bodiless probes (connectivity checks that 404) carry no
		// turn to replay.
		return nil, "empty request body", nil
	}

	var meta wireTraceMeta
	if metaRaw, err := os.ReadFile(filepath.Join(dir, turnDir, "meta.json")); err == nil {
		_ = json.Unmarshal(metaRaw, &meta)
	}

	// A bundle with no response.sse is a call that never completed
	// (client abort, paperd restart). Replay it with an empty response:
	// the raw request still lands, and the node path 422s exactly as a
	// live capture of the same failure would have.
	respBytes, err := os.ReadFile(filepath.Join(dir, turnDir, "response.sse"))
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, "", fmt.Errorf("read response.sse: %w", err)
		}
		respBytes = nil
	}
	if strings.Contains(strings.ToLower(meta.ContentEncoding), "gzip") {
		gz, err := gzip.NewReader(bytes.NewReader(respBytes))
		if err == nil {
			if decoded, err := io.ReadAll(gz); err == nil {
				respBytes = decoded
			}
		}
	}

	// A reduce failure (empty body, upstream error envelope, truncated
	// stream) still produces an envelope: ingest persists the raw turn
	// before parsing, so the bytes survive even when the node path
	// rejects the turn as unprocessable.
	resp, reduceErr := reducer.Reduce(ctx, bytes.NewReader(body), bytes.NewReader(respBytes), meta.ContentType)
	if reduceErr != nil {
		resp = nil
	}

	var reqFields struct {
		Model  string `json:"model"`
		Stream *bool  `json:"stream"`
	}
	_ = json.Unmarshal(body, &reqFields)
	streamLabel := "absent"
	if reqFields.Stream != nil {
		streamLabel = strconv.FormatBool(*reqFields.Stream)
	}

	headers := func(name string) string {
		for _, h := range req.Headers {
			if strings.EqualFold(h[0], name) {
				return h[1]
			}
		}
		return ""
	}

	threadID := threadIDFromHeaders(headers)

	envelope := &wireTraceEnvelope{
		Provider:  provider,
		AgentName: headers("x-tapes-agent-name"),
		Request:   json.RawMessage(body),
		Response:  resp,
		Meta: wireTraceMetaBlock{
			// The turn directory name is unique per captured call and
			// stable across re-runs — exactly the idempotency key the
			// raw layer wants.
			RequestID:       turnDir,
			ContentType:     meta.ContentType,
			ThreadID:        threadID,
			Method:          req.Method,
			Path:            parsedURL.RequestURI(),
			Endpoint:        "messages",
			Model:           reqFields.Model,
			Stream:          streamLabel,
			ContentEncoding: meta.ContentEncoding,
			UpstreamStatus:  meta.Status,
			RequestBytes:    len(body),
			ResponseBytes:   meta.ResponseBytes,
			ElapsedSeconds:  meta.DurationMs / 1000.0,
			BackfillSource:  "paperd-wire-trace",
			TsRequest:       req.Timestamp,
		},
		Session: sessionEnvelopeFromHeaders(headers),
	}
	return envelope, "", nil
}

// threadIDFromHeaders resolves the harness-native sub-thread id for a
// captured turn, or "" for a main-thread call (or a harness with no known
// mapping). It mirrors extproc's harness thread-id mapping (headers.ThreadID
// in tapes-extproc) so backfilled rows attribute threads exactly like live
// capture; any change here belongs in extproc's ThreadID too.
//
// Claude Code is resolved from a header list (first present wins): it stamps
// x-claude-code-agent-id only on calls made from a subagent context, so
// presence alone is the signal.
//
// Codex is a header *pair*, not a list member. Codex stamps thread-id on
// EVERY call — root turns carry thread-id == session-id, and only spawned
// sub-thread (child) turns carry a distinct thread-id. So a sub-thread id is
// resolved only when both headers are present AND they diverge. Both must be
// present: a lone thread-id without its session-id counterpart isn't a
// recognized Codex shape, and treating it as a sub-thread would risk
// misrouting root spines.
//
// The root guard is not cosmetic: a non-empty thread_id on a root turn
// misroutes the root spine into tapes derive's threadCall path and silently
// degrades the session's derived status (terminalMainSpan requires
// ThreadID=="").
func threadIDFromHeaders(header func(string) string) string {
	for _, name := range claudeThreadIDHeaders {
		if v := header(name); v != "" {
			return v
		}
	}
	if tid := header(codexThreadIDHeader); tid != "" {
		if sid := header(codexSessionIDHeader); sid != "" && tid != sid {
			return tid
		}
	}
	return ""
}

// claudeThreadIDHeaders is ordered; first present header wins.
var claudeThreadIDHeaders = []string{
	"x-claude-code-agent-id",
}

const (
	// codexSessionIDHeader is the Codex harness's root session id, present
	// on every Codex call.
	codexSessionIDHeader = "session-id"

	// codexThreadIDHeader is the Codex harness's thread id for this call:
	// equal to session-id on root turns, a distinct id on sub-thread
	// (spawned agent) turns.
	codexThreadIDHeader = "thread-id"
)

// sessionEnvelopeFromHeaders rebuilds the session block from the
// captured X-Tapes-* request headers, mirroring tapes-extproc's
// header→envelope mapping. Returns nil when no envelope headers were
// present (matching extproc's omission semantics).
//
// cwd and session-name are both percent-decoded through
// decodeEnvelopeHeaderValue, which is the same transform extproc
// applies. The two parsers are meant to be interchangeable — the
// shared fixture corpus (fixtures/envelope/cases) is what proves it —
// so any change here belongs in extproc's ParseSessionEnvelope too.
func sessionEnvelopeFromHeaders(header func(string) string) *sessions.IngestEnvelope {
	harnessSessionID := header("x-tapes-harness-session-id")
	harnessID := header("x-tapes-harness-id")
	if harnessSessionID == "" && harnessID == "" {
		return nil
	}
	env := &sessions.IngestEnvelope{
		HarnessID:        harnessID,
		HarnessSessionID: harnessSessionID,
		HarnessVersion:   header("x-tapes-harness-version"),
	}
	if cwd := header("x-tapes-cwd"); cwd != "" {
		env.Cwd = decodeEnvelopeHeaderValue("cwd", cwd)
	}
	if name := header("x-tapes-session-name"); name != "" {
		env.Name = decodeEnvelopeHeaderValue("session-name", name)
	}
	if parent := header("x-tapes-parent-harness-session-id"); parent != "" {
		env.ParentHarnessSessionID = &parent
	}
	if metaB64 := header("x-tapes-harness-metadata"); metaB64 != "" {
		if decoded, err := base64.RawURLEncoding.DecodeString(metaB64); err == nil && json.Valid(decoded) {
			env.HarnessMetadata = decoded
		}
	}
	return env
}

// decodeEnvelopeHeaderValue turns one percent-encoded session-envelope
// header value into the logical value that gets stored.
//
// The contract: the stored envelope value is the *logical* value.
// Percent-encoding is transport framing that dies at the parser, so
// every reader decodes and a non-ASCII path lands as itself rather
// than as the escaped form the header hop demanded. Storing the
// escaped form instead would leak wire encoding into the data model
// and force a decoder into every consumer — the many-parsers
// divergence this contract exists to kill.
//
// PathUnescape, not QueryUnescape: the encoding is RFC 3986
// path-segment escaping, which leaves a literal `+` intact.
// QueryUnescape would silently mistranslate `+` into a space, so a
// session named "go+rust" would land as "go rust".
//
// The producer escapes control bytes so that no second header can be
// smuggled across the header hop. Decoding hands that byte back, so
// the injection defense moves from representation to validation: a
// decoded value carrying any C0 control (< 0x20) or DEL (0x7F) is
// refused outright — logged and stored empty — rather than persisted
// raw. Refusing beats sanitizing because a path that needed a control
// byte to be expressed is not a path any operator meant to record.
//
// A malformed encoding is non-fatal: the raw header value is used so
// the row still records something recognisable. It passes the same
// control-byte gate, so no path through this function can return a
// control byte.
func decodeEnvelopeHeaderValue(field, raw string) string {
	value := raw
	if decoded, err := url.PathUnescape(raw); err == nil {
		value = decoded
	} else {
		slog.Warn("backfill: envelope header percent-decode failed; using raw value",
			"field", field,
			"error", err,
			"raw_len", len(raw),
		)
	}
	if hasControlRune(value) {
		// Log the length, never the value: the whole point is to keep
		// raw control bytes out of anything downstream, and a log sink
		// is downstream.
		slog.Warn("backfill: envelope header contains control bytes after decoding; dropping the value",
			"field", field,
			"decoded_len", len(value),
		)
		return ""
	}
	return value
}

// hasControlRune reports whether s contains a C0 control character
// (< 0x20) or DEL (0x7F). These are the bytes that let a decoded value
// forge header structure if it were ever re-emitted into a
// header-shaped context, and the bytes an operator never intends to
// have in a working directory or a session name.
func hasControlRune(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool {
		return r < 0x20 || r == 0x7F
	})
}

// providerFromPath extracts the provider segment from a gateway path
// like /local-gw/anthropic/v1/messages.
func providerFromPath(path string) string {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	for i, seg := range segments {
		if seg == "v1" && i > 0 {
			switch segments[i-1] {
			case "anthropic", "openai", "ollama":
				return segments[i-1]
			}
		}
	}
	return ""
}

func postWireTraceEnvelope(ctx context.Context, client *http.Client, ingestURL string, envelope *wireTraceEnvelope) (int, error) {
	payload, err := json.Marshal(envelope)
	if err != nil {
		// Error captures (404s, empty bodies) can reduce to a response
		// whose raw fields won't re-marshal. The response was garbage
		// anyway — post without it so the raw request still lands
		// (node path 422s, raw-only).
		envelope.Response = nil
		payload, err = json.Marshal(envelope)
		if err != nil {
			return 0, fmt.Errorf("marshal envelope: %w", err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(ingestURL, "/")+"/v1/ingest", bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return resp.StatusCode, fmt.Errorf("ingest returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp.StatusCode, nil
}
