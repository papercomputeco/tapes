// Package seed seeds demo data by replaying bundled capture corpora
// through the normal ingest write path.
//
// The old demo seed fabricated derived rows (hand-written node chains)
// directly into storage; that data could drift from what real capture
// produces and exercised none of the pipeline. This seed instead
// replays real captured sessions — the gzipped raw_turns JSONL corpora
// also used by the derive regression tests — through an in-process
// ingest server, then runs the deriver. Seeded data is therefore
// indistinguishable from live capture: it lands in raw_turns, creates
// sessions rows via the same ingest transaction, and projects traces/
// spans through the same derive pass.
//
// Re-running the seed is a no-op: raw-turn deduplication (org_id +
// request_id) absorbs replayed wire turns, the transcript dedup key
// includes a content hash, and the derive pass is idempotent.
package seed

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// corpus/ is the single home for the gzipped raw_turns corpora — the same
// files the derive regression gates load (from ../derive via a relative path)
// and the trace-fixture generator replays. They used to be byte-duplicated
// under pkg/derive/testdata; that copy is gone, this is the only one.
//
// The seed embeds an explicit subset: corpus-cb9a87e5 and corpus-9fec0da7 are
// the two demo sessions baked into the binary. corpus-0440f43d also lives in
// this directory (it's a first-class derive/trace-fixture corpus) but is
// deliberately NOT embedded here — it is the "19 sessions, scale" capture, and
// seeding it would ~triple the embedded payload and change the demo/e2e session
// count. Keep this list explicit (not a `corpus/*.jsonl.gz` glob) so co-located
// corpora don't silently join the seed set.
//
//go:embed corpus/corpus-cb9a87e5.jsonl.gz corpus/corpus-9fec0da7.jsonl.gz
var corpusFS embed.FS

// demoProject tags replayed nodes so operators can spot seeded data in
// project-scoped views. It does not participate in node hashes.
const demoProject = "demo"

// nilOrgUUID is the canonical nil-org sentinel; envelopes use "" for
// the same tenant, so the seed normalizes before writing.
const nilOrgUUID = "00000000-0000-0000-0000-000000000000"

// ErrUnsupportedDriver is returned when the storage driver cannot host
// the raw-turn layer or the derive pass (e.g. the in-memory driver).
var ErrUnsupportedDriver = errors.New("demo seeding requires the raw-turn layer (Postgres driver)")

// sessionRederiver is the driver capability the seed's synchronous
// derive step needs; the Postgres driver implements it. The locked
// variant holds the per-session advisory lock across the pass so a
// concurrent derive worker (a clearing seeds with the worker running)
// can't race the seed's derive and prune a just-written turn.
type sessionRederiver interface {
	RederiveSessionLocked(ctx context.Context, project, orgID, harnessID, harnessSessionID string) (*derive.RederiveReport, error)
}

// Result summarizes one seeding run.
type Result struct {
	// Sessions is the number of demo sessions the corpora replay into.
	Sessions int `json:"sessions"`
	// RawTurns is the total number of corpus rows replayed.
	RawTurns int `json:"raw_turns"`
	// RawTurnsInserted counts rows that landed as new raw turns;
	// RawTurnsDeduped counts replays the raw layer's dedup absorbed
	// (a re-seed reports everything deduped).
	RawTurnsInserted int64 `json:"raw_turns_inserted"`
	RawTurnsDeduped  int64 `json:"raw_turns_deduped"`
}

// sessionKey identifies one harness session within the seed org.
type sessionKey struct {
	harnessID        string
	harnessSessionID string
}

// Run seeds the demo corpora into the given driver for orgID (the
// nil-UUID sentinel or "" both mean the default tenant). It replays
// every bundled corpus through an in-process ingest server — the same
// handlers wire capture lands on — and then derives each seeded
// session synchronously so the trace/span projection is queryable the
// moment the call returns (no derive worker required).
func Run(ctx context.Context, driver storage.Driver, logger *slog.Logger, orgID string) (*Result, error) {
	rawStore, ok := driver.(storage.RawTurnStore)
	if !ok {
		return nil, ErrUnsupportedDriver
	}
	rederiver, ok := driver.(sessionRederiver)
	if !ok {
		return nil, ErrUnsupportedDriver
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if orgID == nilOrgUUID {
		orgID = ""
	}

	wire, transcripts, err := loadCorpora()
	if err != nil {
		return nil, err
	}

	report := &Result{RawTurns: len(wire) + len(transcripts)}

	countBefore, err := rawStore.CountRawTurns(ctx)
	if err != nil {
		return nil, fmt.Errorf("count raw turns: %w", err)
	}

	if err := replayThroughIngest(ctx, driver, logger, orgID, wire, transcripts); err != nil {
		return nil, err
	}

	countAfter, err := rawStore.CountRawTurns(ctx)
	if err != nil {
		return nil, fmt.Errorf("count raw turns: %w", err)
	}
	report.RawTurnsInserted = countAfter - countBefore
	report.RawTurnsDeduped = int64(report.RawTurns) - report.RawTurnsInserted

	// Derive synchronously, one session at a time (the bounded unit of
	// work the derive worker uses), so the demo is browsable without a
	// worker running. PutRawTurn also marked these sessions derive-
	// dirty, so a running worker would converge to the same state.
	for _, key := range sessionKeys(wire, transcripts) {
		if _, err := rederiver.RederiveSessionLocked(ctx, demoProject, orgID, key.harnessID, key.harnessSessionID); err != nil {
			return nil, fmt.Errorf("derive seeded session %s/%s: %w", key.harnessID, key.harnessSessionID, err)
		}
		report.Sessions++
	}

	return report, nil
}

// loadCorpora reads every bundled corpus, sorted by name so seeding is
// deterministic, and rewrites row identity for the demo tenant.
func loadCorpora() (wire, transcripts []storage.RawTurnRecord, err error) {
	entries, err := corpusFS.ReadDir("corpus")
	if err != nil {
		return nil, nil, fmt.Errorf("read embedded corpora: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		f, err := corpusFS.Open("corpus/" + name)
		if err != nil {
			return nil, nil, fmt.Errorf("open embedded corpus %s: %w", name, err)
		}
		w, t, err := derive.LoadCorpus(f)
		_ = f.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("load embedded corpus %s: %w", name, err)
		}
		wire = append(wire, w...)
		transcripts = append(transcripts, t...)
	}
	return wire, transcripts, nil
}

// replayThroughIngest stands up an in-process ingest server backed by
// driver and replays the corpus rows against it: wire rows through
// POST /v1/ingest (raw persist + session upsert + node path), then
// transcript rows through POST /v1/ingest/transcript. The server is
// closed — draining its async worker pool — before returning, so the
// caller can derive immediately.
func replayThroughIngest(
	ctx context.Context,
	driver storage.Driver,
	logger *slog.Logger,
	orgID string,
	wire, transcripts []storage.RawTurnRecord,
) error {
	target, stop, err := startIngest(ctx, driver, logger)
	if err != nil {
		return err
	}
	defer stop()

	client := &http.Client{Timeout: 60 * time.Second}

	for i := range wire {
		body, err := wireTurnBody(&wire[i], orgID)
		if err != nil {
			return fmt.Errorf("rebuild wire turn %d: %w", wire[i].ID, err)
		}
		// 422 is acceptable: ingest persists the raw envelope before
		// provider parsing, so an unparseable captured turn (e.g. an
		// upstream 4xx probe) still lands in the raw layer — exactly
		// as it did at live capture time.
		if err := post(ctx, client, target+"/v1/ingest", body, http.StatusAccepted, http.StatusUnprocessableEntity); err != nil {
			return fmt.Errorf("replay wire turn %d: %w", wire[i].ID, err)
		}
	}

	for i := range transcripts {
		body, err := transcriptBody(&transcripts[i], orgID)
		if err != nil {
			return fmt.Errorf("rebuild transcript row %d: %w", transcripts[i].ID, err)
		}
		if err := post(ctx, client, target+"/v1/ingest/transcript", body, http.StatusAccepted); err != nil {
			return fmt.Errorf("replay transcript row %d: %w", transcripts[i].ID, err)
		}
	}

	// Drain the ingest worker pool so every session/node write commits
	// before the derive step reads.
	stop()
	return nil
}

// startIngest binds an in-process ingest server to a random loopback
// port, mirroring the inprocessapi helper. The stop function is
// idempotent and drains the async worker pool.
func startIngest(ctx context.Context, driver storage.Driver, logger *slog.Logger) (string, func(), error) {
	// The ingest server owns an async worker pool whose jobs are
	// deliberately detached from any request context — exactly like the
	// production `tapes serve ingest` process.
	server, err := ingest.New(ingest.Config{Project: demoProject}, driver, logger) //nolint:contextcheck // worker pool jobs are detached by design
	if err != nil {
		return "", nil, fmt.Errorf("creating in-process ingest server: %w", err)
	}

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, fmt.Errorf("binding in-process ingest listener: %w", err)
	}

	go func() {
		_ = server.RunWithListener(listener)
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = server.Close()
		})
	}
	return "http://" + listener.Addr().String(), stop, nil
}

// wireTurnBody reconstructs the ingest TurnPayload one wire corpus row
// was captured from. Every block is the verbatim JSON the raw layer
// preserved; only the session envelope is rewritten for demo identity.
func wireTurnBody(rec *storage.RawTurnRecord, orgID string) ([]byte, error) {
	envelope, err := demoEnvelope(rec.SessionEnvelope, orgID)
	if err != nil {
		return nil, err
	}
	payload := map[string]any{
		"provider": rec.Provider,
		"request":  rec.RawRequest,
		"response": rec.Response,
		"meta":     rec.Meta,
		"session":  envelope,
	}
	if rec.AgentName != "" {
		payload["agent_name"] = rec.AgentName
	}
	return json.Marshal(payload)
}

// transcriptBody reconstructs the ingest TranscriptPayload from a
// transcript corpus row: the records array rides verbatim and the
// subagent attribution comes back out of the stored meta block.
func transcriptBody(rec *storage.RawTurnRecord, orgID string) ([]byte, error) {
	envelope, err := demoEnvelope(rec.SessionEnvelope, orgID)
	if err != nil {
		return nil, err
	}
	var meta struct {
		AgentID     string `json:"agent_id"`
		AgentType   string `json:"agent_type"`
		Description string `json:"description"`
		ToolUseID   string `json:"tool_use_id"`
	}
	if len(rec.Meta) > 0 {
		if err := json.Unmarshal(rec.Meta, &meta); err != nil {
			return nil, fmt.Errorf("decode transcript meta: %w", err)
		}
	}
	payload := map[string]any{
		"session":     envelope,
		"agent_id":    meta.AgentID,
		"agent_type":  meta.AgentType,
		"description": meta.Description,
		"tool_use_id": meta.ToolUseID,
		"records":     rec.RawRequest,
	}
	return json.Marshal(payload)
}

// demoEnvelope rewrites a stored session envelope for the demo tenant:
// org_id points at the seed org and harness_metadata gains a "demo"
// marker so seeded sessions are identifiable wherever they surface.
// Unknown envelope fields pass through untouched.
func demoEnvelope(raw json.RawMessage, orgID string) (json.RawMessage, error) {
	env := map[string]any{}
	if len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, fmt.Errorf("decode session envelope: %w", err)
		}
	}
	env["org_id"] = orgID

	meta := map[string]any{}
	if m, ok := env["harness_metadata"].(map[string]any); ok {
		meta = m
	}
	meta["demo"] = true
	env["harness_metadata"] = meta

	return json.Marshal(env)
}

// post issues one JSON POST, retrying briefly on 502 (the ingest
// worker queue reporting saturation) and treating any status outside
// okStatuses as an error.
func post(ctx context.Context, client *http.Client, url string, body []byte, okStatuses ...int) error {
	const attempts = 5
	var lastErr error
	for attempt := range attempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 200 * time.Millisecond):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if slices.Contains(okStatuses, resp.StatusCode) {
			return nil
		}
		lastErr = fmt.Errorf("ingest returned status %d: %s", resp.StatusCode, bytes.TrimSpace(respBody))
		if resp.StatusCode != http.StatusBadGateway {
			return lastErr
		}
	}
	return lastErr
}

// sessionKeys returns the distinct harness session identities across
// the corpus rows, in first-seen order.
func sessionKeys(rowSets ...[]storage.RawTurnRecord) []sessionKey {
	seen := map[sessionKey]bool{}
	var keys []sessionKey
	for _, rows := range rowSets {
		for _, rec := range rows {
			if rec.HarnessSessionID == "" {
				continue
			}
			key := sessionKey{harnessID: rec.HarnessID, harnessSessionID: rec.HarnessSessionID}
			if seen[key] {
				continue
			}
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys
}
