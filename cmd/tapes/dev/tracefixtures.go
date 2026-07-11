package devcmder

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
)

const traceFixturesLongDesc string = `Render canonical trace-API fixtures from corpus raw layers.

Replays each corpus file (a gzipped JSONL dump of one session's
raw_turns rows) through the real deriver and the real API renderers —
the same code GET /v1/sessions/{id}/traces and the lazy /v1/traces
endpoints execute — and writes the JSON responses as fixture files.
The fixtures are therefore the live contract by construction; the
only divergence from a live response is Postgres JSONB key
normalization inside stored payload blobs, which is not semantic.

Per corpus session <s> (first 8 chars of the harness session id),
written to --out:

  session-traces-<s>.json        composite endpoint, full payloads
  session-traces-<s>.slim.json   composite endpoint, ?payload=preview
  trace-summaries-<s>.json       GET /v1/traces?session_id=
  trace-details-<s>.slim.json    GET /v1/traces/{id}?payload=preview,
                                 one object keyed by trace id

Full per-span payloads for drill-in tests come from the full
composite — GET /v1/traces/{id}/spans/{id} returns the same SpanItem
shape its trace carries there.

The embedded session stanza is folded from the corpus itself: identity
and metadata from the ingest envelopes, title from the deriver's
title fold, token/cost counters and derived_status/tasks/kind_counts
rolled up from the span layer (the v2 direction — every derived rollup
is a deriver output now). The session UUID is minted deterministically
from the harness session id so regeneration is reproducible.

Example:

  tapes dev trace-fixtures \
    --corpus pkg/derive/testdata/corpus-cb9a87e5.jsonl.gz \
    --corpus pkg/derive/testdata/corpus-9fec0da7.jsonl.gz \
    --corpus pkg/derive/testdata/corpus-0440f43d.jsonl.gz \
    --out ../console/src/lib/sessions/__fixtures__`

type traceFixturesCommander struct {
	corpusPaths []string
	outDir      string
}

func newTraceFixturesCmd() *cobra.Command {
	cmder := &traceFixturesCommander{}

	cmd := &cobra.Command{
		Use:   "trace-fixtures",
		Short: "Render trace-API fixtures from corpus raw layers",
		Long:  traceFixturesLongDesc,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.SilenceUsage = true
			for _, path := range cmder.corpusPaths {
				if err := cmder.renderCorpus(cmd, path); err != nil {
					return fmt.Errorf("render %s: %w", path, err)
				}
			}
			return nil
		},
	}

	cmd.Flags().StringArrayVar(&cmder.corpusPaths, "corpus", nil, "corpus file (gzipped JSONL raw_turns dump); repeatable")
	cmd.Flags().StringVar(&cmder.outDir, "out", "", "directory to write fixture files into")
	_ = cmd.MarkFlagRequired("corpus")
	_ = cmd.MarkFlagRequired("out")
	return cmd
}

func (cmder *traceFixturesCommander) renderCorpus(cmd *cobra.Command, path string) error {
	wire, transcriptRows, err := derive.LoadCorpusFile(path)
	if err != nil {
		return err
	}
	if len(wire) == 0 {
		return errors.New("corpus has no wire rows")
	}

	set, err := derive.BuildDerivedSet(wire, "")
	if err != nil {
		return fmt.Errorf("derive: %w", err)
	}
	files := make([]*derive.TranscriptFile, 0, len(transcriptRows))
	for i := range transcriptRows {
		file, err := derive.ParseTranscriptFile(&transcriptRows[i])
		if err != nil {
			return fmt.Errorf("parse transcript row %d: %w", transcriptRows[i].ID, err)
		}
		files = append(files, file)
	}
	derive.ReconcileTranscripts(set, files)
	spanSet := derive.EmitSpans(set)

	key, err := singleSessionKey(spanSet)
	if err != nil {
		return err
	}
	sessionID := fixtureSessionID(key)

	turns, spans, links := recordsFromSpanSet(spanSet, sessionID)
	session := foldSessionItem(key, sessionID, set, wire, transcriptRows, turns)
	session.ModelUsage = modelUsageItems(spanSet.ModelUsage[key])
	// Mirror the deriver's session rollups the handler reads from the
	// session record, so fixtures match a re-derived session. Only set
	// when present, so an empty session keeps the wire's [] / {}.
	if tasks := spanSet.Tasks[key]; len(tasks) > 0 {
		if b, err := json.Marshal(tasks); err == nil {
			session.Tasks = b
		}
	}
	if kc := spanSet.KindCounts[key]; len(kc) > 0 {
		if b, err := json.Marshal(kc); err == nil {
			session.KindCounts = b
		}
	}
	// derived_status is a deriver output now (Phase 1d), so the fixture
	// reflects what a re-derive computes rather than a hard-coded value.
	session.DerivedStatus = spanSet.Status[key].DerivedStatus

	short := key.HarnessSessionID
	if len(short) > 8 {
		short = short[:8]
	}

	full := api.BuildSessionTraces(session, turns, spans, links, api.PayloadFull)
	slim := api.BuildSessionTraces(session, turns, spans, links, api.PayloadPreview)

	summaries := make([]storage.TraceSummaryRecord, 0, len(turns))
	spansByTrace := map[string][]storage.SpanRecord{}
	for _, sp := range spans {
		spansByTrace[sp.TraceID] = append(spansByTrace[sp.TraceID], sp)
	}
	for _, turn := range turns {
		summaries = append(summaries, storage.TraceSummaryRecord{
			SpanTurnRecord: turn,
			SpanCount:      len(spansByTrace[turn.TraceID]),
		})
	}
	traceList := api.BuildTraceList(summaries)

	details := map[string]api.TraceDetail{}
	for _, turn := range turns {
		details[turn.TraceID] = api.BuildTraceDetail(
			turn, spansByTrace[turn.TraceID], linksTouching(links, turn.TraceID), api.PayloadPreview)
	}

	outputs := []struct {
		name string
		v    any
	}{
		{fmt.Sprintf("session-traces-%s.json", short), full},
		{fmt.Sprintf("session-traces-%s.slim.json", short), slim},
		{fmt.Sprintf("trace-summaries-%s.json", short), traceList},
		{fmt.Sprintf("trace-details-%s.slim.json", short), details},
	}
	for _, out := range outputs {
		dst := filepath.Join(cmder.outDir, out.name)
		if err := writeJSONFile(dst, out.v); err != nil {
			return err
		}
		cmd.Printf("wrote %s\n", dst)
	}
	cmd.Printf("session %s: %d traces, %d spans, %d links\n",
		key.HarnessSessionID, len(turns), len(spans), len(links))
	return nil
}

// singleSessionKey asserts the corpus derives to exactly one session —
// corpus dumps are per-session by construction.
func singleSessionKey(spanSet *derive.SpanSet) (derive.SessionKey, error) {
	keys := map[derive.SessionKey]bool{}
	for _, turn := range spanSet.Turns {
		keys[turn.Session] = true
	}
	if len(keys) != 1 {
		return derive.SessionKey{}, fmt.Errorf("corpus derives to %d sessions, want exactly 1", len(keys))
	}
	for key := range keys {
		return key, nil
	}
	return derive.SessionKey{}, errors.New("corpus emitted no traces")
}

// fixtureSessionID mints the session UUID as a pure function of the
// harness identity, so regenerating fixtures never churns ids.
func fixtureSessionID(key derive.SessionKey) string {
	name := "https://tapes.papercompute.com/fixtures/" + key.HarnessID + "/" + key.HarnessSessionID
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(name)).String()
}

// recordsFromSpanSet converts the emitter's output into the storage
// read records the API renders. The field mapping is the in-memory
// twin of writeSpanSet + the postgres read path (pkg/storage/postgres/
// spans.go); ordering matches the read queries' ORDER BY clauses so
// fixtures and live responses agree positionally.
func recordsFromSpanSet(spanSet *derive.SpanSet, sessionID string) ([]storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord) {
	turns := make([]storage.SpanTurnRecord, 0, len(spanSet.Turns))
	spans := make([]storage.SpanRecord, 0, len(spanSet.Turns))
	links := make([]storage.SpanLinkRecord, 0, len(spanSet.Links))

	for _, turn := range spanSet.Turns {
		rec := storage.SpanTurnRecord{
			TraceID:             turn.TraceID,
			SessionID:           sessionID,
			UserPrompt:          turn.UserPrompt,
			ResponsePreview:     turn.ResponsePreview,
			Synthetic:           turn.Synthetic,
			Status:              "ok",
			StartedAt:           turn.StartedAt.UTC(),
			DurationNS:          turn.EndedAt.Sub(turn.StartedAt).Nanoseconds(),
			TotalInputTokens:    turn.TotalInputTokens,
			TotalOutputTokens:   turn.TotalOutputTokens,
			MainInputTokens:     turn.MainInputTokens,
			MainOutputTokens:    turn.MainOutputTokens,
			CacheReadTokens:     turn.CacheReadTokens,
			CacheCreationTokens: turn.CacheCreationTokens,
			TotalCostUSD:        turn.TotalCostUSD,
		}
		if !turn.EndedAt.IsZero() {
			ended := turn.EndedAt.UTC()
			rec.EndedAt = &ended
		}
		turns = append(turns, rec)

		for _, s := range turn.Spans {
			input, err := blocksJSON(s.Input)
			if err != nil {
				panic(fmt.Sprintf("marshal span %s input: %v", s.SpanID, err))
			}
			output, err := blocksJSON(s.Output)
			if err != nil {
				panic(fmt.Sprintf("marshal span %s output: %v", s.SpanID, err))
			}
			var usage json.RawMessage
			if s.Usage != nil {
				usage, err = json.Marshal(s.Usage)
				if err != nil {
					panic(fmt.Sprintf("marshal span %s usage: %v", s.SpanID, err))
				}
			}
			var verdict json.RawMessage
			if s.Verdict != nil {
				verdict, err = json.Marshal(s.Verdict)
				if err != nil {
					panic(fmt.Sprintf("marshal span %s verdict: %v", s.SpanID, err))
				}
			}
			spans = append(spans, storage.SpanRecord{
				TraceID:      turn.TraceID,
				SpanID:       s.SpanID,
				ParentSpanID: s.ParentSpanID,
				Kind:         s.Kind,
				Name:         s.Name,
				Status:       s.Status,
				CallKind:     s.CallKind,
				ThreadID:     s.ThreadID,
				Model:        s.Model,
				StopReason:   s.StopReason,
				StartedAt:    s.StartedAt.UTC(),
				DurationNS:   s.DurationNS,
				Seq:          s.Seq,
				Input:        input,
				Output:       output,
				Usage:        usage,
				RawTurnID:    s.RawTurnID,
				NodeHash:     s.NodeHash,
				Verdict:      verdict,
			})
		}
		for _, l := range turn.Links {
			links = append(links, linkRecord(l))
		}
	}
	for _, l := range spanSet.Links {
		links = append(links, linkRecord(l))
	}

	// Read-path order: turns by (started_at, trace_id), spans by
	// (trace_id, seq).
	sort.SliceStable(turns, func(i, j int) bool {
		if !turns[i].StartedAt.Equal(turns[j].StartedAt) {
			return turns[i].StartedAt.Before(turns[j].StartedAt)
		}
		return turns[i].TraceID < turns[j].TraceID
	})
	sort.SliceStable(spans, func(i, j int) bool {
		if spans[i].TraceID != spans[j].TraceID {
			return spans[i].TraceID < spans[j].TraceID
		}
		return spans[i].Seq < spans[j].Seq
	})
	return turns, spans, links
}

func linkRecord(l *derive.SpanLink) storage.SpanLinkRecord {
	return storage.SpanLinkRecord{
		FromTraceID: l.FromTraceID,
		FromSpanID:  l.FromSpanID,
		FromIO:      l.FromIO,
		ToTraceID:   l.ToTraceID,
		ToSpanID:    l.ToSpanID,
		ToIO:        l.ToIO,
		Kind:        l.Kind,
	}
}

// blocksJSON mirrors the storage layer's contentJSON: empty payloads
// stay null, not [].
func blocksJSON(blocks []llm.ContentBlock) (json.RawMessage, error) {
	if len(blocks) == 0 {
		return nil, nil
	}
	return json.Marshal(blocks)
}

// linksTouching returns the links with either end on the trace — the
// per-trace read query's WHERE clause.
func linksTouching(links []storage.SpanLinkRecord, traceID string) []storage.SpanLinkRecord {
	var out []storage.SpanLinkRecord
	for _, l := range links {
		if l.FromTraceID == traceID || l.ToTraceID == traceID {
			out = append(out, l)
		}
	}
	return out
}

// foldSessionItem assembles the embedded session stanza from the
// corpus: identity and metadata fold from the ingest envelopes
// exactly as ingest applies them (last-write-wins per key), the title
// from the deriver's fold, counters from the span layer.
func foldSessionItem(
	key derive.SessionKey,
	sessionID string,
	set *derive.DerivedSet,
	wire, transcriptRows []storage.RawTurnRecord,
	turns []storage.SpanTurnRecord,
) api.SessionItem {
	item := api.SessionItem{
		ID:               sessionID,
		HarnessID:        key.HarnessID,
		HarnessSessionID: key.HarnessSessionID,
	}

	var first, last time.Time
	for _, rows := range [][]storage.RawTurnRecord{wire, transcriptRows} {
		for _, r := range rows {
			t := r.ReceivedAt.UTC()
			if first.IsZero() || t.Before(first) {
				first = t
			}
			if t.After(last) {
				last = t
			}
		}
	}
	item.StartedAt = first
	item.LastSeenAt = last
	if !last.IsZero() {
		ended := last
		item.EndedAt = &ended
	}

	meta := map[string]any{}
	var envelopeName string
	for _, r := range wire {
		if len(r.SessionEnvelope) == 0 {
			continue
		}
		var env sessions.IngestEnvelope
		if err := json.Unmarshal(r.SessionEnvelope, &env); err != nil {
			continue
		}
		if env.Cwd != "" {
			item.Cwd = env.Cwd
		}
		if env.HarnessVersion != "" {
			item.HarnessVersion = env.HarnessVersion
		}
		if env.Name != "" {
			envelopeName = env.Name
		}
		if len(env.HarnessMetadata) > 0 {
			var m map[string]any
			if err := json.Unmarshal(env.HarnessMetadata, &m); err == nil {
				maps.Copy(meta, m)
			}
		}
	}
	if len(meta) > 0 {
		item.HarnessMetadata = meta
	}

	// Display title: the folded title-gen output, envelope name as
	// fallback — same precedence as the sessions read path.
	if title, ok := set.SessionTitles[key]; ok && title != "" {
		item.Name = title
	} else {
		item.Name = envelopeName
	}

	// Counters roll up from the span layer (the v2 direction; ingest
	// counters retire). turn_count is user-visible turns.
	item.TurnCount = len(turns)
	for _, turn := range turns {
		item.TotalInputTokens += turn.TotalInputTokens
		item.TotalOutputTokens += turn.TotalOutputTokens
		item.TotalCostUsd += turn.TotalCostUSD
	}

	for _, turn := range turns {
		if turn.Synthetic == "" && turn.UserPrompt != "" {
			item.Preview = turn.UserPrompt
			break
		}
	}
	return item
}

// modelUsageItems maps the deriver's per-model breakdown to the API
// shape the session detail carries, so fixtures mirror the live
// response field-for-field.
func modelUsageItems(in []derive.ModelUsage) []api.ModelUsage {
	if len(in) == 0 {
		return nil
	}
	out := make([]api.ModelUsage, len(in))
	for i, mu := range in {
		out[i] = api.ModelUsage{
			Model:        mu.Model,
			Calls:        mu.Calls,
			InputTokens:  mu.InputTokens,
			OutputTokens: mu.OutputTokens,
			CostUsd:      mu.CostUSD,
		}
	}
	return out
}

// writeJSONFile writes indented JSON without HTML escaping — payload
// text is full of <tags>, and fixtures get read by humans in review.
func writeJSONFile(path string, v any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
