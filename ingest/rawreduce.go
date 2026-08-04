package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// This file holds the server-side reduction of stored capture bytes, factored
// out of the ingest handler so it has exactly one implementation.
//
// The reason it is a package-level function rather than a Server method is the
// raw-response ratchet (off → dual → raw). Moving an environment to raw is
// only safe if re-reducing the stored bytes reproduces the reduction the
// adapter computed live, and that has to be PROVEN over real traffic before
// the flip. The proving tool (`tapes raw equivalence`) therefore needs to run
// the reduction mode=raw would run — and if it ran its own copy, the proof
// would be of the copy rather than of the code that will actually execute.
// One function, two callers, is what makes the proof mean anything.
//
// Everything here is a pure function of the stored row: no clock (beyond what
// the reducers themselves stamp), no database, no metrics, no logging. The
// observability the ingest path needs is returned as data — which stamp source
// fired, which stamps were malformed — and emitted by the caller, so the
// offline tool can report the same facts without emitting server metrics.

// rawReducers dispatches verbatim upstream bytes to the reducer for a
// provider. Constructed explicitly and dispatched by provider name, the same
// way proxy.New does it — pkg/capture deliberately keeps no global registry so
// import order and init() stay out of the call graph.
//
// A provider with no entry simply never gets a server-side reduction; its
// adapter is expected to send one. That is a real constraint on the ratchet
// rather than an oversight: ollama is a supported provider with no reducer, so
// ollama traffic cannot move to raw until one exists.
var rawReducers = map[string]capture.Reducer{
	capture.ProviderAnthropic: capture.NewAnthropicReducer(),
	capture.ProviderOpenAI:    capture.NewOpenAIResponsesReducer(),
}

// ReducerForProvider returns the server-side reducer for a provider name, and
// whether one exists.
func ReducerForProvider(provider string) (capture.Reducer, bool) {
	r, ok := rawReducers[provider]
	return r, ok
}

// Sentinel causes for a reduction that did not produce a response. They are
// distinguished because they mean different things for the ratchet: a decode
// failure says the bytes are unreadable by this build, a missing reducer says
// the provider was never wired up, and a reducer failure says the bytes were
// readable but did not parse into a turn. Only the first is plausibly a
// transport or encoding bug; the other two are gaps.
var (
	// ErrNoReducer means no server-side reducer is registered for the
	// provider, so mode=raw would store the turn with no reduction at all.
	ErrNoReducer = errors.New("no server-side reducer for provider")

	// ErrDecode means the stored bytes could not be decoded under their
	// recorded content-encoding.
	ErrDecode = errors.New("decode stored raw response")

	// ErrReduce means the reducer rejected the decoded bytes.
	ErrReduce = errors.New("reduce stored raw response")
)

// StoredRawTurn is the subset of a captured turn that a server-side reduction
// consumes. It is deliberately not TurnPayload: the reduction reads only these
// fields, and naming them makes it explicit that the stored row carries
// everything the reduction needs — which is the property the raw lane exists
// to guarantee.
type StoredRawTurn struct {
	// Provider keys the reducer table.
	Provider string

	// RawRequest is the original provider request body. Both current
	// reducers discard it; it is passed through because the Reducer contract
	// admits enriching a response from request context, and a reducer that
	// started doing so should see the same input on both paths.
	RawRequest json.RawMessage

	// RawResponse is the upstream response body exactly as it arrived,
	// still under RawResponseEncoding.
	RawResponse []byte

	// RawResponseEncoding is the Content-Encoding the bytes are stored
	// under. Empty means identity.
	RawResponseEncoding string

	// Meta is the capture adapter's metadata block. ContentType selects the
	// streaming vs one-shot reduction path; the timestamp and elapsed fields
	// supply the capture-side stamps the bytes cannot carry.
	Meta TurnMeta
}

// MalformedStamp records a capture-side timestamp that was present but not
// parseable. Someone meant to send it, so it is reported rather than ignored;
// it never rejects the turn, because losing a whole turn over a timestamp is a
// far worse trade than dating it imprecisely.
type MalformedStamp struct {
	Field string
	Value string
	Err   error
}

// RawReduction is the outcome of reducing one stored turn's bytes.
type RawReduction struct {
	// Response is the reduced turn. Non-nil whenever the error is nil.
	Response *llm.ChatResponse

	// Truncated reports that the stored body ended early and was salvaged.
	// The reduction went ahead — a turn recovered from a stream that ended
	// early is worth more than no turn — but it may be missing its tail, and
	// nothing downstream can tell that from the row alone.
	Truncated bool

	// DurationSource and CreatedAtSource name the meta field that supplied
	// each capture-side stamp, or StampSourceFallback when the envelope
	// carried nothing usable.
	DurationSource  StampSource
	CreatedAtSource StampSource

	// MalformedStamps lists capture-side timestamps that were present but
	// unparseable.
	MalformedStamps []MalformedStamp
}

// ReduceStoredRawTurn turns one stored turn's verbatim bytes into the reduced
// response mode=raw would persist for it.
//
// This is the whole server-side reduction: decode the bytes under their
// recorded encoding, reduce them with the shared pkg/capture reducer for the
// provider, then restore the two capture-side facts the bytes do not carry.
// Callers that need the ingest-path behaviour get it by emitting metrics and
// logs from the returned outcome; callers proving equivalence offline simply
// read the same outcome.
func ReduceStoredRawTurn(ctx context.Context, in StoredRawTurn) (RawReduction, error) {
	reducer, ok := rawReducers[in.Provider]
	if !ok {
		return RawReduction{}, fmt.Errorf("%w: %q", ErrNoReducer, in.Provider)
	}

	body, stats, err := capture.DecodeContentEncoding(in.RawResponse, in.RawResponseEncoding)
	if err != nil {
		return RawReduction{}, fmt.Errorf("%w (encoding %q): %w", ErrDecode, in.RawResponseEncoding, err)
	}

	resp, err := reducer.Reduce(ctx,
		bytes.NewReader(in.RawRequest),
		bytes.NewReader(body),
		in.Meta.ContentType,
	)
	if err != nil {
		return RawReduction{}, fmt.Errorf("%w (content-type %q): %w", ErrReduce, in.Meta.ContentType, err)
	}
	if resp == nil {
		return RawReduction{}, fmt.Errorf("%w: reducer returned no response", ErrReduce)
	}

	out := RawReduction{Response: resp, Truncated: stats.Truncated}
	out.DurationSource = stampDuration(resp, in.Meta)
	out.CreatedAtSource, out.MalformedStamps = stampCaptureTime(resp, in.Meta)
	return out, nil
}

// ReducedResponseAbsent reports whether a payload carried no reduced response.
//
// It has to be decided on the parsed value, not on the envelope JSON: Response
// is a struct rather than a pointer and has no omitempty, so a client that
// marshals TurnPayload always emits a `response` key. Its presence in the
// bytes therefore says nothing about whether an adapter actually reduced
// anything — the zero value and a deliberate empty reduction are the same JSON.
//
// Every field is checked rather than just the message, so an adapter that
// reduced a turn to an error envelope (stop_reason and usage, no content)
// still counts as having reduced, and keeps its result.
//
// Exported because it is the definition of "this row is raw-only". Ingest uses
// it to decide whether to reduce server-side; the equivalence prover uses it to
// decide whether a stored row has an adapter reduction to compare against. Two
// spellings of that predicate would let the prover measure a different
// population than the one the flip affects.
func ReducedResponseAbsent(r llm.ChatResponse) bool {
	return r.Model == "" &&
		r.Message.Role == "" &&
		len(r.Message.Content) == 0 &&
		!r.Done &&
		r.StopReason == "" &&
		r.Usage == nil
}

// maxElapsedSeconds bounds a plausible single-call duration. Beyond it the
// value is treated as corrupt rather than stamped into timing data: a
// week-long LLM call is not a call, it is a producer clock bug.
const maxElapsedSeconds = 7 * 24 * 60 * 60

// usableElapsed reports whether the meta elapsed value can safely be turned
// into a duration or a timestamp offset. NaN and ±Inf survive JSON-adjacent
// producers more often than one would hope, and either would poison int64
// conversion silently.
func usableElapsed(elapsed float64) bool {
	return !math.IsNaN(elapsed) && !math.IsInf(elapsed, 0) &&
		elapsed > 0 && elapsed <= maxElapsedSeconds
}

// stampDuration sets Usage.TotalDurationNs from the capture adapter's
// meta.elapsed_seconds, allocating Usage if needed, and reports which source
// supplied the value.
//
// This is the raw-only counterpart of proxy.stampDuration (PCC-514/570):
// Anthropic and OpenAI do not surface call duration on the wire, so a
// reduction performed from stored bytes has no duration in it, and the column
// lands NULL — the exact regression those issues fixed on the proxy path. The
// value survives the raw-only crossing on meta.elapsed_seconds, so it is
// re-stamped here.
//
// Overwriting rather than filling-if-empty is deliberate, and matches the
// proxy: a provider-reported internal duration (Ollama) measures something
// different from wall-clock time at the capture point, and aggregate stats are
// only comparable if every turn's duration means the same thing.
//
// An absent elapsed_seconds leaves the reduction alone. There is no second
// source to fall back on — ingest's own clock measures the dispatch hop, not
// the call — so the honest outcome is an unstamped duration, reported as a
// fallback so it is visible rather than silent.
func stampDuration(resp *llm.ChatResponse, meta TurnMeta) StampSource {
	if resp == nil {
		return StampSourceFallback
	}
	if !usableElapsed(meta.ElapsedSeconds) {
		return StampSourceFallback
	}
	if resp.Usage == nil {
		resp.Usage = &llm.Usage{}
	}
	resp.Usage.TotalDurationNs = int64(meta.ElapsedSeconds * float64(time.Second))
	return StampSourceElapsed
}

// stampCaptureTime sets CreatedAt to the capture-side instant the envelope
// reports, so a raw-only row means the same thing a pre-reduced one does.
//
// The contract this enforces: CreatedAt is when the turn happened, never when
// tapes heard about it. Under dual-send the producer reduced live and stamped
// its own clock, so CreatedAt was capture time by construction. Under raw-only
// the reduction moves to the server, and the reducers stamp time.Now()
// (pkg/capture/anthropic.go, anthropic_state.go) — which is now ingest time.
// Same field, silently different quantity: rows would sort and bucket by when
// the ingest hop happened, and a replay of stored bytes would date every turn
// to the replay.
//
// Sources, most precise first. Each is a capture-side clock; none is ingest's:
//
//  1. meta.captured_at — the completion instant outright.
//  2. meta.ts_request + meta.elapsed_seconds — request instant plus the
//     call's duration, which is the same quantity by construction.
//  3. meta.ts_request alone — the request instant, early by the call's
//     duration but a real capture-side time, and already what
//     derive.CapturedAt uses for the row's chronology.
//
// Preferring ts_request over ingest's clock is what keeps CreatedAt and the
// derived span's StartedAt (derive.CapturedAt, same field) from disagreeing
// about when one turn happened. It also means backfilled rows, which carry
// ts_request today, get a correct CreatedAt without any producer change.
//
// With none of them present the reducer's own value stands and a fallback is
// reported. That fallback is not uniform, which is why it is reported rather
// than assumed:
//
//   - OpenAI Responses reductions carry the upstream's own created_at
//     (pkg/capture/openai_responses.go), so CreatedAt is already a real
//     capture-side time and overwriting it would lose information.
//   - Anthropic reductions carry time.Now(), so CreatedAt is reduction time —
//     the drift this function exists to close, left visible on the counter
//     until producers send one of the fields above.
//
// Guessing capture time as now-minus-elapsed is deliberately not done: it is
// indistinguishable from the truth on a healthy dispatch and arbitrarily wrong
// on a retried, buffered, or replayed one, which is the case that matters.
func stampCaptureTime(resp *llm.ChatResponse, meta TurnMeta) (StampSource, []MalformedStamp) {
	if resp == nil {
		return StampSourceFallback, nil
	}

	var malformed []MalformedStamp

	completed, ok, err := parseCaptureStamp(meta.CapturedAt)
	if err != nil {
		malformed = append(malformed, MalformedStamp{Field: "captured_at", Value: meta.CapturedAt, Err: err})
	}
	if ok {
		resp.CreatedAt = completed
		return StampSourceCapturedAt, malformed
	}

	requested, ok, err := parseCaptureStamp(meta.TsRequest)
	if err != nil {
		malformed = append(malformed, MalformedStamp{Field: "ts_request", Value: meta.TsRequest, Err: err})
	}
	if ok {
		// The elapsed offset is what turns a request instant into the
		// completion instant CreatedAt denotes. Without it the request
		// instant still stands — early by the call's duration, which is a
		// far smaller error than the ingest hop it replaces.
		if usableElapsed(meta.ElapsedSeconds) {
			requested = requested.Add(time.Duration(meta.ElapsedSeconds * float64(time.Second)))
		}
		resp.CreatedAt = requested
		return StampSourceTsRequest, malformed
	}

	return StampSourceFallback, malformed
}

// parseCaptureStamp parses one RFC 3339 capture-side timestamp off the meta
// block, reporting whether it yielded a usable instant and, separately,
// whether a present value failed to parse.
//
// RFC3339Nano matches derive.CapturedAt, so the two agree on what they accept
// — a timestamp the deriver honors cannot be one ingest rejects.
//
// An absent field is ordinary: no producer sends either of these yet. A
// present-but-malformed one is a producer bug and is reported, since someone
// meant to send it.
func parseCaptureStamp(value string) (time.Time, bool, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return time.Time{}, false, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false, err
	}
	return ts.UTC(), true, nil
}
