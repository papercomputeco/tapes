package extproc

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// Dispatcher owns the "turn → tapes-ingest" path. Kept separate from the
// processor state machine so retry/backoff/marshal-error handling is one
// reviewable diff and the silent "_" on json.Marshal that kept the 4-week
// bug invisible cannot be reintroduced without also breaking its test.
type Dispatcher struct {
	ingestURL  string
	httpClient *http.Client
	sem        chan struct{}

	// observer receives every terminal outcome (accept / drop / retry). Tests
	// inject an in-memory observer; production leaves it nil and relies on
	// the metrics set from C13.
	observer Observer
}

// Observer hooks every terminal outcome. nil-safe.
type Observer interface {
	OnAccepted(provider string, requestID string)
	OnDrop(provider string, reason DropReason, requestID string)
}

// ContextObserver is an optional extension for observers that need safe
// per-turn context. The base Observer stays small for existing tests; metrics
// implements this richer interface to emit low-cardinality terminal outcomes.
type ContextObserver interface {
	OnAcceptedContext(provider string, requestID string, ctx OutcomeContext)
	OnDropContext(provider string, reason DropReason, requestID string, ctx OutcomeContext)
}

// DispatchObserver is an optional extension for dispatch-stage metrics.
type DispatchObserver interface {
	OnDispatchLatency(provider string, requestID string, seconds float64)
	OnInflight(n int)
}

// RawLaneObserver is an optional extension covering the verbatim-bytes lane.
// Every outcome is non-fatal to the turn, so none of them belong in
// DropReason — but each one silently changes the fidelity a row lands with,
// which makes them exactly the things an operator needs counted rather than
// inferred.
type RawLaneObserver interface {
	// OnRawResponseAttached fires when verbatim bytes go on the wire.
	// shape is "dual" or "raw_only".
	OnRawResponseAttached(provider, shape string)
	// OnRawResponseSkipped fires when the mode wanted bytes but they were
	// withheld. The row lands fidelity:reduced.
	OnRawResponseSkipped(provider, reason string)
	// OnRawResponseFallback fires when a raw-only turn kept its reduction
	// because ingest could not have produced one.
	OnRawResponseFallback(provider, reason string)
}

// OutcomeContext carries safe request/response metadata for logs and metrics.
// Raw high-cardinality values are used only in structured logs; metrics fold
// them through bounded label normalizers in Metrics.
type OutcomeContext struct {
	Method string
	Path   string
	// ThreadID is the harness sub-thread identifier (e.g. a Claude Code
	// subagent agent-id), "" for main-thread calls. It is high-cardinality
	// and surfaced ONLY in structured logs via logAttrs — never folded into
	// a metric label set. Metrics read the bounded fields below directly
	// through normalizers (see Metrics.ObserveTerminal), so this field is
	// invisible to the metrics path.
	ThreadID            string
	Endpoint            string
	Model               string
	ModelFamily         string
	Stream              string
	ContentType         string
	ContentEncoding     string
	UpstreamStatus      int
	UpstreamStatusClass string
	RequestBytes        int
	ResponseBytes       int
	ElapsedSeconds      float64
}

func (c OutcomeContext) logAttrs() []any {
	attrs := []any{
		"endpoint", c.Endpoint,
		"stream", c.Stream,
		"model_family", c.ModelFamily,
		"upstream_status_class", c.UpstreamStatusClass,
		"req_bytes", c.RequestBytes,
		"resp_bytes", c.ResponseBytes,
	}
	if c.Method != "" {
		attrs = append(attrs, "method", c.Method)
	}
	if c.Path != "" {
		attrs = append(attrs, "path", c.Path)
	}
	if c.Model != "" {
		attrs = append(attrs, "model", c.Model)
	}
	if c.ThreadID != "" {
		attrs = append(attrs, "thread_id", c.ThreadID)
	}
	if c.ContentType != "" {
		attrs = append(attrs, "content_type", c.ContentType)
	}
	if c.ContentEncoding != "" {
		attrs = append(attrs, "content_encoding", c.ContentEncoding)
	}
	if c.UpstreamStatus > 0 {
		attrs = append(attrs, "upstream_status", c.UpstreamStatus)
	}
	if c.ElapsedSeconds > 0 {
		attrs = append(attrs, "elapsed_ms", int(c.ElapsedSeconds*1000))
	}
	return attrs
}

// DropReason is a closed enum of why a turn failed to land in tapes. Keeping
// it a named type (not a string literal) forces new call sites to add a
// constant and — by extension — a metric label row. Dashboards and alerts
// then stay stable against typos.
//
// The enum has two halves, and which half a reason is in is a decision rather
// than a grouping — see fixtures/drop-reason/, where both halves are specified
// and the line between them is argued.
type DropReason string

// The capture-policy half. These say what makes a turn capturable at all, so
// they belong to every implementation of tapes capture and not to this one:
// two capture paths that disagree about any of them record different sessions
// from identical traffic.
//
// The strings are therefore NOT declared here. They are conversions of the
// specified vocabulary in pkg/capture, so this adapter reads the contract
// instead of restating it — restating it is how the last capture contract
// drifted while both copies stayed green. Behaviour and metric labels are
// unchanged: these are the same strings they have always been, sourced from
// the place that now owns them.
const (
	DropUpstreamStatus  = DropReason(capture.DropUpstreamStatus)
	DropNonTurnRequest  = DropReason(capture.DropNonTurnRequest)
	DropRequestDecode   = DropReason(capture.DropRequestDecode)
	DropEmptyResponse   = DropReason(capture.DropEmptyResponse)
	DropUnknownProvider = DropReason(capture.DropUnknownProvider)
	DropResponseDecode  = DropReason(capture.DropResponseDecode)
	DropReducerError    = DropReason(capture.DropReducerError)
)

// The transport and runtime half. These are correctly this adapter's own: each
// one names a way THIS deployment can fail to move bytes, and an implementation
// without a dispatch queue, a downstream client connection or a remote ingest
// endpoint cannot produce them. Promoting them to the shared vocabulary would
// specify one deployment's plumbing as everyone's contract.
//
// They stay declared here for exactly that reason, and the corpus specifies
// them as non-contract so that "not shared" is recorded rather than assumed.
const (
	DropMarshalError     DropReason = "marshal_error"
	DropIngestReject     DropReason = "ingest_reject"
	DropIngestTimeout    DropReason = "ingest_timeout"
	DropSemFull          DropReason = "sem_full"
	DropClientDisconnect DropReason = "client_disconnect"
	// DropUpstreamNoResponse: the stream was torn down after the request
	// completed and before any response byte arrived. Distinct from
	// DropEmptyResponse, which is a response phase that completed normally
	// carrying nothing: that is a property of the exchange, this is a
	// property of the connection.
	DropUpstreamNoResponse DropReason = "upstream_no_response"
	// DropMissingStatus: the response phase ended without Envoy ever
	// sending :status, which violates the ext_proc message contract. It is
	// a transport reason despite looking like a policy one — no
	// implementation that reads a status directly off a response can
	// reach it.
	DropMissingStatus DropReason = "missing_status"
	// DropRequestOverBudget: request accumulation stopped at
	// requestCaptureBudget — a request that large can never land at ingest,
	// so the turn is shed before any marshal or POST. Forwarding is untouched.
	DropRequestOverBudget DropReason = "request_over_budget"
)

// AllDropReasons enumerates every constant above so metric wiring can
// preallocate label rows and the metric-enumeration test can assert
// completeness.
func AllDropReasons() []DropReason {
	return []DropReason{
		DropMarshalError,
		DropIngestReject,
		DropIngestTimeout,
		DropSemFull,
		DropReducerError,
		DropRequestDecode,
		DropResponseDecode,
		DropUnknownProvider,
		DropClientDisconnect,
		DropUpstreamNoResponse,
		DropUpstreamStatus,
		DropMissingStatus,
		DropNonTurnRequest,
		DropEmptyResponse,
		DropRequestOverBudget,
	}
}

// TurnEnvelope is the JSON body posted to tapes-ingest. Request stays raw
// provider JSON while Response is the already-reduced canonical LLM response.
type TurnEnvelope struct {
	Provider  string            `json:"provider"`
	AgentName string            `json:"agent_name,omitempty"`
	Request   json.RawMessage   `json:"request"`
	Response  *llm.ChatResponse `json:"response"`

	// RawResponse is the upstream response body exactly as it arrived on
	// the wire — still under whatever Content-Encoding the upstream used.
	// encoding/json renders it as standard padded base64.
	//
	// It is NOT the decoded bytes: tapes stores the column byte-faithfully
	// and decodes only to reduce, so decompressing here would make the
	// stored bytes something the upstream never sent.
	RawResponse []byte `json:"raw_response,omitempty"`

	// RawResponseEncoding is the Content-Encoding describing RawResponse.
	// Empty means identity. Ingest needs it to decode before reducing;
	// without it the bytes are unreducible archive.
	RawResponseEncoding string `json:"raw_response_encoding,omitempty"`

	// RawResponseWithheld says this adapter captured verbatim bytes for the
	// turn and chose not to send them — always because including them would
	// have pushed the envelope past ingest's body limit, and a turn without
	// its bytes beats no turn at all.
	//
	// Only the producer can know this. An envelope with no raw_response is
	// otherwise the same envelope whether the bytes never existed or were
	// dropped on the way out, and those are opposite operational facts: the
	// first says which adapters are deployed, the second says a limit bit and
	// wants tuning. Ingest folds this into raw_response_dropped; the field is
	// spelled differently on purpose, because that column is the union of "the
	// producer withheld" and "ingest capped" and this is only the first.
	//
	// It is set at exactly the two places bytes are withheld after being
	// captured — the pre-dispatch transport-budget decision, and
	// enforceBodyLimit's post-marshal strip — and nowhere else. In particular
	// it is NOT set when the mode never asked for bytes: mode=off captured
	// nothing to withhold, and marking those turns would report the whole
	// fleet as losing bytes it never had.
	//
	// Invariant, relied on by ingest, which honors the marker only when no
	// bytes arrived: withheld implies raw_response absent. Both writers below
	// clear the bytes in the same breath as setting this.
	RawResponseWithheld bool `json:"raw_response_withheld,omitempty"`

	// No omitempty: it has no effect on a non-pointer struct, and meta is
	// always sent.
	Meta TurnMeta `json:"meta"`
	// Session is the optional session-tracking block. Present
	// (non-nil) when the inbound request carried any X-Tapes-*
	// header; nil otherwise. omitempty keeps the wire shape stable
	// for tapes-ingest endpoints that don't know about session
	// blocks yet.
	Session *DispatchedSessionEnvelope `json:"session,omitempty"`

	// There is deliberately no raw_response_dropped field. Ingest computes
	// that marker itself from the bytes it actually received, and does not
	// read one off the payload — a producer-supplied value would be
	// unverifiable and could contradict what was stored. RawResponseWithheld
	// above is not a counterexample: it reports a decision made here, which
	// ingest cannot observe, rather than an outcome ingest can see for itself.

	// reducedFallback holds the adapter's reduction on a raw-only envelope
	// so the transport backstop in Dispatch can restore it if the bytes
	// have to be stripped. Unexported, so it never reaches the wire; the
	// envelope's public shape is exactly what ingest parses.
	reducedFallback *llm.ChatResponse
}

// TurnMeta carries capture-side metadata. Kept in its own struct so it can
// grow without disturbing the Provider/Request/Response contract ingest
// already accepts.
//
// Every field serializes onto the wire envelope: tapes-ingest persists the
// whole meta block verbatim into its immutable raw-turn store, so anything
// captured here is recoverable downstream without re-capture. Ingest
// deployments that predate the raw store simply ignore the extra keys.
type TurnMeta struct {
	RequestID   string `json:"request_id,omitempty"`
	ContentType string `json:"content_type,omitempty"`

	// ThreadID is the harness's sub-thread identifier for this call
	// (e.g. Claude Code's subagent agent-id), "" for main-thread
	// calls. Resolved harness-neutrally by headers.ThreadID.
	ThreadID string `json:"thread_id,omitempty"`

	Method              string  `json:"method,omitempty"`
	Path                string  `json:"path,omitempty"`
	Endpoint            string  `json:"endpoint,omitempty"`
	Model               string  `json:"model,omitempty"`
	ModelFamily         string  `json:"model_family,omitempty"`
	Stream              string  `json:"stream,omitempty"`
	ContentEncoding     string  `json:"content_encoding,omitempty"`
	UpstreamStatus      int     `json:"upstream_status,omitempty"`
	UpstreamStatusClass string  `json:"upstream_status_class,omitempty"`
	RequestBytes        int     `json:"request_bytes,omitempty"`
	ResponseBytes       int     `json:"response_bytes,omitempty"`
	ElapsedSeconds      float64 `json:"elapsed_seconds,omitempty"`
}

func (m TurnMeta) outcomeContext() OutcomeContext {
	return OutcomeContext{
		Method:              m.Method,
		Path:                m.Path,
		ThreadID:            m.ThreadID,
		Endpoint:            m.Endpoint,
		Model:               m.Model,
		ModelFamily:         m.ModelFamily,
		Stream:              m.Stream,
		ContentType:         m.ContentType,
		ContentEncoding:     m.ContentEncoding,
		UpstreamStatus:      m.UpstreamStatus,
		UpstreamStatusClass: m.UpstreamStatusClass,
		RequestBytes:        m.RequestBytes,
		ResponseBytes:       m.ResponseBytes,
		ElapsedSeconds:      m.ElapsedSeconds,
	}
}

// DispatchedSessionEnvelope is the session-tracking block posted
// to tapes-ingest. The JSON tags are the wire shape tapes-ingest
// parses against; renaming a field is a breaking change to ingest,
// not just to this struct.
//
// Naming: this type is the wire-shaped sibling of
// headers.SessionEnvelope (the parsed view of the inbound request).
// The two are intentionally separate — headers.SessionEnvelope tracks
// parser-side state (Present, HarnessMetadataMalformed) that has no
// place on the dispatched JSON, and DispatchedSessionEnvelope carries
// the auth-derived fields (OrgID, AuthSubject) the headers package
// can't see. buildSessionEnvelope is the one site that maps between
// them.
//
// HarnessMetadata is the decoded JSON object (already base64url-
// decoded by the processor). nil means no metadata header was
// attached — distinct from "{}" which the caller may attach
// explicitly.
type DispatchedSessionEnvelope struct {
	OrgID                  string         `json:"org_id"`
	AuthSubject            string         `json:"auth_subject"`
	HarnessID              string         `json:"harness_id"`
	HarnessSessionID       string         `json:"harness_session_id,omitempty"`
	HarnessVersion         string         `json:"harness_version,omitempty"`
	Cwd                    string         `json:"cwd,omitempty"`
	Name                   string         `json:"name,omitempty"`
	ParentHarnessSessionID string         `json:"parent_harness_session_id,omitempty"`
	HarnessMetadata        map[string]any `json:"harness_metadata,omitempty"`
}

// NewDispatcher returns a Dispatcher with the given ingest URL and in-flight
// cap. maxInflight <= 0 defaults to 100.
func NewDispatcher(ingestURL string, maxInflight int, client *http.Client) *Dispatcher {
	if maxInflight <= 0 {
		maxInflight = 100
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Dispatcher{
		ingestURL:  ingestURL,
		httpClient: client,
		sem:        make(chan struct{}, maxInflight),
	}
}

// SetObserver installs a metrics/spy observer. Thread-unsafe once Dispatch
// is running; call during setup only.
func (d *Dispatcher) SetObserver(o Observer) { d.observer = o }

// RecordDrop is used by the processor to record drops that happened before
// the dispatch stage (client disconnect, unknown provider, …) so all drop
// reasons pass through one observation point.
func (d *Dispatcher) RecordDrop(provider string, reason DropReason, requestID string) {
	d.RecordDropContext(provider, reason, requestID, OutcomeContext{})
}

// RecordDropContext is the rich variant used when the processor has already
// parsed request metadata. It keeps metric labels bounded while logs retain
// enough context to answer model/endpoint/size/status questions.
func (d *Dispatcher) RecordDropContext(provider string, reason DropReason, requestID string, ctx OutcomeContext) {
	attrs := make([]any, 0, 6+len(ctx.logAttrs()))
	attrs = append(attrs,
		"provider", provider,
		"reason", string(reason),
		"request_id", requestID,
	)
	attrs = append(attrs, ctx.logAttrs()...)
	slog.Warn("extproc drop",
		attrs...,
	)
	d.safeOnDrop(provider, reason, requestID, ctx)
}

// safeOnDrop and safeOnAccepted invoke the observer with a recover() guard so
// a misbehaving observer can't crash the dispatcher goroutine (the most
// common cause is a test double that accidentally dereferences nil). Panics
// are logged but not re-raised — metrics are observability, not a critical
// path.
func (d *Dispatcher) safeOnDrop(provider string, reason DropReason, requestID string, ctx OutcomeContext) {
	if d.observer == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Error("extproc: observer OnDrop panicked",
				"panic", r,
				"provider", provider,
				"reason", reason,
			)
		}
	}()
	d.observer.OnDrop(provider, reason, requestID)
	if o, ok := d.observer.(ContextObserver); ok {
		o.OnDropContext(provider, reason, requestID, ctx)
	}
}

func (d *Dispatcher) safeOnAccepted(provider, requestID string, ctx OutcomeContext) {
	if d.observer == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Error("extproc: observer OnAccepted panicked",
				"panic", r,
				"provider", provider,
			)
		}
	}()
	d.observer.OnAccepted(provider, requestID)
	if o, ok := d.observer.(ContextObserver); ok {
		o.OnAcceptedContext(provider, requestID, ctx)
	}
}

// safeOnRawResponse* mirror the recover()-guarded pattern above: raw-lane
// accounting is observability, and a misbehaving observer must not take down
// the dispatch goroutine.
func (d *Dispatcher) safeOnRawResponseAttached(provider, shape string) {
	d.withRawLaneObserver(func(o RawLaneObserver) { o.OnRawResponseAttached(provider, shape) })
}

func (d *Dispatcher) safeOnRawResponseSkipped(provider, reason string) {
	d.withRawLaneObserver(func(o RawLaneObserver) { o.OnRawResponseSkipped(provider, reason) })
}

func (d *Dispatcher) safeOnRawResponseFallback(provider, reason string) {
	d.withRawLaneObserver(func(o RawLaneObserver) { o.OnRawResponseFallback(provider, reason) })
}

func (d *Dispatcher) withRawLaneObserver(fn func(RawLaneObserver)) {
	o, ok := d.observer.(RawLaneObserver)
	if !ok {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Error("extproc: raw-lane observer panicked", "panic", r)
		}
	}()
	fn(o)
}

func (d *Dispatcher) safeOnDispatchLatency(provider, requestID string, seconds float64) {
	if d.observer == nil {
		return
	}
	if o, ok := d.observer.(DispatchObserver); ok {
		o.OnDispatchLatency(provider, requestID, seconds)
	}
}

func (d *Dispatcher) safeOnInflight(n int) {
	if d.observer == nil {
		return
	}
	if o, ok := d.observer.(DispatchObserver); ok {
		o.OnInflight(n)
	}
}

// Dispatch enqueues a turn for POST to tapes-ingest. Returns immediately;
// the actual HTTP call runs on a goroutine bounded by the semaphore.
func (d *Dispatcher) Dispatch(ctx context.Context, env TurnEnvelope) {
	select {
	case d.sem <- struct{}{}:
		d.safeOnInflight(len(d.sem))
	default:
		d.RecordDropContext(env.Provider, DropSemFull, env.Meta.RequestID, env.Meta.outcomeContext())
		return
	}

	payload, err := json.Marshal(env)
	if err == nil {
		payload, env = d.enforceBodyLimit(env, payload)
	}
	if err != nil {
		// THIS is the test that retroactively catches the 4-week bug: a
		// silent _ on json.Marshal is what kept every streaming turn from
		// landing. Fail loudly, meter it, log it.
		<-d.sem
		d.safeOnInflight(len(d.sem))
		slog.Error("extproc: dispatch marshal failed",
			"provider", env.Provider,
			"error", err,
			"request_id", env.Meta.RequestID,
		)
		d.RecordDropContext(env.Provider, DropMarshalError, env.Meta.RequestID, env.Meta.outcomeContext())
		return
	}

	// Attachment is metered here — after enforceBodyLimit has ruled on the
	// real marshalled size — so each turn is counted once, by the shape it
	// actually ships with. Metering the pre-dispatch decision instead would
	// count a stripped turn as attached AND skipped, and under raw mode
	// record a raw_only shape for a turn that landed with its reduction.
	if len(env.RawResponse) > 0 {
		shape := rawShapeDual
		if env.Response == nil {
			shape = rawShapeRawOnly
		}
		d.safeOnRawResponseAttached(env.Provider, shape)
	}

	go func() {
		defer func() {
			<-d.sem
			d.safeOnInflight(len(d.sem))
		}()
		d.postWithRetry(ctx, env.Provider, env.Meta.RequestID, env.Meta.outcomeContext(), payload)
	}()
}

// enforceBodyLimit is the exact backstop on the raw lane's one real hazard:
// an envelope too large for ingest's Fiber body limit is rejected at the
// transport, losing the entire turn — reduction, request, and session
// attribution — with no fidelity marker recorded anywhere.
//
// The estimate in rawResponseFits runs before the envelope exists and cannot
// see the marshalled size. This runs after, on the real bytes, and is the
// authority. When an oversize envelope is carrying verbatim bytes, the bytes
// are the expendable part: strip them, restore the reduction if the envelope
// was raw-only, and re-marshal. A turn that lands reduced beats one that
// does not land.
//
// If the payload is still oversize without the bytes, it is sent as-is. That
// is the pre-raw-lane behavior for an enormous turn (a 413 answered with
// DropIngestReject), and the raw lane should not invent a new failure mode
// for turns it is not responsible for.
func (d *Dispatcher) enforceBodyLimit(env TurnEnvelope, payload []byte) ([]byte, TurnEnvelope) {
	if len(payload) <= ingest.MaxIngestBodyBytes || len(env.RawResponse) == 0 {
		return payload, env
	}

	slog.Warn("extproc: envelope over ingest body limit, dropping verbatim bytes",
		"provider", env.Provider,
		"request_id", env.Meta.RequestID,
		"payload_bytes", len(payload),
		"limit", ingest.MaxIngestBodyBytes,
		"raw_response_bytes", len(env.RawResponse),
		"raw_only", env.Response == nil,
	)

	stripped := env
	stripped.RawResponse = nil
	stripped.RawResponseEncoding = ""
	// Bytes existed and this process decided not to send them. Set in the
	// same breath as clearing them so the two can never disagree — ingest
	// honors the marker only on an envelope carrying no bytes.
	stripped.RawResponseWithheld = true
	if stripped.Response == nil {
		// Raw-only: without the bytes there is no response at all, so the
		// reduction has to come back or ingest rejects an empty turn.
		stripped.Response = stripped.reducedFallback
	}

	repacked, err := json.Marshal(stripped)
	if err != nil {
		// Cannot happen in practice — the same envelope minus a []byte
		// field already marshalled once. Keep the original rather than
		// failing the turn: a 413 beats a guaranteed marshal drop.
		return payload, env
	}
	d.safeOnRawResponseSkipped(stripped.Provider, rawSkipOversizeStripped)
	return repacked, stripped
}

// postWithRetry wraps the HTTP POST with bounded exponential backoff. 5xx and
// network errors retry; 4xx is terminal. Honors ctx cancellation between
// attempts so shutdown doesn't hang on a stuck retry loop.
func (d *Dispatcher) postWithRetry(ctx context.Context, provider, requestID string, outcome OutcomeContext, payload []byte) {
	const maxAttempts = 3
	backoff := 500 * time.Millisecond
	started := time.Now()
	defer func() {
		d.safeOnDispatchLatency(provider, requestID, time.Since(started).Seconds())
	}()

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			d.RecordDropContext(provider, DropIngestTimeout, requestID, outcome)
			return
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.ingestURL+"/v1/ingest", bytes.NewReader(payload))
		if err != nil {
			d.RecordDropContext(provider, DropIngestTimeout, requestID, outcome)
			return
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := d.httpClient.Do(req)
		if err != nil {
			slog.Warn("extproc: ingest POST network error",
				"error", err,
				"attempt", attempt,
				"request_id", requestID,
			)
			if attempt == maxAttempts {
				d.RecordDropContext(provider, DropIngestTimeout, requestID, outcome)
				return
			}
			if !sleepForBackoff(ctx, backoff) {
				d.RecordDropContext(provider, DropIngestTimeout, requestID, outcome)
				return
			}
			backoff *= 2
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK:
			d.safeOnAccepted(provider, requestID, outcome)
			return
		case resp.StatusCode >= 500:
			slog.Warn("extproc: ingest 5xx, retrying",
				"status", resp.StatusCode,
				"body", string(body),
				"attempt", attempt,
				"request_id", requestID,
			)
			if attempt == maxAttempts {
				d.RecordDropContext(provider, DropIngestTimeout, requestID, outcome)
				return
			}
			if !sleepForBackoff(ctx, backoff) {
				d.RecordDropContext(provider, DropIngestTimeout, requestID, outcome)
				return
			}
			backoff *= 2
		default:
			// 4xx — terminal. Don't retry.
			slog.Warn("extproc: ingest rejected turn",
				"status", resp.StatusCode,
				"body", string(body),
				"request_id", requestID,
			)
			d.RecordDropContext(provider, DropIngestReject, requestID, outcome)
			return
		}
	}
}

// sleepForBackoff blocks for d. Returns true if the sleep finished, false if
// the context was cancelled first — callers use the return value to bail on
// shutdown rather than starting another retry attempt.
func sleepForBackoff(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
