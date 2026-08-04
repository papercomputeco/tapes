package extproc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	processingmodev3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	"github.com/papercomputeco/tapes/extproc/headers"
	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/llm"
)

// processRequestHeadersResponse builds the ProcessingResponse the
// processor sends back for the RequestHeaders phase. Pulled out so
// envelope stripping (a HeaderMutation listing every X-Tapes-* key
// observed) lives next to the only place it's constructed. removeKeys
// nil/empty produces a plain ack response with no mutation block.
func processRequestHeadersResponse(removeKeys []string) *extprocv3.ProcessingResponse {
	hr := &extprocv3.HeadersResponse{}
	if len(removeKeys) > 0 {
		hr.Response = &extprocv3.CommonResponse{
			HeaderMutation: &extprocv3.HeaderMutation{
				RemoveHeaders: removeKeys,
			},
		}
	}
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: hr,
		},
	}
}

// Processor implements the ext_proc ExternalProcessor service. It is the
// sidecar that observes traffic through a tenant's Envoy AI Gateway and
// forwards completed turns to tapes-ingest.
//
// The processor is a per-stream state machine: accumulate request and
// response bodies with append (never overwrite), gate dispatch on
// EndOfStream so partial frames don't trigger a premature POST, and issue
// ModeOverride to FULL_DUPLEX_STREAMED only when stream:true is seen in the
// request body so non-streaming turns stay on the default BUFFERED path.
// Body accumulators are plain bytes.Buffer — per-turn memory scales with
// the upstream response, ceiling'd in practice by the caller's max_tokens.
// tapes_extproc_body_bytes (histogram) and tapes_extproc_turns_large_total
// (counter above LargeTurnThreshold) track the distribution so cluster
// sizing can be adjusted from real data.
type Processor struct {
	extprocv3.UnimplementedExternalProcessorServer

	ingestURL   string
	providerMap map[string]string
	dispatcher  *Dispatcher
	metrics     *Metrics
	maxInflight int
	reducers    map[string]capture.Reducer

	// rawResponseMode selects the response half of the dispatch envelope.
	// Zero value is RawResponseOff, so a Processor built from a bare
	// Config keeps the historical wire shape.
	rawResponseMode RawResponseMode
}

// streamState holds all per-stream bookkeeping the processor gathers as the
// five ext_proc phases march through. One instance per Process RPC.
type streamState struct {
	provider               string
	agentName              string
	threadID               string
	contentType            string
	contentEncoding        string
	requestContentEncoding string
	requestDecodeErr       error
	statusCode             int
	streaming              bool
	streamLabel            string
	requestID              string
	path                   string
	method                 string
	endpoint               string
	model                  string
	modelFamily            string
	startedAt              time.Time

	// session is the parsed X-Tapes-* envelope from the inbound
	// request. session.Present == false means no X-Tapes-* header
	// arrived — the dispatcher omits the session block entirely
	// from the envelope POST in that case.
	session headers.SessionEnvelope

	// orgID and authSubject are server-trusted identity fields read
	// from PaperAuthOrgID / PaperAuthSubject — headers the upstream
	// gateway populates from validated JWT claims (via Envoy's
	// claim_to_headers feature). Clients are not permitted to send
	// these themselves; if the gateway isn't configured to populate
	// them, both stay empty and flow through to the session block as
	// empty strings.
	orgID       string
	authSubject string

	reqBuf     bytes.Buffer
	decodedReq []byte
	respBuf    bytes.Buffer

	reqEOS  bool
	respEOS bool
}

// NewProcessor builds a Processor from Config.
func NewProcessor(cfg Config) (*Processor, error) {
	providerMap := map[string]string{}
	if cfg.ProviderMapFile != "" {
		data, err := os.ReadFile(cfg.ProviderMapFile)
		if err != nil {
			slog.Warn("Could not read provider map file", "path", cfg.ProviderMapFile, "error", err)
		} else if err := yaml.Unmarshal(data, &providerMap); err != nil {
			slog.Warn("Could not parse provider map file", "error", err)
		}
	}

	metrics := NewMetrics()
	dispatcher := NewDispatcher(cfg.IngestURL, cfg.MaxInflight, &http.Client{
		Timeout: 30 * time.Second,
	})
	dispatcher.SetObserver(metrics.AsObserver())

	return &Processor{
		ingestURL:       cfg.IngestURL,
		providerMap:     providerMap,
		dispatcher:      dispatcher,
		metrics:         metrics,
		maxInflight:     cfg.MaxInflight,
		rawResponseMode: cfg.RawResponseMode,
		reducers: map[string]capture.Reducer{
			capture.ProviderAnthropic: capture.NewAnthropicReducer(),
			capture.ProviderOpenAI:    capture.NewOpenAIResponsesReducer(),
		},
	}, nil
}

// Metrics exposes the Prometheus registry so the cmd wiring can mount
// /metrics on its existing HTTP mux.
func (p *Processor) Metrics() *Metrics { return p.metrics }

// reducerFor returns the reducer able to consume this turn's wire format.
// Eligibility is a positive (provider, endpoint) allowlist: each reducer
// parses exactly one wire format, so a provider's reducer must never be
// handed another endpoint's bytes — the OpenAI Responses reducer cannot
// parse Chat Completions frames, and the Anthropic Messages reducer only
// understands Messages turns. Ineligible turns keep the pre-capture
// behavior: default BUFFERED Envoy mode and an unknown_provider drop.
// Used to gate behavior that only makes sense when we can actually
// consume the upstream bytes.
func (p *Processor) reducerFor(provider, endpoint string) (capture.Reducer, bool) {
	if !reducerHandlesEndpoint(provider, endpoint) {
		return nil, false
	}
	r, ok := p.reducers[provider]
	return r, ok
}

// reducerHandlesEndpoint is the wire-format allowlist backing reducerFor.
// New reducers must add their (provider, endpoint) pair here explicitly —
// defaulting to false makes a missing entry a loud test failure rather
// than a silently mis-fed reducer.
func reducerHandlesEndpoint(provider, endpoint string) bool {
	switch provider {
	case capture.ProviderAnthropic:
		return endpoint == endpointMessages
	case capture.ProviderOpenAI:
		return endpoint == endpointResponses
	default:
		return false
	}
}

// RegisterServer installs p on the given gRPC server.
func RegisterServer(s *grpc.Server, p *Processor) {
	extprocv3.RegisterExternalProcessorServer(s, p)
}

// Dispatcher returns the dispatch path, exposed for tests and metric wiring.
func (p *Processor) Dispatcher() *Dispatcher { return p.dispatcher }

// SetProviderMap replaces the backend-to-provider mapping used by
// resolveProvider. Tests use this to exercise the unknown-provider path
// without creating a real ProviderMapFile on disk.
func (p *Processor) SetProviderMap(m map[string]string) {
	if m == nil {
		m = map[string]string{}
	}
	p.providerMap = m
}

// Process implements the bidirectional ext_proc RPC. The state machine is
// linear: RequestHeaders → RequestBody* → ResponseHeaders → ResponseBody*
// with dispatch firing once on response-body EOS (or on client disconnect for
// a drop metric).
func (p *Processor) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	st := &streamState{}
	// turnDone records that this turn has reached a terminal state (dispatched
	// or explicitly dropped). The defer uses it to decide whether to fire a
	// late-stage drop — the name is deliberately separate from "dispatched"
	// since we flip it to true in drop paths that never hit ingest.
	turnDone := false

	defer func() {
		if turnDone {
			return
		}
		// Stream closed before we reached a terminal state. Classify by
		// what we observed:
		//   1. Request ended but we never got a response → upstream silence.
		//   2. A 200 OK with response bytes but no EOS → most often the
		//      downstream client closed the connection right after parsing
		//      message_stop, before Envoy observed the upstream TCP FIN.
		//      Under ext_proc Streamed mode this is the dominant shape, and
		//      the buffer typically holds a complete logical turn. Try the
		//      reducer; dispatchTurn classifies its own failure modes
		//      (decode_error / reducer_error / unknown_provider / empty
		//      response), so an unsalvageable buffer still records the
		//      right reason rather than masquerading as client_disconnect.
		//      Operators distinguish a clean early-close from a genuine
		//      mid-stream error via the captured ChatResponse.Done field
		//      in the ingest envelope.
		//   3. Anything else with bytes or a completed request → genuine
		//      client disconnect (mid-request tear-down or non-2xx
		//      response shape).
		switch {
		case st.reqEOS && !st.respEOS && len(st.respBuf.Bytes()) == 0:
			p.recordDrop(st, DropUpstreamNoResponse)
		case st.reqEOS && !st.respEOS && st.statusCode == http.StatusOK && len(st.respBuf.Bytes()) > 0:
			p.dispatchTurn(context.Background(), st)
		case len(st.respBuf.Bytes()) > 0 || st.reqEOS:
			p.recordDrop(st, DropClientDisconnect)
		}
	}()

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv: %v", err)
		}

		var resp *extprocv3.ProcessingResponse

		switch v := msg.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			p.onRequestHeaders(st, v.RequestHeaders)
			// Strip the X-Tapes-* envelope from the request before
			// Envoy forwards it upstream to the LLM provider — these
			// headers are internal to capture and must not leak.
			// Prefix-based removal so forward-compatible additions
			// to the envelope are also stripped without a code
			// change here.
			resp = processRequestHeadersResponse(
				headers.EnvelopeHeaderKeysFromRequest(v.RequestHeaders),
			)

		case *extprocv3.ProcessingRequest_RequestBody:
			resp = p.onRequestBody(st, v.RequestBody)

		case *extprocv3.ProcessingRequest_ResponseHeaders:
			st.statusCode = headers.StatusCode(v.ResponseHeaders)
			st.contentType = headers.Get(v.ResponseHeaders, headers.ContentType)
			st.contentEncoding = headers.Get(v.ResponseHeaders, headers.ContentEncoding)
			resp = &extprocv3.ProcessingResponse{
				Response: &extprocv3.ProcessingResponse_ResponseHeaders{
					ResponseHeaders: &extprocv3.HeadersResponse{},
				},
			}

		case *extprocv3.ProcessingRequest_ResponseBody:
			if _, err := st.respBuf.Write(v.ResponseBody.GetBody()); err != nil {
				slog.Warn("extproc: respBuf write error", "error", err)
			}
			resp = &extprocv3.ProcessingResponse{
				Response: &extprocv3.ProcessingResponse_ResponseBody{
					ResponseBody: &extprocv3.BodyResponse{},
				},
			}
			if v.ResponseBody.GetEndOfStream() {
				st.respEOS = true
				switch st.statusCode {
				case 0:
					// Envoy should always send ResponseHeaders before ResponseBody.
					// If we got here with no :status, something violated the contract
					// — treat as a drop rather than implying success.
					p.recordDrop(st, DropMissingStatus)
				case http.StatusOK:
					p.dispatchTurn(context.Background(), st)
				default:
					p.recordDrop(st, DropUpstreamStatus)
				}
				turnDone = true
			}
		}

		if resp != nil {
			if err := stream.Send(resp); err != nil {
				return status.Errorf(codes.Internal, "send: %v", err)
			}
		}
	}
}

func (p *Processor) onRequestHeaders(st *streamState, hdrs *extprocv3.HttpHeaders) {
	st.startedAt = time.Now()
	st.provider = p.resolveProvider(hdrs)
	st.agentName = headers.Get(hdrs, headers.AgentName)
	st.threadID = headers.ThreadID(hdrs)
	st.path = headers.Get(hdrs, headers.Path)
	st.method = headers.Get(hdrs, headers.Method)
	st.endpoint = classifyEndpoint(st.path)
	st.requestContentEncoding = headers.Get(hdrs, headers.ContentEncoding)
	st.streamLabel = labelUnknown
	st.modelFamily = labelUnknown
	st.requestID = headers.Get(hdrs, headers.RequestID)
	if st.requestID == "" {
		// Upstream sometimes arrives without x-request-id (clients without
		// tracing, or older Envoy configs). Synthesize a local id so every
		// log line and drop metric has something to pivot on — better than
		// an empty tag that makes log triage impossible.
		st.requestID = "extproc-" + randHex(8)
	}
	// Session tracking: parse the X-Tapes-* envelope and capture
	// the auth-derived identity headers. Both are read here so the
	// envelope is built once, on the same headers payload that
	// drives stripping — guaranteeing the values dispatched and the
	// values removed describe the same request.
	st.session = headers.ParseSessionEnvelope(hdrs)
	st.orgID = headers.Get(hdrs, headers.PaperAuthOrgID)
	st.authSubject = headers.Get(hdrs, headers.PaperAuthSubject)
}

// onRequestBody accumulates request-body chunks and, on EndOfStream, parses
// the body to detect streaming mode. If the client requested a streaming
// response, issue ModeOverride so Envoy switches response-body handling to
// FULL_DUPLEX_STREAMED for this one request — decoupling the client tee
// from the capture accumulator and keeping non-streaming turns on BUFFERED.
func (p *Processor) onRequestBody(st *streamState, body *extprocv3.HttpBody) *extprocv3.ProcessingResponse {
	if _, err := st.reqBuf.Write(body.GetBody()); err != nil {
		slog.Warn("extproc: reqBuf write error", "error", err)
	}

	resp := &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestBody{
			RequestBody: &extprocv3.BodyResponse{},
		},
	}

	if body.GetEndOfStream() {
		st.reqEOS = true
		st.decodedReq, st.requestDecodeErr = decodeRequestBody(st.reqBuf.Bytes(), st.requestContentEncoding)
		if st.requestDecodeErr == nil {
			meta := parseRequestMeta(st.provider, st.decodedReq)
			st.streaming = meta.Streaming
			st.streamLabel = meta.StreamLabel
			st.model = meta.Model
			st.modelFamily = meta.ModelFamily
		}
		// Only flip the response-body mode when we have a reducer that can
		// actually consume the streamed bytes. Exposing non-capturable
		// providers to FULL_DUPLEX_STREAMED would trade a stable Envoy code
		// path for a newer one without any capture benefit in return.
		if _, ok := p.reducerFor(st.provider, st.endpoint); st.streaming && ok {
			resp.ModeOverride = &processingmodev3.ProcessingMode{
				ResponseHeaderMode: processingmodev3.ProcessingMode_SEND,
				ResponseBodyMode:   processingmodev3.ProcessingMode_FULL_DUPLEX_STREAMED,
			}
		}
	}

	return resp
}

// dispatchTurn hands the accumulated bodies to the matching reducer and
// forwards the canonical envelope to tapes-ingest.
func (p *Processor) dispatchTurn(ctx context.Context, st *streamState) {
	rawReqBytes := st.reqBuf.Bytes()
	reqBytes := st.decodedReq
	respBytes := st.respBuf.Bytes()

	if p.metrics != nil {
		p.metrics.ObserveBodyBytes(st.provider, "request", len(rawReqBytes))
		p.metrics.ObserveBodyBytes(st.provider, "response", len(respBytes))
		// "Large turn" is observability for cluster sizing — the turn still
		// dispatches normally. Operators cross-reference this counter with
		// the body_bytes histogram to decide when to bump pod memory limits.
		p.metrics.ObserveTurnSize(st.provider, len(respBytes))
	}

	if !isTurnRequestPath(st.path) || (st.method != "" && !strings.EqualFold(st.method, http.MethodPost)) {
		slog.Debug("extproc: skipping non-turn request",
			"provider", st.provider,
			"path", st.path,
			"method", st.method,
			"request_id", st.requestID,
		)
		p.recordDrop(st, DropNonTurnRequest)
		return
	}

	if st.requestDecodeErr != nil {
		slog.Warn("extproc: request decode failed",
			"provider", st.provider,
			"request_id", st.requestID,
			"content_encoding", st.requestContentEncoding,
			"req_bytes", len(rawReqBytes),
			"error", st.requestDecodeErr,
		)
		p.recordDrop(st, DropRequestDecode)
		return
	}

	// An upstream that finishes the response phase with zero body bytes
	// is a structurally different shape from one whose bytes parse to an
	// empty reduction: there is nothing to feed the reducer, no encoding
	// to undo, and no preview that would tell an operator anything new.
	// Health checks and keepalive probes can sit at this surface, so
	// count via a dedicated drop reason (so dashboards separate "upstream
	// sent nothing" from "reducer produced empty content") and log at
	// Debug rather than Warn — this is expected traffic, not an error.
	if len(respBytes) == 0 {
		slog.Debug("extproc: empty upstream response body",
			"provider", st.provider,
			"content_type", st.contentType,
			"content_encoding", st.contentEncoding,
			"request_id", st.requestID,
		)
		p.recordDrop(st, DropEmptyResponse)
		return
	}

	r, ok := p.reducerFor(st.provider, st.endpoint)
	if !ok {
		slog.Warn("extproc: no reducer for provider",
			"provider", st.provider,
			"endpoint", st.endpoint,
			"request_id", st.requestID,
		)
		p.recordDrop(st, DropUnknownProvider)
		return
	}

	// Reducers parse textual SSE / JSON, so any non-identity
	// Content-Encoding on the upstream response yields silent empty
	// content (SSE path) or a JSON parse error (oneshot path) when
	// fed raw to the reducer. Decode here based on the response's
	// Content-Encoding so the reducer always sees the canonical
	// bytes. A non-decodable encoding drops the turn with a distinct
	// reason so it shows up in dashboards rather than being laundered
	// into "reducer_error".
	decodedResp, decodeStats, decodeErr := decodeResponseBodyWithStats(respBytes, st.contentEncoding)
	if decodeErr != nil {
		if p.metrics != nil {
			p.metrics.ObserveResponseDecoded(st.contentEncoding, "error")
		}
		slog.Warn("extproc: response decode failed",
			"provider", st.provider,
			"request_id", st.requestID,
			"content_encoding", st.contentEncoding,
			"content_type", st.contentType,
			"resp_bytes", len(respBytes),
			"resp_preview", respBodyPreview(respBytes, 80),
			"error", decodeErr,
		)
		p.recordDrop(st, DropResponseDecode)
		return
	}
	if p.metrics != nil {
		p.metrics.ObserveResponseDecoded(st.contentEncoding, "ok")
		if st.streaming || normalizeContentType(st.contentType) == "text/event-stream" {
			p.metrics.ObserveSSEChunks(st.provider, countSSEDataFrames(decodedResp))
		}
	}
	if decodeStats.Truncated {
		messageStopSeen := bytes.Contains(decodedResp, []byte("event: message_stop")) ||
			bytes.Contains(decodedResp, []byte(`"type":"message_stop"`))
		if p.metrics != nil {
			p.metrics.ObserveResponseDecodeSalvaged(st.provider, st.contentEncoding, messageStopSeen)
		}
		slog.Info("extproc: response decode salvaged truncated gzip",
			"provider", st.provider,
			"request_id", st.requestID,
			"content_encoding", st.contentEncoding,
			"content_type", st.contentType,
			"resp_bytes_raw", len(respBytes),
			"resp_bytes_decoded", len(decodedResp),
			"message_stop_seen", messageStopSeen,
		)
	}

	chatResp, err := r.Reduce(
		ctx,
		bytes.NewReader(reqBytes),
		bytes.NewReader(decodedResp),
		st.contentType,
	)
	if err != nil {
		slog.Warn("extproc: reducer error",
			"provider", st.provider,
			"error", err,
			"request_id", st.requestID,
		)
		p.recordDrop(st, DropReducerError)
		return
	}

	// Stamp the proxy-measured wall-clock onto Usage before dispatch.
	// tapes-ingest persists Usage.TotalDurationNs verbatim, and the extproc
	// path is the only place this turn's wall-clock is known — the legacy
	// tapes/proxy stampDuration (PCC-514) never runs on cluster traffic
	// (PCC-570). Stamped before the empty-reducer check below so salvaged
	// or edge-case turns that are still dispatched don't re-introduce NULL.
	if chatResp != nil {
		if chatResp.Usage == nil {
			chatResp.Usage = &llm.Usage{}
		}
		chatResp.Usage.TotalDurationNs = st.elapsedNanos()
	}

	// Surface reducer outputs that would fail downstream validation
	// before they are dispatched. The log names every piece of
	// upstream-side state that produced the empty output —
	// content_type, upstream HTTP status, body sizes, and a printable
	// preview of the first bytes of the response body — so an operator
	// can tell from a single line whether the upstream sent SSE, JSON,
	// an error envelope, compressed bytes, or nothing at all. Without
	// this, an empty reduction is indistinguishable from a transient
	// downstream error; with it, the class of upstream shape feeding
	// the reducer is observable from the extproc log alone.
	if reason, empty := reducerEmptyReason(chatResp); empty {
		if p.metrics != nil {
			p.metrics.ObserveReducerEmpty(st.provider, st.contentType, st.statusCode)
		}
		attrs := []any{
			"provider", st.provider,
			"request_id", st.requestID,
			"reason", reason,
			"content_type", st.contentType,
			"content_encoding", st.contentEncoding,
			"upstream_status", st.statusCode,
			"req_bytes", len(rawReqBytes),
			"resp_bytes_raw", len(respBytes),
			"resp_bytes_decoded", len(decodedResp),
			// Preview the bytes the reducer actually saw, post-decode.
			// Raw bytes are not previewed here — if decode reported "ok"
			// the raw view is gzip-noise; if decode failed we never
			// reach this branch.
			"resp_preview", respBodyPreview(decodedResp, 240),
		}
		if chatResp != nil {
			attrs = append(attrs,
				"reducer_role", chatResp.Message.Role,
				"reducer_stop_reason", chatResp.StopReason,
				"reducer_done", chatResp.Done,
			)
			if chatResp.Extra != nil {
				if v, ok := chatResp.Extra["reducer_error"]; ok {
					attrs = append(attrs, "reducer_error", v)
				}
			}
		}
		slog.Warn("extproc: reducer produced empty content", attrs...)
	}

	// Resolve the response half of the envelope. respBytes — not
	// decodedResp — is what the raw lane carries: the column is meant to be
	// byte-faithful to what the upstream actually sent, and st.contentEncoding
	// is what lets ingest undo the compression on its own terms.
	//
	// decodeErr is the raw-only interlock, and it is nil by the time control
	// reaches here — a body this build could not decode dropped the turn
	// above. That early return is what makes raw-only safe rather than
	// hopeful: the decode that already succeeded on this exact
	// (respBytes, st.contentEncoding) pair ran capture.DecodeContentEncoding,
	// which is the function ingest will run on the same pair. The interlock is
	// therefore a fact carried forward, not a guess about the receiver.
	//
	// It is passed rather than hardcoded true so the guard survives
	// refactoring: a future path that attaches bytes it did not decode
	// inherits the fallback instead of silently shipping unreducible bytes
	// with no reduction beside them.
	rawLane := decideRawLane(p.rawResponseMode, len(respBytes), len(reqBytes), decodeErr == nil)
	p.recordRawLane(st, rawLane)

	envelope := TurnEnvelope{
		Provider:  st.provider,
		AgentName: st.agentName,
		Request:   json.RawMessage(reqBytes),
		Response:  chatResp,
		Meta: TurnMeta{
			RequestID:           st.requestID,
			ContentType:         st.contentType,
			ThreadID:            st.threadID,
			Method:              st.method,
			Path:                st.path,
			Endpoint:            st.endpoint,
			Model:               st.model,
			ModelFamily:         st.modelFamily,
			Stream:              st.streamLabel,
			ContentEncoding:     st.contentEncoding,
			UpstreamStatus:      st.statusCode,
			UpstreamStatusClass: statusClass(st.statusCode),
			RequestBytes:        len(rawReqBytes),
			ResponseBytes:       len(respBytes),
			ElapsedSeconds:      st.elapsedSeconds(),
		},
		Session: buildSessionEnvelope(st),
	}

	if rawLane.attachRaw {
		envelope.RawResponse = respBytes
		envelope.RawResponseEncoding = st.contentEncoding
	}
	if rawLane.skipReason != "" {
		// We captured bytes for this turn and are not sending them. Say so:
		// without the marker this envelope is indistinguishable from one
		// produced by an adapter that never captured raw bytes at all, and
		// ingest would read a lost capture as an absent feature.
		//
		// Keyed off skipReason rather than off mode, so the modes that never
		// wanted bytes stay unmarked — they withheld nothing.
		envelope.RawResponseWithheld = true
	}
	if rawLane.omitReduction {
		// Raw-only: ingest reduces server-side with the shared reducers.
		// reducedFallback keeps our result reachable for the transport
		// backstop, but it never reaches the wire.
		envelope.reducedFallback = chatResp
		envelope.Response = nil
	}

	p.dispatcher.Dispatch(ctx, envelope)
}

// recordRawLane logs and meters the raw lane's pre-dispatch outcomes for one
// turn. Both non-attaching outcomes change the fidelity the row lands with, so
// neither is allowed to be silent. Attachment itself is deliberately NOT
// metered here: the dispatcher's post-marshal backstop can still strip the
// bytes, so the attach counter is owned by Dispatch, after final sizing —
// otherwise a stripped turn counts as both attached and skipped, and under
// raw mode with the wrong shape.
func (p *Processor) recordRawLane(st *streamState, d rawLaneDecision) {
	switch {
	case d.skipReason != "":
		slog.Warn("extproc: verbatim response bytes withheld",
			"provider", st.provider,
			"request_id", st.requestID,
			"reason", d.skipReason,
			"resp_bytes", st.respBuf.Len(),
			"req_bytes", st.reqBuf.Len(),
			"limit", ingest.MaxIngestBodyBytes,
		)
		p.dispatcher.safeOnRawResponseSkipped(st.provider, d.skipReason)
	case d.fallbackReason != "":
		slog.Info("extproc: raw-only turn kept its reduction",
			"provider", st.provider,
			"request_id", st.requestID,
			"reason", d.fallbackReason,
			"content_encoding", st.contentEncoding,
		)
		p.dispatcher.safeOnRawResponseFallback(st.provider, d.fallbackReason)
	}
}

func (p *Processor) recordDrop(st *streamState, reason DropReason) {
	p.dispatcher.RecordDropContext(st.provider, reason, st.requestID, st.outcomeContext())
}

func (st *streamState) outcomeContext() OutcomeContext {
	return OutcomeContext{
		Method:              st.method,
		Path:                st.path,
		ThreadID:            st.threadID,
		Endpoint:            st.endpoint,
		Model:               st.model,
		ModelFamily:         st.modelFamily,
		Stream:              st.streamLabel,
		ContentType:         st.contentType,
		ContentEncoding:     st.contentEncoding,
		UpstreamStatus:      st.statusCode,
		UpstreamStatusClass: statusClass(st.statusCode),
		RequestBytes:        st.reqBuf.Len(),
		ResponseBytes:       st.respBuf.Len(),
		ElapsedSeconds:      st.elapsedSeconds(),
	}
}

func (st *streamState) elapsedSeconds() float64 {
	if st.startedAt.IsZero() {
		return 0
	}
	return time.Since(st.startedAt).Seconds()
}

// elapsedNanos is the nanosecond sibling of elapsedSeconds, used to stamp
// Usage.TotalDurationNs on the dispatched turn (PCC-570). Same zero-guard:
// a never-stamped startedAt yields 0 rather than a since-epoch duration.
func (st *streamState) elapsedNanos() int64 {
	if st.startedAt.IsZero() {
		return 0
	}
	return time.Since(st.startedAt).Nanoseconds()
}

// buildSessionEnvelope materializes the session block posted to
// tapes-ingest. Returns nil — which json.Marshal omits via the
// omitempty tag — when no X-Tapes-* envelope arrived on this
// request. When the block IS present, org_id and auth_subject are
// mirrored verbatim from PaperAuthOrgID / PaperAuthSubject and the
// X-Tapes-* fields are mirrored verbatim from the inbound request.
func buildSessionEnvelope(st *streamState) *DispatchedSessionEnvelope {
	if !st.session.Present {
		return nil
	}
	return &DispatchedSessionEnvelope{
		OrgID:                  st.orgID,
		AuthSubject:            st.authSubject,
		HarnessID:              st.session.HarnessID,
		HarnessSessionID:       st.session.HarnessSessionID,
		HarnessVersion:         st.session.HarnessVersion,
		Cwd:                    st.session.Cwd,
		Name:                   st.session.Name,
		ParentHarnessSessionID: st.session.ParentHarnessSessionID,
		HarnessMetadata:        st.session.HarnessMetadata,
	}
}

// Decoding a captured body's Content-Encoding is capture.DecodeContentEncoding
// — the same function the receiving ingest runs on the bytes this adapter
// ships. It used to be a second implementation here, byte-identical by
// discipline rather than by construction, from the era when extproc built
// against a published tapes module and could not name it.
//
// That mattered for more than tidiness. The raw-only lane interlocks on
// whether ingest can decode what we send (see rawlane.go); with two decoders
// the interlock could only ever be an assertion about a copy. With one, a turn
// that decoded here has been decoded by ingest's decoder, on the exact bytes
// and encoding the envelope carries.
//
// The request and response halves still differ, and that difference is the
// reason these two wrappers exist rather than raw calls at each site: a
// truncated response is salvaged, a truncated request is refused.

// decodeRequestBody returns canonical request JSON for metadata parsing,
// reducer input, and the ingest envelope. Pi 0.80.4+ compresses Codex
// request bodies with zstd, so treating the wire bytes as json.RawMessage
// causes the entire turn to fail during envelope marshaling.
//
// A salvaged request is refused where a salvaged response is kept: the
// response is prose, and most of it still reads, while the request is JSON
// the reducer and metadata parser both have to parse. A truncated one is not
// partially useful, it is a syntax error.
func decodeRequestBody(body []byte, contentEncoding string) ([]byte, error) {
	decoded, stats, err := capture.DecodeContentEncoding(body, contentEncoding)
	if err != nil {
		return nil, err
	}
	if stats.Truncated {
		return nil, errors.New("truncated compressed request body")
	}
	return decoded, nil
}

// decodeResponseBody returns the bytes the reducer should parse, given the
// captured Content-Encoding header. What is and is not decodable, how stacked
// layers are peeled, and where the size cap sits are all
// capture.DecodeContentEncoding's to state — restating them here is how the
// two copies started drifting the first time.
//
// What is local: an error here becomes a DropResponseDecode in dispatchTurn,
// so an encoding this build cannot read shows up in dashboards under its own
// reason rather than laundered into "reducer_error".
func decodeResponseBody(body []byte, contentEncoding string) ([]byte, error) {
	decoded, _, err := capture.DecodeContentEncoding(body, contentEncoding)
	return decoded, err
}

// decodeResponseBodyWithStats is decodeResponseBody plus the salvage report.
// Callers that care whether the stream ended early — the truncation metric,
// and nothing else — take this form.
func decodeResponseBodyWithStats(body []byte, contentEncoding string) ([]byte, capture.DecodeStats, error) {
	return capture.DecodeContentEncoding(body, contentEncoding)
}

// reducerEmptyReason classifies a reducer output that ingest's
// validateReducedResponse would reject. Returns ("", false) for outputs that
// satisfy the validator. Reasons are intentionally a small bounded set so
// they're safe to use as a metric label.
func reducerEmptyReason(resp *llm.ChatResponse) (string, bool) {
	if resp == nil {
		return "nil_response", true
	}
	if resp.Message.Role == "" {
		return "missing_role", true
	}
	if len(resp.Message.Content) == 0 {
		return "empty_content", true
	}
	for _, block := range resp.Message.Content {
		if block.Type == "" {
			return "missing_block_type", true
		}
	}
	return "", false
}

// respBodyPreview returns a printable preview of up to maxBytes bytes from
// body. Non-printable bytes are replaced with their hex code so the log
// line stays single-line and grep-friendly even when the upstream sent a
// binary, gzipped, or otherwise opaque payload that the reducer choked on.
// Truncation is signalled with a trailing "...(<total>B total)".
func respBodyPreview(body []byte, maxBytes int) string {
	if len(body) == 0 {
		return "<empty>"
	}
	n := min(len(body), maxBytes)
	var b strings.Builder
	b.Grow(n + 16)
	for i := range n {
		c := body[i]
		switch {
		case c == '\\':
			b.WriteString("\\\\")
		case c == '"':
			b.WriteString("\\\"")
		case c >= 0x20 && c < 0x7f:
			b.WriteByte(c)
		case c == '\n':
			b.WriteString("\\n")
		case c == '\r':
			b.WriteString("\\r")
		case c == '\t':
			b.WriteString("\\t")
		default:
			fmt.Fprintf(&b, "\\x%02x", c)
		}
	}
	if len(body) > maxBytes {
		fmt.Fprintf(&b, "...(%dB total)", len(body))
	}
	return b.String()
}

// resolveProvider picks the provider name from request headers: prefer the
// explicit backend selector injected by Envoy AI Gateway, then fall back to
// matching the path. The parameter is named `hdrs` rather than `headers`
// to avoid shadowing the package-level headers import.
func (p *Processor) resolveProvider(hdrs *extprocv3.HttpHeaders) string {
	backend := headers.Get(hdrs, headers.AIGSelectedBackend)
	if provider, ok := p.providerMap[backend]; ok {
		return provider
	}

	path := headers.Get(hdrs, headers.Path)
	switch {
	case pathHasCleanSuffix(path, "/v1/chat/completions"):
		return labelOpenAI
	case pathHasCleanSuffix(path, "/v1/responses"), pathHasCleanSuffix(path, "/codex/responses"):
		// Codex/OpenAI Responses traffic rides direct platform-gateway
		// routes that bypass Envoy AI Gateway (api.openai.com under
		// /v1/responses; chatgpt.com plan-auth under /codex/responses),
		// so no backend-selector header is present; the path is the only
		// provider signal.
		return labelOpenAI
	case pathHasCleanSuffix(path, "/v1/messages/count_tokens"):
		return labelAnthropic
	case pathHasCleanSuffix(path, "/v1/messages"):
		return labelAnthropic
	case pathHasCleanSuffix(path, "/api/chat"):
		return labelOllama
	}

	return labelAnthropic
}

// isTurnRequestPath reports whether path is a provider chat-completion turn.
// Anthropic has adjacent non-turn endpoints such as /v1/messages/count_tokens
// whose successful responses contain token counts, not assistant content. Those
// must not be reduced and posted to ingest as conversation turns.
func isTurnRequestPath(path string) bool {
	return pathHasCleanSuffix(path, "/v1/chat/completions") ||
		pathHasCleanSuffix(path, "/v1/responses") ||
		pathHasCleanSuffix(path, "/codex/responses") ||
		pathHasCleanSuffix(path, "/v1/messages") ||
		pathHasCleanSuffix(path, "/api/chat")
}

// classifyEndpoint labels that reducerHandlesEndpoint keys capture
// eligibility on.
const (
	endpointMessages  = "messages"
	endpointResponses = "responses"
)

func classifyEndpoint(path string) string {
	switch {
	case pathHasCleanSuffix(path, "/v1/messages/count_tokens"):
		return "messages_count_tokens"
	case pathHasCleanSuffix(path, "/v1/messages"):
		return endpointMessages
	case pathHasCleanSuffix(path, "/v1/chat/completions"):
		return "chat_completions"
	case pathHasCleanSuffix(path, "/v1/responses"), pathHasCleanSuffix(path, "/codex/responses"):
		return endpointResponses
	case pathHasCleanSuffix(path, "/api/chat"):
		return "ollama_chat"
	default:
		return labelOther
	}
}

func pathHasCleanSuffix(path, suffix string) bool {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	path = strings.TrimRight(path, "/")
	return strings.HasSuffix(path, suffix)
}

type requestMeta struct {
	Streaming   bool
	StreamLabel string
	Model       string
	ModelFamily string
}

// parseRequestMeta extracts only safe top-level request metadata. It avoids
// payload logging and never walks message/tool content, so callers can use the
// result in metrics/log dimensions without exposing prompt text.
func parseRequestMeta(provider string, reqBody []byte) requestMeta {
	meta := requestMeta{
		StreamLabel: labelUnknown,
		ModelFamily: labelUnknown,
	}
	if len(reqBody) == 0 {
		return meta
	}

	switch provider {
	case labelAnthropic, labelOpenAI, labelOllama:
		var probe struct {
			Stream *bool  `json:"stream"`
			Model  string `json:"model"`
		}
		if err := json.Unmarshal(reqBody, &probe); err != nil {
			return meta
		}
		meta.Model = safeModel(probe.Model)
		meta.ModelFamily = modelFamily(probe.Model)
		if probe.Stream == nil {
			meta.StreamLabel = labelFalse
			return meta
		}
		meta.Streaming = *probe.Stream
		if meta.Streaming {
			meta.StreamLabel = "true"
		} else {
			meta.StreamLabel = labelFalse
		}
		return meta
	default:
		return meta
	}
}

func safeModel(model string) string {
	model = strings.TrimSpace(model)
	if len(model) > 128 {
		return model[:128]
	}
	return model
}

func modelFamily(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.HasPrefix(model, "claude-fable-5"):
		return "claude-fable-5"
	case strings.HasPrefix(model, "claude-opus-4"):
		return "claude-opus-4"
	case strings.HasPrefix(model, "claude-sonnet-5"):
		return "claude-sonnet-5"
	case strings.HasPrefix(model, "claude-sonnet-4"):
		return "claude-sonnet-4"
	case strings.HasPrefix(model, "claude-haiku-4-5"):
		return "claude-haiku-4-5"
	case strings.HasPrefix(model, "claude-3-7-sonnet"):
		return "claude-3-7-sonnet"
	case strings.HasPrefix(model, "claude-3-5-sonnet"):
		return "claude-3-5-sonnet"
	case strings.HasPrefix(model, "gpt-5"):
		return "gpt-5"
	case model == "":
		return labelUnknown
	default:
		return labelOther
	}
}

func statusClass(status int) string {
	return normalizeStatusClass("", status)
}

func countSSEDataFrames(body []byte) int {
	count := 0
	for line := range bytes.SplitSeq(body, []byte{'\n'}) {
		if bytes.HasPrefix(bytes.TrimSpace(line), []byte("data:")) {
			count++
		}
	}
	return count
}

// randHex returns a random hex string of the given byte count; used to
// synthesize request IDs when upstream didn't set one. Falls back to a
// timestamp on RNG error.
func randHex(nBytes int) string {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}
