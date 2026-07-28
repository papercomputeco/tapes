package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/proxy/worker"
)

// Ingest error classes. Each maps to a distinct HTTP status so operators can
// tell malformed envelopes from unknown providers from downstream outages
// without tailing logs.
var (
	// ErrEnvelope means the POST body could not be decoded as a TurnPayload.
	// Returned as 400 Bad Request.
	ErrEnvelope = errors.New("invalid envelope")

	// ErrUnprocessable covers validation / parse failures inside a well-formed
	// envelope: unknown provider, unparseable provider-specific request /
	// response body, etc. Returned as 422 Unprocessable Entity.
	ErrUnprocessable = errors.New("unprocessable turn")

	// ErrDownstream covers failures that originate below the handler: worker
	// pool saturation, DAG write errors, storage unavailability. Returned as
	// 502 Bad Gateway.
	ErrDownstream = errors.New("downstream failure")
)

// Server-trusted identity headers, populated by the upstream gateway
// from validated JWT claims. This is the same contract the wire-capture
// path consumes (tapes-extproc's internal/headers package reads the
// identical names into the session envelope): clients are not permitted
// to send these themselves, and the gateway is responsible for
// stripping inbound values so only edge-verified identity reaches the
// handler. When the gateway is not configured to populate them, the
// headers are absent and the payload envelope's own identity fields
// stand.
const (
	// HeaderPaperAuthOrgID carries the verified org claim.
	HeaderPaperAuthOrgID = "x-paper-auth-org-id"

	// HeaderPaperAuthSubject carries the verified `sub` claim.
	HeaderPaperAuthSubject = "x-paper-auth-subject"
)

// TurnPayload is the ingest request body for a single completed conversation turn.
// It carries the raw provider request plus an already-reduced response.
// Capture adapters such as tapes-extproc own protocol-specific stream reduction;
// ingest owns request parsing, validation, and durable storage.
type TurnPayload struct {
	// Provider type: "openai", "anthropic", "ollama"
	Provider string `json:"provider"`

	// AgentName optionally tags the turn (same as X-Tapes-Agent-Name header)
	AgentName string `json:"agent_name,omitempty"`

	// RawRequest is the original request body sent to the LLM provider.
	RawRequest json.RawMessage `json:"request"`

	// Response is the already reduced, provider-agnostic response for the turn.
	Response llm.ChatResponse `json:"response"`

	// RawResponse is the upstream response body exactly as it arrived on the
	// wire, base64-encoded in the JSON envelope. Optional and independent of
	// Response: an adapter may send both (reduction plus the bytes it reduced
	// from), only the reduction (the historical shape), or only the bytes.
	//
	// Raw-only is the interesting case. Reduction is lossy and adapter-
	// specific, so an adapter that ships only the bytes lets ingest perform
	// the reduction with the shared pkg/capture reducers — which is what makes
	// two capture paths produce identical rows for identical upstream traffic
	// instead of two subtly different ones.
	RawResponse []byte `json:"raw_response,omitempty"`

	// RawResponseEncoding is the Content-Encoding of RawResponse ("identity",
	// "gzip", …). Empty means identity. The bytes are stored under this
	// encoding rather than decompressed, so the stored column stays literally
	// what the upstream sent.
	RawResponseEncoding string `json:"raw_response_encoding,omitempty"`

	// Meta is the capture adapter's metadata block. Parsed for the
	// fields ingest promotes (request_id for raw-turn dedup); the
	// verbatim JSON is persisted alongside the raw turn so fields
	// unknown to this build survive.
	Meta TurnMeta `json:"meta"`

	// Session is the optional session-tracking envelope. When present,
	// ingest UPSERTs a `sessions` row keyed by
	// (org_id, harness_id, harness_session_id), resolves the
	// parent_session_id FK (placeholder-inserting when needed), and
	// rolls up turn counters — all in the same transaction as the
	// nodes insert. When absent, ingest treats the turn as
	// harness_id="unknown" and derives a synthetic harness_session_id
	// from the captured turn's Merkle root prefix.
	//
	// The type lives in pkg/sessions to avoid an import cycle
	// (proxy/worker depends on it too).
	Session *sessions.IngestEnvelope `json:"session,omitempty"`
}

// TurnMeta mirrors the capture adapter's meta block (tapes-extproc
// TurnMeta). Every field is optional; adapters that predate a field
// simply omit it. Ingest only reads RequestID directly (raw-turn
// dedup) — the rest ride along verbatim in the raw layer and become
// queryable post-derive.
type TurnMeta struct {
	RequestID   string `json:"request_id,omitempty"`
	ContentType string `json:"content_type,omitempty"`

	// ThreadID is the harness sub-thread id resolved by the capture
	// adapter (extproc headers.ThreadID); "" for main-thread calls.
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

// rawEnvelope is the shadow decode of an ingest body used for the
// immutable raw-turn store: every block is kept as verbatim
// json.RawMessage so persisting it never round-trips through parsed
// structs (which would drop fields this build doesn't know about).
type rawEnvelope struct {
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
	Meta     json.RawMessage `json:"meta"`
	Session  json.RawMessage `json:"session"`
}

// Server is an HTTP server that accepts completed LLM conversation turns
// for async capture to the raw_turns log.
type Server struct {
	config     Config
	driver     storage.Driver
	workerPool *worker.Pool
	logger     *slog.Logger
	server     *fiber.App
	providers  map[string]provider.Provider
	metrics    *Metrics

	// reducers turn verbatim upstream bytes into a canonical response for
	// raw-only payloads. Constructed explicitly and dispatched by provider
	// name, the same way proxy.New does it — pkg/capture deliberately keeps
	// no global registry so import order and init() stay out of the call
	// graph. A provider with no entry simply never gets a server-side
	// reduction; its adapter is expected to send one.
	reducers map[string]capture.Reducer

	// rawStore is the optional immutable raw-capture layer. Non-nil
	// only when the configured driver hosts it (Postgres). When nil,
	// ingest behaves exactly as before the raw layer existed.
	rawStore storage.RawTurnStore
}

// New creates a new ingest Server.
func New(config Config, driver storage.Driver, log *slog.Logger) (*Server, error) {
	providers := make(map[string]provider.Provider)
	for _, name := range provider.SupportedProviders() {
		prov, err := provider.New(name)
		if err != nil {
			return nil, fmt.Errorf("could not create provider %s: %w", name, err)
		}
		providers[name] = prov
	}

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		BodyLimit:             MaxIngestBodyBytes,
	})

	wp, err := worker.NewPool(&worker.Config{
		Driver:  driver,
		Project: config.Project,
		Logger:  log,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create worker pool: %w", err)
	}

	s := &Server{
		config:     config,
		driver:     driver,
		workerPool: wp,
		logger:     log,
		server:     app,
		providers:  providers,
		metrics:    NewMetrics(),
		reducers: map[string]capture.Reducer{
			capture.ProviderAnthropic: capture.NewAnthropicReducer(),
			capture.ProviderOpenAI:    capture.NewOpenAIResponsesReducer(),
		},
	}
	if rawStore, ok := driver.(storage.RawTurnStore); ok {
		s.rawStore = rawStore
	}

	app.Get("/ping", s.handlePing)
	app.Get("/metrics", adaptor.HTTPHandler(s.metrics.Handler()))
	app.Post("/v1/ingest", s.handleIngest)
	app.Post("/v1/ingest/transcript", s.handleTranscriptIngest)

	return s, nil
}

// Metrics exposes the ingest metrics so tests and health checks can scrape
// the registry programmatically.
func (s *Server) Metrics() *Metrics { return s.metrics }

// Run starts the ingest server on the configured address.
func (s *Server) Run() error {
	s.logger.Info("starting ingest server",
		"listen", s.config.ListenAddr,
	)
	return s.server.Listen(s.config.ListenAddr)
}

// RunWithListener starts the ingest server using the provided listener.
func (s *Server) RunWithListener(listener net.Listener) error {
	s.logger.Info("starting ingest server",
		"listen", listener.Addr().String(),
	)
	return s.server.Listener(listener)
}

// Close gracefully shuts down the server and waits for the worker pool to drain.
func (s *Server) Close() error {
	s.workerPool.Close()
	return s.server.Shutdown()
}

// @Summary		Liveness probe
// @ID			ingestPing
// @Description	Reports that the ingest server is up. Does not check the database — a 200 here means the process is serving, not that a write would succeed.
// @Tags			health
// @Produce		json
// @Success		200	{object}	pingResponse
// @Router			/ping [get]
func (s *Server) handlePing(c *fiber.Ctx) error {
	return c.JSON(pingResponse{Status: "ok"})
}

// @Summary		Ingest one captured turn
// @ID			ingestTurn
// @Description	Appends one completed LLM turn to the immutable raw-turn log. The raw envelope is persisted BEFORE parsing, so a turn that fails provider parsing is still captured and a later parser fix re-derives it rather than needing a re-capture.
// @Description
// @Description	Idempotent when the adapter supplies meta.request_id: a retried POST of the same captured turn dedupes at the raw layer instead of appending twice.
// @Description
// @Description	The response may carry a reduced response, the verbatim upstream bytes (raw_response), or both. Raw-only is reduced server-side with the shared reducers, which is what lets two capture paths produce identical rows for identical traffic.
// @Tags			ingest
// @Accept			json
// @Produce		json
// @Param			request	body		TurnPayload	true	"Captured turn"
// @Success		202		{object}	ingestAcceptedResponse	"Captured and queued for derivation"
// @Failure		400		{object}	llm.ErrorResponse		"Malformed envelope or invalid session block"
// @Failure		422		{object}	llm.ErrorResponse		"Well-formed but unprocessable (e.g. unknown provider)"
// @Failure		502		{object}	llm.ErrorResponse		"A downstream dependency failed"
// @Router			/v1/ingest [post]
func (s *Server) handleIngest(c *fiber.Ctx) error {
	bodySize := len(c.Body())

	var payload TurnPayload
	if err := c.BodyParser(&payload); err != nil {
		s.logger.Warn("ingest envelope rejected",
			"reason", "envelope",
			"error", err,
			"bytes", bodySize,
		)
		s.metrics.ObserveWrite("", ResultRejectEnv, bodySize)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: %s", ErrEnvelope, err),
		})
	}

	if err := payload.Session.Validate(); err != nil {
		s.logger.Warn("ingest envelope rejected",
			"reason", "session",
			"error", err,
			"bytes", bodySize,
		)
		s.metrics.ObserveWrite("", ResultRejectEnv, bodySize)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: %s", ErrEnvelope, err),
		})
	}

	// A raw-only payload carries the verbatim upstream bytes and no reduction.
	// Reduce it here with the shared reducers so the stored row does not depend
	// on which capture path produced it. This runs before the raw-layer write
	// so the reduction lands on the same row as the bytes it came from, and
	// before processTurn so the derived path sees a populated response.
	reduced := s.reduceRawOnly(c.Context(), &payload)

	// Persist the immutable raw envelope BEFORE parsing: a turn that
	// fails provider parsing (422) is still captured, so a future
	// parser fix re-derives it instead of needing a re-capture.
	if s.rawStore != nil {
		var raw rawEnvelope
		if err := json.Unmarshal(c.Body(), &raw); err == nil {
			if len(reduced) > 0 {
				raw.Response = reduced
			}
			s.persistRawTurn(c.Context(), &payload, raw)
		}
	}

	start := time.Now()
	if err := s.processTurn(&payload); err != nil {
		s.recordProcessTurnError(payload.Provider, err, bodySize)
		return s.writeProcessTurnError(c, err)
	}
	s.metrics.ObserveDAGLatency(payload.Provider, time.Since(start).Seconds())
	s.metrics.ObserveWrite(payload.Provider, ResultAccepted, bodySize)

	return c.Status(fiber.StatusAccepted).JSON(ingestAcceptedResponse{Status: "accepted"})
}

// TranscriptPayload is the ingest body for one harness transcript file
// — the main session transcript or one subagent's. The records land in
// the immutable raw layer verbatim (source: transcript); the deriver
// reconciles them against the wire capture to recover the causal/fork
// skeleton. No node-path processing happens at ingest time.
type TranscriptPayload struct {
	// Session identifies the harness session the transcript belongs to.
	Session *sessions.IngestEnvelope `json:"session"`

	// AgentID is empty for the main transcript, or the subagent id for
	// subagents/agent-<id>.jsonl files.
	AgentID string `json:"agent_id,omitempty"`

	// AgentType / Description / ToolUseID mirror the harness's
	// subagent meta.json: ToolUseID is the Task tool_use that forked
	// this agent — the causal fork edge the deriver attaches.
	AgentType   string `json:"agent_type,omitempty"`
	Description string `json:"description,omitempty"`
	ToolUseID   string `json:"tool_use_id,omitempty"`

	// Records is the transcript's JSONL content as a JSON array,
	// verbatim.
	Records json.RawMessage `json:"records"`
}

// transcriptMeta is the meta block stored alongside a transcript raw
// row so the deriver can route and attribute it without decoding the
// records.
type transcriptMeta struct {
	Transcript  bool   `json:"transcript"`
	AgentID     string `json:"agent_id,omitempty"`
	AgentType   string `json:"agent_type,omitempty"`
	Description string `json:"description,omitempty"`
	ToolUseID   string `json:"tool_use_id,omitempty"`
	Records     int    `json:"records"`
}

// transcriptWriteProvider labels transcript-sourced writes on the shared
// tapes_ingest_writes_total counter. The counter's "provider" dimension is
// a wire-capture notion; a transcript has no LLM provider, so it carries
// this sentinel instead of falling into the "unknown" bucket it would
// otherwise share with malformed wire turns. Dashboards select
// provider="transcript" to see this path's health in isolation.
const transcriptWriteProvider = "transcript"

// handleTranscriptIngest appends one transcript file to the raw layer.
// Idempotent per content version: the dedup key includes a content
// hash, so re-uploading an unchanged file is a no-op while a grown
// transcript (session continued) appends a new version — append-only,
// like everything in the raw layer. The deriver reads the latest
// version per (session, agent).
//
//	@Summary		Ingest one harness transcript file
//	@ID			ingestTranscript
//	@Description	Appends one harness transcript — the main session file or a single subagent's — to the raw layer verbatim. The deriver reconciles the records against the wire capture to recover the causal and fork skeleton; no node-path processing happens here.
//	@Description
//	@Description	Idempotent per content version: the dedup key includes a content hash, so re-uploading an unchanged file is a no-op (deduped=true) while a transcript that has grown appends a new version. The deriver reads the latest version per session and agent.
//	@Tags			ingest
//	@Accept			json
//	@Produce		json
//	@Param			request	body		TranscriptPayload	true	"Transcript file and its harness metadata"
//	@Success		202		{object}	transcriptAcceptedResponse	"Stored, or already present as this content version"
//	@Failure		400		{object}	llm.ErrorResponse			"Malformed body, invalid session block, or records not a JSON array"
//	@Failure		500		{object}	llm.ErrorResponse			"Persisting the transcript failed"
//	@Failure		501		{object}	llm.ErrorResponse			"Driver does not host the raw-turn layer"
//	@Router			/v1/ingest/transcript [post]
func (s *Server) handleTranscriptIngest(c *fiber.Ctx) error {
	if s.rawStore == nil {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{
			Error: "transcript ingest requires the raw-turn layer (Postgres driver)",
		})
	}

	bodySize := len(c.Body())

	var payload TranscriptPayload
	if err := c.BodyParser(&payload); err != nil {
		s.metrics.ObserveWrite(transcriptWriteProvider, ResultRejectEnv, bodySize)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: %s", ErrEnvelope, err),
		})
	}
	resolveGatewayIdentity(c, payload.Session)
	if err := payload.Session.Validate(); err != nil {
		s.metrics.ObserveWrite(transcriptWriteProvider, ResultRejectEnv, bodySize)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: %s", ErrEnvelope, err),
		})
	}
	if payload.Session == nil || payload.Session.HarnessSessionID == "" {
		s.metrics.ObserveWrite(transcriptWriteProvider, ResultRejectEnv, bodySize)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: transcript ingest requires session.harness_session_id", ErrEnvelope),
		})
	}
	var records []json.RawMessage
	if err := json.Unmarshal(payload.Records, &records); err != nil {
		s.metrics.ObserveWrite(transcriptWriteProvider, ResultRejectEnv, bodySize)
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: records must be a JSON array: %s", ErrEnvelope, err),
		})
	}

	agentKey := payload.AgentID
	if agentKey == "" {
		agentKey = "main"
	}
	sum := sha256.Sum256(payload.Records)
	requestID := fmt.Sprintf("transcript:%s:%s:%s",
		payload.Session.HarnessSessionID, agentKey, hex.EncodeToString(sum[:8]))

	meta, err := json.Marshal(transcriptMeta{
		Transcript:  true,
		AgentID:     payload.AgentID,
		AgentType:   payload.AgentType,
		Description: payload.Description,
		ToolUseID:   payload.ToolUseID,
		Records:     len(records),
	})
	if err != nil {
		s.metrics.ObserveWrite(transcriptWriteProvider, ResultInternalErr, bodySize)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	sessionJSON, err := json.Marshal(payload.Session)
	if err != nil {
		s.metrics.ObserveWrite(transcriptWriteProvider, ResultInternalErr, bodySize)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	inserted, err := s.rawStore.PutRawTurn(c.Context(), storage.RawTurnRecord{
		OrgID:            payload.Session.OrgID,
		Source:           storage.RawTurnSourceTranscript,
		HarnessID:        payload.Session.HarnessIDOrUnknown(),
		HarnessSessionID: payload.Session.HarnessSessionID,
		RequestID:        requestID,
		RawRequest:       payload.Records,
		Meta:             meta,
		SessionEnvelope:  sessionJSON,
	})
	if err != nil {
		// A content-level rejection (invalid Unicode/bytes Postgres JSONB
		// refuses) is the client's malformed payload, not a storage outage:
		// return 422 so it stops reading as a gateway fault, and retrying the
		// identical bytes will never succeed. Everything else stays a 502.
		if errors.Is(err, storage.ErrInvalidContent) {
			s.logger.Warn("transcript ingest rejected: unstorable content", "error", err)
			s.metrics.ObserveWrite(transcriptWriteProvider, ResultRejectParse, bodySize)
			return c.Status(fiber.StatusUnprocessableEntity).JSON(llm.ErrorResponse{
				Error: fmt.Sprintf("%s: %v", ErrUnprocessable, err),
			})
		}
		s.logger.Error("transcript ingest failed", "error", err)
		s.metrics.ObserveWrite(transcriptWriteProvider, ResultDownstreamErr, bodySize)
		return c.Status(fiber.StatusBadGateway).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("%s: %v", ErrDownstream, err),
		})
	}
	s.metrics.ObserveWrite(transcriptWriteProvider, ResultAccepted, bodySize)
	return c.Status(fiber.StatusAccepted).JSON(transcriptAcceptedResponse{
		Status:  "accepted",
		Deduped: !inserted,
		Records: len(records),
		AgentID: payload.AgentID,
	})
}

// resolveGatewayIdentity overrides the envelope's identity fields with
// the server-trusted gateway headers when present. The transcript
// client (paperd) cannot fill org_id itself — it holds a WorkOS org id,
// not the platform org UUID the store keys on — so the payload value is
// only trusted for direct in-cluster / override callers such as
// `tapes backfill transcripts`. Anything arriving through the gateway
// gets its identity from the edge-verified JWT, exactly like the
// wire-capture path (tapes-extproc reads the same headers into the
// session envelope at capture time). The override runs BEFORE envelope
// validation so a malformed gateway-supplied org rejects loudly at the
// HTTP boundary instead of corrupting attribution downstream.
func resolveGatewayIdentity(c *fiber.Ctx, session *sessions.IngestEnvelope) {
	if session == nil {
		return
	}
	if org := c.Get(HeaderPaperAuthOrgID); org != "" {
		session.OrgID = org
	}
	if sub := c.Get(HeaderPaperAuthSubject); sub != "" {
		session.AuthSubject = sub
	}
}

// persistRawTurn appends one captured turn to the immutable raw layer.
// Failures are logged, never propagated: the raw layer must not take
// down the node-ingest path, and a Postgres-level outage will surface
// through processTurn anyway.
// reduceRawOnly reduces a raw-only payload in place and returns the reduced
// response as JSON for the raw layer, or nil when nothing was reduced.
//
// It is a no-op unless the payload carried verbatim bytes AND no reduced
// response. An adapter that already reduced keeps its reduction: it consumed
// the live stream and may have seen framing the stored bytes no longer show,
// so re-reducing here could only lose information, never add it.
//
// A failure to reduce is not a failure to ingest: the raw bytes still land, so
// the information needed to recover the turn is kept rather than discarded.
//
// Recovery is NOT automatic, and nothing in the current tree performs it. The
// derive read path (GetRawTurn) deliberately does not select raw_response —
// the deriver reads the reduced `response` column, and pulling the verbatim
// bytes on every derive read would move megabytes per turn to be discarded.
// So a turn whose reduction failed here persists with an empty reduction, and
// re-deriving it reads that same empty reduction: fixing the reducer does not
// on its own bring the turn back.
//
// Closing that gap means reducing from raw at read time for exactly the turns
// whose reduction is absent — a conditional select plus a reducer on the
// storage read path, keeping pkg/derive a pure function of the rows it is
// handed. Until that exists, treat the stored bytes as
// recoverable in principle and not in practice, and expect a repair pass
// rather than a plain re-derive.
func (s *Server) reduceRawOnly(ctx context.Context, turn *TurnPayload) json.RawMessage {
	if len(turn.RawResponse) == 0 {
		return nil
	}
	if !reducedResponseAbsent(turn.Response) {
		return nil
	}

	reducer, ok := s.reducers[turn.Provider]
	if !ok {
		s.logger.Warn("raw-only turn not reduced: no reducer for provider",
			"provider", turn.Provider,
			"request_id", turn.Meta.RequestID,
		)
		return nil
	}

	body, err := decodeContentEncoding(turn.RawResponse, turn.RawResponseEncoding)
	if err != nil {
		s.logger.Warn("raw-only turn not reduced: decode failed",
			"provider", turn.Provider,
			"request_id", turn.Meta.RequestID,
			"encoding", turn.RawResponseEncoding,
			"error", err,
		)
		return nil
	}

	resp, err := reducer.Reduce(ctx,
		bytes.NewReader(turn.RawRequest),
		bytes.NewReader(body),
		turn.Meta.ContentType,
	)
	if err != nil || resp == nil {
		s.logger.Warn("raw-only turn not reduced: reducer failed",
			"provider", turn.Provider,
			"request_id", turn.Meta.RequestID,
			"content_type", turn.Meta.ContentType,
			"error", err,
		)
		return nil
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return nil
	}
	turn.Response = *resp
	return out
}

// reducedResponseAbsent reports whether a payload carried no reduced response.
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
func reducedResponseAbsent(r llm.ChatResponse) bool {
	return r.Model == "" &&
		r.Message.Role == "" &&
		len(r.Message.Content) == 0 &&
		!r.Done &&
		r.StopReason == "" &&
		r.Usage == nil
}

// decodeContentEncoding returns body decoded per encoding, for handing to a
// reducer. The stored column keeps the ENCODED bytes; only the reduction sees
// the decoded form.
//
// An unrecognized encoding is an error rather than a pass-through: handing
// compressed bytes to a reducer that expects text yields a confusing parse
// failure well away from the actual cause.
func decodeContentEncoding(body []byte, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return body, nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer zr.Close()
		out, err := io.ReadAll(zr)
		if err != nil {
			return nil, fmt.Errorf("gzip decode: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported content-encoding %q", encoding)
	}
}

// MaxRawResponseBytes caps the verbatim response bytes ingest will store on a
// single turn. Beyond it the bytes are dropped and the row is marked
// (raw_response_dropped), rather than the write being refused: the reduced
// response, the raw request, and the session attribution are all still worth
// keeping, and a turn that vanishes entirely is a worse outcome than one whose
// verbatim bytes are known to be missing.
//
// 8 MiB is well above a normal turn — it is a backstop against a pathological
// response, not a working limit. Reduction happens before the cap is applied,
// so an oversize turn still lands with a usable reduced response.
const MaxRawResponseBytes = 8 << 20

// MaxIngestBodyBytes is the request body limit, derived from
// MaxRawResponseBytes rather than chosen independently: raw_response travels
// base64-encoded (4/3 expansion), alongside the raw request, the reduced
// response, and the meta block. Fiber's 4 MiB default would reject a body
// carrying a raw response well under the cap, which would make the cap
// unreachable and its drop-and-mark path dead code — the limit that actually
// bit would be an unrelated framework default, and turns would fail at the
// transport with no fidelity marker recorded anywhere.
const MaxIngestBodyBytes = MaxRawResponseBytes*4/3 + 4<<20

func (s *Server) persistRawTurn(ctx context.Context, turn *TurnPayload, raw rawEnvelope) {
	rawResponse := turn.RawResponse
	dropped := false
	if len(rawResponse) > MaxRawResponseBytes {
		s.logger.Warn("raw response dropped: over cap",
			"provider", turn.Provider,
			"request_id", turn.Meta.RequestID,
			"bytes", len(rawResponse),
			"cap", MaxRawResponseBytes,
		)
		rawResponse, dropped = nil, true
	}

	rec := storage.RawTurnRecord{
		Source:          storage.RawTurnSourceWire,
		Provider:        turn.Provider,
		AgentName:       turn.AgentName,
		RequestID:       turn.Meta.RequestID,
		RawRequest:      raw.Request,
		Response:        raw.Response,
		Meta:            raw.Meta,
		SessionEnvelope: raw.Session,

		RawResponse:         rawResponse,
		RawResponseEncoding: turn.RawResponseEncoding,
		RawResponseDropped:  dropped,
	}
	if turn.Session != nil {
		rec.OrgID = turn.Session.OrgID
		rec.HarnessID = turn.Session.HarnessIDOrUnknown()
		rec.HarnessSessionID = turn.Session.HarnessSessionID
	}
	if _, err := s.rawStore.PutRawTurn(ctx, rec); err != nil {
		s.logger.Error("raw turn persist failed",
			"provider", turn.Provider,
			"request_id", turn.Meta.RequestID,
			"error", err,
		)
	}
}

// recordProcessTurnError maps an internal error to the matching metric label
// without affecting the HTTP response flow. Kept separate from
// writeProcessTurnError so a caller can record the metric without also
// owning the HTTP reply.
func (s *Server) recordProcessTurnError(provider string, err error, bodyBytes int) {
	result := ResultRejectParse
	switch {
	case errors.Is(err, ErrEnvelope):
		result = ResultRejectEnv
	case errors.Is(err, ErrDownstream):
		result = ResultDownstreamErr
	case errors.Is(err, ErrUnprocessable):
		// Unknown provider is the most common and worth a distinct label so
		// operators can see it separately from generic parse failures.
		if _, ok := s.providers[provider]; !ok {
			result = ResultUnknownProv
		}
	}
	s.metrics.ObserveWrite(provider, result, bodyBytes)
}

// writeProcessTurnError maps an error returned by processTurn to the matching
// HTTP status code. This is the mechanism that splits 400 / 422 / 502 so
// operators can distinguish failure classes at a glance.
func (s *Server) writeProcessTurnError(c *fiber.Ctx, err error) error {
	status := fiber.StatusUnprocessableEntity
	reason := "unprocessable"
	switch {
	case errors.Is(err, ErrEnvelope):
		status = fiber.StatusBadRequest
		reason = "envelope"
	case errors.Is(err, ErrDownstream):
		status = fiber.StatusBadGateway
		reason = "downstream"
	}

	// An unprocessable turn is still captured in the raw layer (persisted
	// before parsing), so a parser/deriver fix re-derives it later — it's
	// recoverable, not operator-actionable, and common when replaying a
	// demo corpus. Log it at debug to keep the happy path quiet; metrics
	// still count it. Envelope/downstream failures are genuine — keep warn.
	logArgs := []any{"reason", reason, "status", status, "error", err}
	if reason == "unprocessable" {
		s.logger.Debug("ingest rejected", logArgs...)
	} else {
		s.logger.Warn("ingest rejected", logArgs...)
	}
	return c.Status(status).JSON(llm.ErrorResponse{Error: err.Error()})
}

// validateReducedResponse is a sanity check ontop of a provided llm.ChatResponse
// that ensures the payload to the ingest server is valid.
func validateReducedResponse(resp *llm.ChatResponse) error {
	if resp.Message.Role == "" {
		return errors.New("missing response.message.role")
	}
	if len(resp.Message.Content) == 0 {
		return errors.New("missing response.message.content")
	}
	for i, block := range resp.Message.Content {
		if block.Type == "" {
			return fmt.Errorf("missing response.message.content[%d].type", i)
		}
	}
	return nil
}

// processTurn parses a raw turn payload and enqueues it for async DAG storage.
// Returned errors wrap one of ErrEnvelope / ErrUnprocessable / ErrDownstream so
// the caller can map to an HTTP status without re-parsing the message.
func (s *Server) processTurn(turn *TurnPayload) error {
	prov, ok := s.providers[turn.Provider]
	if !ok {
		return fmt.Errorf("%w: unsupported provider %q (supported: %v)",
			ErrUnprocessable, turn.Provider, provider.SupportedProviders())
	}

	parsedReq, err := prov.ParseRequest(turn.RawRequest)
	if err != nil {
		return fmt.Errorf("%w: cannot parse request: %w", ErrUnprocessable, err)
	}

	parsedResp := turn.Response
	if err := validateReducedResponse(&parsedResp); err != nil {
		return fmt.Errorf("%w: invalid reduced response: %w", ErrUnprocessable, err)
	}

	// Log session attribution at debug. The full envelope isn't logged
	// (auth_subject, harness_metadata are sensitive); we surface just
	// the natural-key tuple so operators can correlate a turn with
	// the eventual sessions row when triaging ingestion issues.
	var (
		sessHarnessID, sessHarnessSessionID, sessOrgID string
	)
	if turn.Session != nil {
		sessHarnessID = turn.Session.HarnessIDOrUnknown()
		sessHarnessSessionID = turn.Session.HarnessSessionID
		sessOrgID = turn.Session.OrgID
	}
	s.logger.Debug("ingesting turn",
		"provider", prov.Name(),
		"agent", turn.AgentName,
		"model", parsedReq.Model,
		"session_org_id", sessOrgID,
		"session_harness_id", sessHarnessID,
		"session_harness_session_id", sessHarnessSessionID,
	)

	if ok := s.workerPool.Enqueue(worker.Job{
		Provider:  prov.Name(),
		AgentName: turn.AgentName,
		ThreadID:  turn.Meta.ThreadID,
		Req:       parsedReq,
		Resp:      &parsedResp,
		Session:   turn.Session,
	}); !ok {
		s.logger.Error("ingest enqueue failed: worker queue full",
			"provider", prov.Name(),
			"agent", turn.AgentName,
			"model", parsedReq.Model,
		)
		// Snapshot depth even on a drop so the gauge reflects saturation.
		s.metrics.SetQueueDepth(s.workerPool.Len())
		return fmt.Errorf("%w: worker queue full", ErrDownstream)
	}

	// Best-effort snapshot of post-enqueue depth. Workers may have already
	// drained the slot we just wrote, so the value can lag actual depth — but
	// over many turns this tracks back-pressure well enough to alert on.
	s.metrics.SetQueueDepth(s.workerPool.Len())
	return nil
}
