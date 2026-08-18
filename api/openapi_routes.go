package api

import (
	"github.com/gofiber/adaptor/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/seed"
	"github.com/papercomputeco/tapes/pkg/storage"
	oas "github.com/papercomputeco/tapes/pkg/tapesoapi"
	"github.com/papercomputeco/tapes/pkg/tapesoapi/oasfiber"
)

// This file is the read API's route table and its OpenAPI description, in one
// place.
//
// Registration and documentation are the same call on purpose. They used to be
// two: routes here, annotations in doc comments above the handlers, joined by a
// source-scanning generator. That arrangement let a route ship with no
// description at all — an entire surface once did — because nothing failed when
// the two halves disagreed. Here a route that is registered is described, and a
// description that names no route does not compile.

// mountV1 registers every documented route on the read API.
func (s *Server) mountV1(router *oasfiber.Router) {
	s.mountHealth(router)
	s.mountSessions(router)
	s.mountTraces(router)
	s.mountSearch(router)
	s.mountAdmin(router)
	s.mountMCP(router)
}

func (s *Server) mountHealth(router *oasfiber.Router) {
	router.Get("/ping", s.handlePing,
		oasfiber.Doc("ping").
			Summary("Health check").
			Description("Returns a simple JSON string confirming that the API server is reachable.").
			Tag("health").
			ContentResponse(200, "pong", "application/json", oas.String()))
}

func (s *Server) mountSessions(router *oasfiber.Router) {
	router.Get("/v1/stats", s.handleStats,
		oasfiber.Doc("getStats").
			Summary("Get aggregate session stats").
			Description("Returns counts plus cost / token / duration / tool-call / completed-count "+
				"totals for the window. The numbers are span-grain trace rollup sums (delta-only usage, "+
				"agent time = sum of trace durations) so they agree with the session and trace views; "+
				"turn_count counts traces. Filter the window with since/until, and narrow every total "+
				"to one user with auth_subject — the same subject the /v1/sessions filter takes, so a "+
				"personal surface can show totals that match the rows beside them.").
			Tag("sessions").
			QueryParam("since", oas.String(oas.Format("date-time")),
				oas.ParamDescription("Only include records at or after this RFC3339 timestamp")).
			QueryParam("until", oas.String(oas.Format("date-time")),
				oas.ParamDescription("Only include records before or at this RFC3339 timestamp")).
			QueryParam("auth_subject", oas.String(),
				oas.ParamDescription("Narrow every total to sessions captured for this gateway-stamped "+
					"JWT subject (exact match). Omitted, the totals are org-wide. A subject with no "+
					"sessions in the window aggregates to zeros, not an error")).
			JSONResponse(200, "Aggregate stats for the window", s.schema(StatsResponse{})).
			JSONResponse(400, "Invalid query parameters", s.errorSchema()).
			JSONResponse(500, "Failed to compute stats", s.errorSchema()))

	router.Get("/v1/sessions", s.handleListSessions,
		oasfiber.Doc("listSessions").
			Summary("List sessions").
			Description("Returns one row per harness session from the sessions table, "+
				"cursor-paginated. Default order is last_active (last_seen_at) desc; override with the "+
				"sort and direction query params.").
			Tag("sessions").
			QueryParam("limit", oas.Integer(oas.Minimum(1)),
				oas.ParamDescription("Maximum number of sessions to return (default 50, max 200)")).
			QueryParam("cursor", oas.String(),
				oas.ParamDescription("Opaque pagination cursor returned by a previous response")).
			QueryParam("sort", oas.String(),
				oas.ParamDescription("Sort column: last_active|started_at|turn_count|total_cost_usd|"+
					"total_tokens|duration_ns|derived_status|auth_subject (default last_active)")).
			QueryParam("direction", oas.String(oas.Enum("asc", "desc")),
				oas.ParamDescription("Sort direction: asc|desc (default desc)")).
			QueryParam("since", oas.String(oas.Format("date-time")),
				oas.ParamDescription("Only include sessions with a turn started at or after this "+
					"RFC3339 timestamp (activity window, matches /v1/stats)")).
			QueryParam("until", oas.String(oas.Format("date-time")),
				oas.ParamDescription("Only include sessions with a turn started before this RFC3339 "+
					"timestamp (activity window, matches /v1/stats)")).
			QueryParam("harness_id", oas.String(),
				oas.ParamDescription("Combined with harness_session_id, narrows the filter to the "+
					"single session with this harness id (exact match). Rejected alone (400): a "+
					"harness id names a harness, not a session. Incompatible with cursor, sort, "+
					"direction, since, and until (400); limit is ignored when the filter is active")).
			QueryParam("harness_session_id", oas.String(),
				oas.ParamDescription("Filter to sessions with this harness session id (exact match). "+
					"Alone it matches across all harnesses — the id is unique per harness, so at most "+
					"one row per harness returns, in practice zero or one; with harness_id it is a "+
					"single-harness point lookup. Incompatible with cursor, sort, direction, since, "+
					"and until (400); limit is ignored when the filter is active")).
			QueryParam("auth_subject", oas.String(),
				oas.ParamDescription("Filter the paged list to sessions captured for this "+
					"gateway-stamped JWT subject (exact match; ignored on the harness filter path)")).
			JSONResponse(200, "One page of sessions", s.schema(SessionListResponse{})).
			JSONResponse(400, "Invalid query parameters, a lone harness_id, or cursor, sort, "+
				"direction, since, or until combined with the harness filter", s.errorSchema()).
			JSONResponse(500, "Failed to list sessions", s.errorSchema()).
			JSONResponse(501, "Sessions not supported by this backend", s.errorSchema()))

	router.Get("/v1/sessions/:id/traces", s.handleGetSessionTraces,
		oasfiber.Doc("getSessionTraces").
			Summary("Get a session's trace/span projection").
			Description("Returns the session's user-visible turns as traces with nested spans (llm "+
				"calls, tools, subagents, shadow calls, injected context) and dataflow links. "+
				"Cross-trace links (compaction seams) are at the response top level.").
			Tag("sessions").
			PathParam("id", oas.String(), oas.ParamDescription("Session id (UUID)")).
			QueryParam("payload", oas.String(oas.Enum("full", "preview")),
				oas.ParamDescription("Span payload mode: full (default) or preview (strings truncated; "+
					"fetch the span endpoint for full payloads)")).
			JSONResponse(200, "The session's traces and spans", s.schema(SessionTracesResponse{})).
			JSONResponse(400, "Missing or malformed id", s.errorSchema()).
			JSONResponse(404, "Session not found", s.errorSchema()).
			JSONResponse(500, "Failed to load session", s.errorSchema()).
			JSONResponse(501, "Span traces not supported by this backend", s.errorSchema()))

	router.Get("/v1/sessions/:id/raw_turns", s.handleListSessionRawTurns,
		oasfiber.Doc("listRawTurns").
			Summary("List a session's raw capture log (operator)").
			Description("The raw layer's wire log: one row per captured call or transcript push, "+
				"identity and sizes only. `source` distinguishes what crossed the wire from what the "+
				"harness pushed as its own account.").
			Tag("sessions").
			PathParam("id", oas.String(), oas.ParamDescription("Session id (UUID)")).
			JSONResponse(200, "The session's raw turn headers", s.schema(RawTurnListResponse{})).
			JSONResponse(400, "Missing or malformed id", s.errorSchema()).
			JSONResponse(404, "Session not found", s.errorSchema()).
			JSONResponse(500, "Failed to list raw turns", s.errorSchema()).
			JSONResponse(501, "Raw turns not supported by this backend", s.errorSchema()))

	router.Get("/v1/sessions/:id", s.handleGetSession,
		oasfiber.Doc("getSession").
			Summary("Get a session").
			Description("Returns a single session record. The conversation content lives on the span "+
				"model: GET /v1/sessions/{id}/traces.").
			Tag("sessions").
			PathParam("id", oas.String(), oas.ParamDescription("Session id (UUID)")).
			JSONResponse(200, "The session", s.schema(SessionDetailResponse{})).
			JSONResponse(400, "Missing or malformed id", s.errorSchema()).
			JSONResponse(404, "Session not found", s.errorSchema()).
			JSONResponse(500, "Failed to load session", s.errorSchema()).
			JSONResponse(501, "Sessions not supported by this backend", s.errorSchema()))

	router.Delete("/v1/sessions/:id", s.handleDeleteSession,
		oasfiber.Doc("deleteSession").
			Summary("Delete a session").
			Description("Permanently deletes a session and its subtree: subagent child sessions and "+
				"their derived traces/spans cascade with it. The immutable raw_turns capture log is "+
				"left intact.").
			Tag("sessions").
			PathParam("id", oas.String(), oas.ParamDescription("Session id (UUID)")).
			EmptyResponse(204, "Session deleted").
			JSONResponse(400, "Missing or malformed id", s.errorSchema()).
			JSONResponse(404, "Session not found", s.errorSchema()).
			JSONResponse(500, "Failed to delete session", s.errorSchema()).
			JSONResponse(501, "Sessions not supported by this backend", s.errorSchema()))

	router.Patch("/v1/sessions/:id", s.handleUpdateSession,
		oasfiber.Doc("updateSession").
			Summary("Update a session's title").
			Description("Updates the user-editable display_name. An absent field is a 400; null or "+
				"empty (after trim) clears back to the auto-derived title. Length is bounded to 200 "+
				"characters.").
			Tag("sessions").
			PathParam("id", oas.String(), oas.ParamDescription("Session id (UUID)")).
			JSONBody("Update request", s.schema(sessionUpdateRequest{})).
			JSONResponse(200, "The updated session", s.schema(SessionDetailResponse{})).
			JSONResponse(400, "Missing/malformed id, missing display_name field, or display_name "+
				"exceeds 200 characters", s.errorSchema()).
			JSONResponse(404, "Session not found or not in caller's org", s.errorSchema()).
			JSONResponse(500, "Failed to update session", s.errorSchema()).
			JSONResponse(501, "Sessions not supported by this backend", s.errorSchema()))
}

func (s *Server) mountTraces(router *oasfiber.Router) {
	router.Get("/v1/traces", s.handleListTraceSummaries,
		oasfiber.Doc("listTraces").
			Summary("List a session's traces (summaries)").
			Description("Returns turn headers for a session — no span payloads. Fetch GET "+
				"/v1/traces/{trace_id} per turn for spans and links.").
			Tag("traces").
			QueryParam("session_id", oas.String(), oas.ParamRequired(),
				oas.ParamDescription("Session id (UUID)")).
			JSONResponse(200, "The session's trace summaries", s.schema(TraceListResponse{})).
			JSONResponse(400, "Missing or malformed session_id", s.errorSchema()).
			JSONResponse(500, "Failed to list traces", s.errorSchema()).
			JSONResponse(501, "Traces not supported by this backend", s.errorSchema()))

	router.Get("/v1/traces/:trace_id/spans/:span_id", s.handleGetSpan,
		oasfiber.Doc("getSpan").
			Summary("Get one span with full payloads").
			Description("The payload drill-in: one span's complete input/output content.").
			Tag("traces").
			PathParam("trace_id", oas.String(), oas.ParamDescription("Trace id")).
			PathParam("span_id", oas.String(), oas.ParamDescription("Span id")).
			JSONResponse(200, "The span", s.schema(SpanItem{})).
			JSONResponse(404, "Span not found", s.errorSchema()).
			JSONResponse(500, "Failed to load span", s.errorSchema()).
			JSONResponse(501, "Spans not supported by this backend", s.errorSchema()))

	router.Get("/v1/traces/:trace_id", s.handleGetTrace,
		oasfiber.Doc("getTrace").
			Summary("Get one trace with spans and links").
			Description("Returns one user-visible turn: its spans nested by parent_span_id and its "+
				"dataflow links (links touching other traces included).").
			Tag("traces").
			PathParam("trace_id", oas.String(), oas.ParamDescription("Trace id")).
			QueryParam("payload", oas.String(oas.Enum("full", "preview")),
				oas.ParamDescription("Span payload mode: full (default) or preview (strings truncated; "+
					"fetch the span endpoint for full payloads)")).
			JSONResponse(200, "The trace", s.schema(TraceDetail{})).
			JSONResponse(404, "Trace not found", s.errorSchema()).
			JSONResponse(500, "Failed to load trace", s.errorSchema()).
			JSONResponse(501, "Traces not supported by this backend", s.errorSchema()))
}

func (s *Server) mountSearch(router *oasfiber.Router) {
	router.Get("/v1/search/spans", s.handleSearchSpansEndpoint,
		oasfiber.Doc("searchSpans").
			Summary("Semantic search over span embeddings").
			Description("Embeds the query text and runs vector similarity over the embedded span "+
				"projection (main llm spans, delta-only content). Each hit carries span, trace, and "+
				"turn context.").
			Tag("search").
			QueryParam("query", oas.String(), oas.ParamRequired(),
				oas.ParamDescription("Search query")).
			QueryParam("top_k", oas.Integer(oas.Minimum(1), oas.Default(5)),
				oas.ParamDescription("Maximum number of results to return")).
			JSONResponse(200, "Search hits", s.schema(SpanSearchOutput{})).
			JSONResponse(400, "Missing or invalid query parameters", s.errorSchema()).
			JSONResponse(500, "Search execution failed", s.errorSchema()).
			JSONResponse(503, "Span search is not configured or not yet initialized", s.errorSchema()))
}

func (s *Server) mountAdmin(router *oasfiber.Router) {
	router.Post("/v1/admin/seed/demo", s.handleSeedDemo,
		oasfiber.Doc("seedDemo").
			Summary("Seed demo sessions (operator)").
			Description("Replays the bundled demo capture corpora through the ingest write path into "+
				"the caller's org, then derives the seeded sessions. Idempotent: raw-turn dedup makes "+
				"repeat seeds no-ops.").
			Tag("admin").
			OptionalJSONBody("Seed options (overwrite is no longer supported)",
				s.schema(seedDemoRequest{})).
			JSONResponse(200, "What was seeded", s.schema(seed.Result{})).
			JSONResponse(400, "Invalid payload or unsupported option", s.errorSchema()).
			JSONResponse(500, "Seeding failed", s.errorSchema()).
			JSONResponse(501, "Driver does not host the raw-turn layer", s.errorSchema()))

	router.Post("/v1/admin/derive/run", s.handleDeriveRun,
		oasfiber.Doc("runDerive").
			Summary("Re-derive the span projection (operator)").
			Description("Rebuilds traces, spans, links, and session rollups for every org from the "+
				"immutable raw-turn store. Idempotent: re-running reproduces the same projection and "+
				"prunes rows the current derive no longer emits.\n\nThis is how a projection or "+
				"classifier change reaches already-captured data — it re-derives rather than "+
				"re-captures. Cost scales with the raw layer, so it is an operator lever, not a "+
				"request-path call.").
			Tag("admin").
			JSONResponse(200, "Per-org derive reports", s.schema(deriveRunResponse{})).
			JSONResponse(500, "Derive failed", s.errorSchema()).
			JSONResponse(501, "Driver does not host the raw-turn layer", s.errorSchema()))

	router.Post("/v1/admin/raw-turns/attribution-repair", s.handleRawTurnAttributionRepair,
		oasfiber.Doc("repairRawTurnAttribution").
			Summary("Repair raw-turn attribution (operator)").
			Description("Records an audited, append-only attribution correction without modifying "+
				"raw_turns, then synchronously re-derives the previous and effective sessions. "+
				"Select exactly one row by raw_turn_id or paper_proxy_request_id.").
			Tag("admin").
			JSONBody("Attribution repair", s.schema(storage.RawTurnAttributionRepairRequest{})).
			JSONResponse(200, "Repair applied; source_cleanup_pending discloses a cosmetic "+
				"leftover source session row that nothing retries automatically",
				s.schema(storage.RawTurnAttributionRepairResult{})).
			JSONResponse(202, "Correction recorded; projections_pending lists sessions the "+
				"derive worker will converge",
				s.schema(storage.RawTurnAttributionRepairResult{})).
			JSONResponse(400, "Invalid payload or replacement attribution", s.errorSchema()).
			JSONResponse(404, "Raw turn not found", s.errorSchema()).
			JSONResponse(409, "Correlation selector is ambiguous", s.errorSchema()).
			JSONResponse(500, "Repair failed", s.errorSchema()).
			JSONResponse(501, "Driver does not support attribution repair", s.errorSchema()))
}

// mountMCP registers the streamable MCP transport.
//
// One handler serves three verbs, so the route is mounted with All and each
// verb is described separately: app.All also mounts verbs this transport does
// not implement, and publishing those would hand a generated client operations
// that cannot work.
func (s *Server) mountMCP(router *oasfiber.Router) {
	handler := adaptor.HTTPHandler(s.mcpServer.Handler())

	router.All("/v1/mcp", handler,
		oasfiber.DocFor("POST", "invokeMcp").
			Summary("Invoke the streamable MCP endpoint").
			Description("Sends a JSON-RPC 2.0 request to the stateless Model Context Protocol "+
				"endpoint mounted at /v1/mcp.\n\nTypical calls include initialize, tools/list, and "+
				"tools/call. The server exposes tools advertised by installed cassettes, plus the "+
				"legacy core search tool while search is configured in core.").
			Tag("mcp").
			JSONBody("JSON-RPC 2.0 request", s.schema(MCPRequest{})).
			JSONResponse(200, "JSON-RPC 2.0 response", s.schema(MCPResponse{})).
			JSONResponse(400, "Invalid JSON-RPC request", s.schema(MCPResponse{})).
			JSONResponse(500, "Server-side MCP error", s.schema(MCPResponse{})),

		oasfiber.DocFor("GET", "openMcpStream").
			Summary("Open an MCP event stream").
			Description("Opens the streamable MCP endpoint for server-sent events. Stateless clients "+
				"can use this to receive streamed MCP messages.").
			Tag("mcp").
			ContentResponse(200, "Server-sent event stream", "text/event-stream", oas.String()),

		oasfiber.DocFor("DELETE", "closeMcpSession").
			Summary("Close an MCP session").
			Description("Requests termination of a streamable MCP session when a client is using "+
				"session-oriented transport semantics.").
			Tag("mcp").
			JSONResponse(200, "Session closed", s.schema(MCPResponse{})).
			JSONResponse(400, "Invalid request", s.schema(MCPResponse{})))
}

// schema reflects a Go type into this server's OpenAPI component registry.
func (s *Server) schema(value any) *oas.Schema { return s.openapi.Schema(value) }

// errorSchema is the shared failure body. Every non-2xx on this surface returns
// it, so it is worth one name rather than a repeated literal.
func (s *Server) errorSchema() *oas.Schema { return s.openapi.Schema(llm.ErrorResponse{}) }
