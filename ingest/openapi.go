package ingest

import (
	"context"
	"log/slog"
	"strings"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	oas "github.com/papercomputeco/tapes/pkg/tapesoapi"
	"github.com/papercomputeco/tapes/pkg/tapesoapi/oasfiber"
)

// This file is the ingest write surface's route table and its OpenAPI
// description.
//
// The contract matters more than its three endpoints suggest: every capture
// path — tapes-extproc, tapesctl, paperd — writes this envelope, and "identical
// fidelity whichever path captured it" is unenforceable while the shape those
// paths must agree on lives only in Go structs.

// ingestInfo describes the ingest write surface.
//
// It is a SEPARATE contract from the read API's, published at this server's own
// /openapi. The two are different servers on different ports with different
// trust models: the read
// API is reached by clients through the edge gateway, while ingest trusts the
// org in its payload envelope because its only legitimate caller is an
// in-cluster capture adapter. Publishing them as one document would imply a
// surface that is reachable and safe to call from outside, which this one is
// not.
func ingestInfo() oas.Info {
	return oas.Info{
		Title:   "Tapes Ingest API",
		Version: "1.0",
		Description: strings.Join([]string{
			"Write surface for captured LLM turns and harness transcripts.",
			"",
			"Capture adapters (tapes-extproc, tapesctl, paperd) POST completed turns here; " +
				"the server appends them to the immutable raw-turn log and the deriver projects " +
				"them into sessions, traces, and spans.",
			"",
			"**Not internet-facing.** These endpoints trust the org identity in the request " +
				"envelope, so the only legitimate caller is an in-cluster capture adapter. " +
				"Exposing them through an edge gateway would let any JWT holder write turns " +
				"under an arbitrary org.",
		}, "\n"),
	}
}

// NewOpenAPIParser returns a parser configured for the ingest contract.
//
// docs is non-nil only under `tapes dev openapi --docs-root`, which supplies
// the repository's doc comments so the compiled document carries the prose that
// documents each envelope field in the source. A deployed binary has no source
// tree, so what it serves describes every field's shape but not its meaning.
func NewOpenAPIParser(docs oas.TypeDocs) *oas.Parser {
	return oas.NewParser(
		oas.WithInfo(ingestInfo()),
		oas.WithSchemaReflector(oas.NewReflector(oas.WithDocs(docs))),
	)
}

// CompileOpenAPI builds the ingest write surface's published contract.
//
// Like the read API's, it constructs a server purely to make it register its
// routes, then compiles what that registration produced. The server is never
// started; it is given no driver and no worker pool it will use.
func CompileOpenAPI(ctx context.Context, docs oas.TypeDocs) (*oas.CompiledDoc, error) {
	server, err := newServer( //nolint:contextcheck // worker pool jobs are detached by design
		Config{ListenAddr: ":0"},
		nil,
		slog.New(slog.DiscardHandler),
		docs,
	)
	if err != nil {
		return nil, err
	}

	return server.openapi.Compile(ctx, oas.WithTarget(oas.V30))
}

// OpenAPIParser returns the live parser this server's routes registered into.
func (s *Server) OpenAPIParser() *oas.Parser { return s.openapi }

// handleOpenAPI serves this surface's contract, compiled from its own routes.
//
// A capture adapter reads this to learn the envelope it must write, so it is
// worth being blunt about what it can and cannot tell that adapter: the shapes
// and the required fields are exact, because they are reflected from the same
// Go types the handler decodes into. Per-field prose is present only if this
// binary was handed a source tree, which a deployed one is not.
func (s *Server) handleOpenAPI(c *fiber.Ctx) error {
	s.contractOnce.Do(func() {
		compiled, err := s.openapi.Compile(c.UserContext(), oas.WithTarget(oas.V30))
		if err != nil {
			s.contractErr = err

			return
		}
		s.contract = compiled.JSON()
	})
	if s.contractErr != nil {
		// A surface that cannot describe itself is a defect in this package, not
		// in the request — every input to the compile is compiled-in.
		s.logger.Error("could not compile the ingest contract", "error", s.contractErr)

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "openapi_compile_failed",
			"message": s.contractErr.Error(),
		})
	}

	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)

	return c.Send(s.contract)
}

// mountRoutes registers every documented route on the ingest server.
func (s *Server) mountRoutes(router *oasfiber.Router) {
	router.Get("/ping", s.handlePing,
		oasfiber.Doc("ingestPing").
			Summary("Liveness probe").
			Description("Reports that the ingest server is up. Does not check the database — a 200 "+
				"here means the process is serving, not that a write would succeed.").
			Tag("health").
			JSONResponse(200, "The server is serving", s.schema(pingResponse{})))

	router.Post("/v1/ingest", s.handleIngest,
		oasfiber.Doc("ingestTurn").
			Summary("Ingest one captured turn").
			Description("Appends one completed LLM turn to the immutable raw-turn log. The raw "+
				"envelope is persisted BEFORE parsing, so a turn that fails provider parsing is still "+
				"captured and a later parser fix re-derives it rather than needing a re-capture."+
				"\n\nIdempotent when the adapter supplies meta.request_id: a retried POST of the same "+
				"captured turn dedupes at the raw layer instead of appending twice."+
				"\n\nThe response may carry a reduced response, the verbatim upstream bytes "+
				"(raw_response), or both. Raw-only is reduced server-side with the shared reducers, "+
				"which is what lets two capture paths produce identical rows for identical traffic.").
			Tag("ingest").
			JSONBody("Captured turn", s.schema(TurnPayload{})).
			JSONResponse(202, "Captured and queued for derivation", s.schema(ingestAcceptedResponse{})).
			JSONResponse(400, "Malformed envelope or invalid session block", s.errorSchema()).
			JSONResponse(413, "Body exceeds the ingest size limit", s.errorSchema()).
			JSONResponse(422, "Well-formed but unprocessable (e.g. unknown provider)", s.errorSchema()).
			JSONResponse(502, "A downstream dependency failed", s.errorSchema()))

	router.Post("/v1/ingest/transcript", s.handleTranscriptIngest,
		oasfiber.Doc("ingestTranscript").
			Summary("Ingest one harness transcript file").
			Description("Appends one harness transcript — the main session file or a single "+
				"subagent's — to the raw layer verbatim. The deriver reconciles the records against "+
				"the wire capture to recover the causal and fork skeleton; no node-path processing "+
				"happens here."+
				"\n\nIdempotent per content version: the dedup key includes a content hash, so "+
				"re-uploading an unchanged file is a no-op (deduped=true) while a transcript that has "+
				"grown appends a new version. The deriver reads the latest version per session and "+
				"agent.").
			Tag("ingest").
			JSONBody("Transcript file and its harness metadata", s.schema(TranscriptPayload{})).
			JSONResponse(202, "Stored, or already present as this content version",
				s.schema(transcriptAcceptedResponse{})).
			JSONResponse(400, "Malformed body, invalid session block, or records not a JSON array",
				s.errorSchema()).
			JSONResponse(413, "Body exceeds the ingest size limit", s.errorSchema()).
			JSONResponse(500, "Persisting the transcript failed", s.errorSchema()).
			JSONResponse(501, "Driver does not host the raw-turn layer", s.errorSchema()))
}

// schema reflects a Go type into this server's OpenAPI component registry.
func (s *Server) schema(value any) *oas.Schema { return s.openapi.Schema(value) }

// errorSchema is the shared failure body for this surface.
func (s *Server) errorSchema() *oas.Schema { return s.openapi.Schema(llm.ErrorResponse{}) }
