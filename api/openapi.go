package api

import (
	"context"
	"log/slog"
	"reflect"
	"strings"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// There is no checked-in contract file and no embedded one. The document is
// compiled from the route registrations on demand, and `GET /openapi` is the
// only place it is published.
//
// A generated file would be a second copy of something the server can already
// state exactly, and a copy can be stale — which is the failure a checked-in
// contract invites: it describes the routes as of the last time someone
// remembered to regenerate it. Compiling per request removes the staleness
// window rather than adding a check for it, and it is what lets /openapi
// describe the cassettes mounted at runtime, which no build-time file could.
//
// The cost is per-field prose: doc comments live in the source, and a running
// binary has no source tree. `tapes dev openapi --docs-root .` compiles the
// fully-documented document from a checkout for the consumers that want it.

// apiInfo describes the read API. It lives here rather than in a doc comment
// because it is data the compiler can check and a generator can read, not prose
// a separate parser has to be taught to find.
func apiInfo() tapesoapi.Info {
	return tapesoapi.Info{
		Title:   "Tapes API",
		Version: "1.0",
		Description: strings.Join([]string{
			"HTTP API for inspecting, querying, and searching stored Tapes sessions.",
			"",
			"The REST surface exposes health checks, session listing and retrieval, " +
				"derived session summaries, aggregate stats, semantic search, skill authoring " +
				"and publishing, operator maintenance endpoints, and a streamable MCP endpoint.",
			"",
			"Not covered here: the ingest write surface, which is a separate server that " +
				"publishes its own contract at its own /openapi.",
		}, "\n"),
	}
}

// NewOpenAPIParser returns a parser configured the way both this server and the
// contract generator need it.
//
// docs may be nil, and is nil in the running server: a deployed binary has no
// source tree to read prose out of. It is non-nil under `tapes dev openapi`,
// which points the doc reader at a checkout so the compiled document carries
// the prose sitting next to each field in the source.
func NewOpenAPIParser(docs tapesoapi.TypeDocs) *tapesoapi.Parser {
	return tapesoapi.NewParser(
		tapesoapi.WithInfo(apiInfo()),
		tapesoapi.WithSchemaReflector(tapesoapi.NewReflector(
			tapesoapi.WithDocs(docs),
			tapesoapi.WithTypeNamer(componentTypeName),
		)),
	)
}

// CompileOpenAPI builds the read API's published contract.
//
// It constructs a server purely to make it register its routes, then compiles
// what that registration produced. Generating from the real construction path
// rather than from a description of it is the point: there is no second list of
// routes to fall out of step with the first.
//
// The server is never started and never serves a request, so it is given no
// driver — the handlers it registers are function values, and nothing calls
// them.
func CompileOpenAPI(ctx context.Context, docs tapesoapi.TypeDocs) (*tapesoapi.CompiledDoc, error) {
	server, err := newServer( //nolint:contextcheck // Fiber establishes context only when a request arrives.
		Config{ListenAddr: ":0"},
		nil,
		slog.New(slog.DiscardHandler),
		docs,
	)
	if err != nil {
		return nil, err
	}

	return server.openapi.Compile(ctx, tapesoapi.WithTarget(tapesoapi.V30))
}

// OpenAPIParser returns the live parser every route on this server registered
// itself into.
//
// It is the single source the published contract is generated from, which is
// what makes "served but undocumented" impossible by construction rather than
// by a convention someone has to remember.
func (s *Server) OpenAPIParser() *tapesoapi.Parser { return s.openapi }

// reservedComponentNames are bare type names that must not become component
// schema keys.
//
// A downstream Rust client (progenitor) emits a type per component schema, and
// a name that shadows a prelude type stops the generated code compiling —
// `Result` shadowing `std::result::Result` is the one that actually bit us.
// Those types keep a package-qualified name instead.
var reservedComponentNames = map[string]bool{
	"Result": true, "Option": true, "String": true, "Vec": true, "Box": true,
	"Self": true, "Ok": true, "Err": true, "Some": true, "None": true,
	"Iterator": true, "Future": true,
}

// componentTypeName maps a Go type to the component name it is published under.
func componentTypeName(t reflect.Type) string {
	name := t.Name()
	if !reservedComponentNames[name] {
		return name
	}

	// Qualify with the defining package rather than mangling the name, so
	// `seed.Result` becomes `SeedResult` — still readable as a type identifier
	// in every target language, and unambiguous about where it came from.
	pkg := t.PkgPath()
	if index := strings.LastIndex(pkg, "/"); index >= 0 {
		pkg = pkg[index+1:]
	}
	if pkg == "" {
		return name
	}

	return strings.ToUpper(pkg[:1]) + pkg[1:] + name
}
