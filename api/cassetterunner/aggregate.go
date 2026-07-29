package cassetterunner

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// Document returns one description of everything this core publishes.
//
// base is core's own surface — the live parser the API server's routes
// registered themselves into. Passing it means /openapi answers the question a
// client actually has ("what can I call on this origin?") rather than the
// narrower one the endpoint used to answer ("what do the cassettes serve?").
// A nil base yields the cassettes alone.
//
// base is never mutated: the aggregate is compiled from a copy, because it is
// built per request and core's live parser outlives all of them.
func (runner *Runner) Document(ctx context.Context, base *tapesoapi.Parser) ([]byte, error) {
	options := []tapesoapi.Option{
		tapesoapi.WithInfo(tapesoapi.Info{
			Title:       runner.title,
			Version:     runner.version,
			Description: "Aggregated API surface.",
		}),
		// Core's routes and a cassette's document describe disjoint path
		// spaces — a cassette is admitted only if every path it declares lives
		// under its own prefix — so a collision here is a real defect. But it
		// is a defect in a document core fetched from a service it does not
		// control, and refusing to serve /openapi at all would make one
		// misbehaving cassette hide every other one. Last-wins keeps the
		// endpoint answering; the loser is reported in the compiled warnings.
		tapesoapi.WithConflictPolicy(tapesoapi.PolicyLastWins),
	}

	if base != nil {
		// Core's fragments hold its operations, and its operations $ref component
		// schemas that live in its reflector's registry rather than in any
		// fragment — a route says `parser.Schema(SessionDetailResponse{})` and the
		// name is claimed there. Compiling the fragments against a fresh reflector
		// would carry every one of those refs into a document that defines none of
		// them, so the aggregate borrows core's registry instead of starting an
		// empty one.
		//
		// Borrowing is read-only: nothing here reflects a Go type, and the
		// registry hands out clones under its own lock.
		options = append(options, tapesoapi.WithSchemaReflector(base.Reflector()))
	}

	aggregate := tapesoapi.NewParser(options...)

	if base != nil {
		for _, fragment := range base.Fragments() {
			if err := aggregate.AddFragment(fragment); err != nil {
				return nil, fmt.Errorf("aggregate core surface: %w", err)
			}
		}
	}

	documents := runner.specs.documents()
	names := make([]string, 0, len(documents))
	for name := range documents {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		document := documents[name]
		if document == nil {
			continue
		}
		if err := aggregate.AddParsedDocument(document,
			tapesoapi.WithProvenance(tapesoapi.Provenance{
				Kind: tapesoapi.KindDocument,
				Name: "cassette " + name,
			}),
			// Two cassettes that each define a `Row` are not describing one
			// type. Namespacing by cassette name is what lets both appear.
			tapesoapi.WithComponentNamespace(componentNamespace(cassette.Name(name))),
			// And the same for operationIds, for the same reason: two cassettes
			// are free to have both named an operation `read`, and an operationId
			// has to be unique across the document it appears in.
			//
			// This does rewrite part of a cassette's contract, which core does not
			// otherwise do. It is confined to the aggregate, where the original ids
			// could not have coexisted: /v1/cassettes/{name}/openapi.json still
			// serves that cassette's own bytes, ids untouched, so a client
			// generated against one cassette is unaffected by what another one
			// happens to be called.
			tapesoapi.WithOperationIDPrefix(componentNamespace(cassette.Name(name))),
			// A cassette's own info, servers, and manifest describe the
			// cassette, not the aggregate. Carrying its servers through would
			// point clients at a listener they cannot reach: they reach a
			// cassette through core's proxy, never directly.
			tapesoapi.WithoutInfo(),
			tapesoapi.WithoutServers(),
			tapesoapi.WithoutRootExtensions(),
		); err != nil {
			return nil, fmt.Errorf("aggregate cassette %q: %w", name, err)
		}
	}

	compiled, err := aggregate.Compile(ctx,
		// This document is the canonical description of the origin — the thing a
		// client points a code generator at to reach core and every cassette
		// behind it — so it is compiled as a contract, validated, and not as a
		// best-effort catalogue.
		//
		// That is possible because the two ways a merge of independently authored
		// documents ordinarily goes invalid are both pre-empted above: colliding
		// component names by the namespace, colliding operationIds by the id
		// prefix. What is left for validation to catch is a cassette document
		// that was already malformed on its own, and those never reach here —
		// [publishable] refuses them when the cassette is published.
		//
		// Lints, not just structure. A document that omits an operationId or
		// declares no responses is legal OpenAPI and useless to a generator, and
		// this endpoint's whole purpose is to be generated from.
		tapesoapi.WithLint(
			tapesoapi.OperationIDPresent{},
			tapesoapi.OperationIDUnique{},
			tapesoapi.ResponsesDeclared{},
		),
		// Cassettes may publish 3.1. Approximating their 3.1-only constructs
		// beats refusing to describe a surface that is already reachable.
		tapesoapi.WithDowngradeLossy(),
	)
	if err != nil {
		return nil, err
	}

	return compiled.JSON(), nil
}

// publishable reports whether one cassette's document can survive [Document].
//
// /openapi is compiled as a contract rather than as a best-effort catalogue, so
// a single malformed cassette document would otherwise be able to fail the
// whole endpoint and take every healthy cassette's surface down with it. This is
// where that is prevented: a document that cannot be compiled is refused at the
// source that served it, which is reported as that source's rejection and
// changes nothing about the cassettes already published.
//
// It is checked in the shape it will be published in — after republication, not
// as fetched — so that what passes here is exactly what the aggregate merges,
// and exactly what /v1/cassettes/{name}/openapi.json hands out.
//
// The document is compiled alone rather than trial-merged, because a cassette
// has to stand or fall on its own contents and not on which other cassettes
// happen to be installed the moment it was fetched. The two options that give
// the aggregate its cross-document guarantees are therefore absent here, and
// the lint set is the aggregate's minus the one rule aggregation supplies the
// answer to: an anonymous operation is legal in a cassette, because the id
// prefix names it. A duplicate id is not, because prefixing every id in a
// document uniformly leaves a self-collision exactly where it was.
func publishable(ctx context.Context, document *tapesoapi.Document, name cassette.Name) error {
	solo := tapesoapi.NewParser()
	if err := solo.AddParsedDocument(document,
		tapesoapi.WithProvenance(tapesoapi.Provenance{
			Kind: tapesoapi.KindDocument,
			Name: "cassette " + string(name),
		}),
		// The parts the aggregate discards cannot be grounds to refuse a
		// cassette, or admission would reject documents /openapi would have
		// served without complaint.
		tapesoapi.WithoutInfo(),
		tapesoapi.WithoutServers(),
		tapesoapi.WithoutRootExtensions(),
	); err != nil {
		return err
	}
	if _, err := solo.Compile(ctx,
		tapesoapi.WithLint(
			tapesoapi.OperationIDUnique{},
			tapesoapi.ResponsesDeclared{},
		),
		tapesoapi.WithDowngradeLossy(),
	); err != nil {
		return fmt.Errorf("document cannot be published: %w", err)
	}

	return nil
}

// componentNamespace turns a cassette name into a component-name prefix.
//
// Component names appear as generated type identifiers downstream, so the
// separator has to be one an identifier can carry: a hyphen is legal in a
// cassette name and not in most target languages' identifiers.
func componentNamespace(name cassette.Name) string {
	return strings.ReplaceAll(string(name), "-", "_") + "_"
}
