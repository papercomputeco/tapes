// Package tapesoapi parses, aggregates, and compiles OpenAPI v3 documents.
//
// It exists because tapes assembles its API description from two sources that
// nothing off the shelf treats alike: its own live Fiber routes, and the
// OpenAPI documents cassettes publish over HTTP for reverse-proxy mounting.
// Both are handled here as the same thing — a Fragment, a partial contribution
// tagged with where it came from — so one merge, validate, and render pipeline
// serves both instead of two that drift.
//
// The shape of a use is always the same:
//
//	parser := tapesoapi.NewParser(tapesoapi.WithInfo(tapesoapi.Info{
//		Title: "tapes", Version: "v1",
//	}))
//	if err := parser.AddDocument(ctx, cassetteSpec,
//		tapesoapi.WithComponentNamespace("hello_world_")); err != nil {
//		return err
//	}
//	compiled, err := parser.Compile(ctx)
//
// The Parser is a mutable accumulator guarded by a mutex; Compile is a pure
// function of the fragments it holds. Compiling the same fragments twice yields
// byte-identical output, which is what makes the generated contracts diffable
// in CI and cacheable behind an ETag.
//
// All I/O happens at Add time. Compile never reads a file or opens a socket, so
// it is safe to call on a request path — which /openapi does.
//
// # Versions
//
// The internal model is version-neutral: it stores the union of 3.0 and 3.1
// semantics, and the version decision happens once, at render time. Both [V30]
// and [V31] render. [V30] is the default because the Rust client generator
// tapes publishes for reads only accepts 3.0.x; a 3.1-only construct reaching a
// 3.0 render is a documented loss, refused unless [WithDowngradeLossy] says to
// approximate it. See version.go.
//
// # Fiber
//
// The core has no web-framework dependency. The route-registration wrapper
// lives in the oasfiber subpackage, against the same Source interface any
// other adapter would implement.
package tapesoapi

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Parser accumulates fragments from any number of sources.
//
// The zero value is not usable; call NewParser. A Parser is safe for concurrent
// use, because the Fiber adapter contributes a fragment per route registration
// and nothing orders those against a concurrent Compile.
type Parser struct {
	mutex     sync.RWMutex
	fragments []Fragment
	frozen    bool
	options   parserOptions
}

// parserOptions is the resolved parser configuration.
//
// There is deliberately no option to resolve external references. This package
// performs no network or filesystem I/O outside an explicit Add call, which is
// what makes Compile safe to run on a request path — and a cassette's document
// arrives from a service core does not control, so following its references
// would let it choose what core fetches.
type parserOptions struct {
	conflictPolicy ConflictPolicy
	info           *Info
	servers        []Server
	reflector      Reflector
}

// Option configures a Parser.
type Option func(*parserOptions)

// WithConflictPolicy sets how colliding contributions are resolved. The default
// is [PolicyError], which reports every collision at once rather than picking
// a winner — an aggregate whose contents depend on load order is worse than one
// that refuses to build.
func WithConflictPolicy(policy ConflictPolicy) Option {
	return func(o *parserOptions) { o.conflictPolicy = policy }
}

// WithInfo sets the authoritative document Info. A parser given one is immune
// to Info conflicts between ingested documents: the aggregate is this API, and
// the documents merged into it describe parts of it.
func WithInfo(info Info) Option {
	return func(o *parserOptions) { o.info = &info }
}

// WithServer appends a server to the compiled document.
func WithServer(url string, description ...string) Option {
	return func(o *parserOptions) {
		server := Server{URL: url}
		if len(description) > 0 {
			server.Description = description[0]
		}
		o.servers = append(o.servers, server)
	}
}

// WithSchemaReflector replaces the Go-type-to-schema reflector.
func WithSchemaReflector(reflector Reflector) Option {
	return func(o *parserOptions) { o.reflector = reflector }
}

// NewParser returns an empty parser.
func NewParser(options ...Option) *Parser {
	resolved := parserOptions{
		conflictPolicy: PolicyError,
		reflector:      NewReflector(),
	}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}

	return &Parser{options: resolved}
}

// ErrFrozen is returned by every Add method once the parser is frozen.
var ErrFrozen = errors.New("parser is frozen; no further contributions accepted")

// Freeze makes the parser read-only.
//
// The intended lifecycle is register-everything-then-compile, and a route
// registered after startup silently changes a document already served. Freezing
// turns that into a loud error at the registration site.
func (p *Parser) Freeze() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.frozen = true
}

// Frozen reports whether the parser is read-only.
func (p *Parser) Frozen() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	return p.frozen
}

// Reflector returns the parser's Go-type schema reflector, so an adapter can
// reflect a handler's types against the same registry the parser will compile.
func (p *Parser) Reflector() Reflector { return p.options.reflector }

// Schema derives the schema for a Go value's type, registering named struct
// types as components of this parser's compiled document.
//
// It is the shorthand a route declaration reaches for:
//
//	Response(200, "the session", parser.Schema(SessionDetailResponse{}))
//
// A type that cannot be described — a channel, a func — yields a schema that
// carries the reason, so one bad field degrades that one property instead of
// failing a registration that has no way to report an error.
func (p *Parser) Schema(value any) *Schema {
	schema, err := p.options.reflector.Reflect(value)
	if err != nil {
		return &Schema{Description: "schema unavailable: " + err.Error()}
	}

	return schema
}

// AddFragment records one contribution.
func (p *Parser) AddFragment(fragment Fragment) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.frozen {
		return fmt.Errorf("adding fragment from %s: %w", fragment.Provenance, ErrFrozen)
	}
	p.fragments = append(p.fragments, fragment.clone())

	return nil
}

// AddSource records every fragment a source produces.
func (p *Parser) AddSource(ctx context.Context, source Source) error {
	if source == nil {
		return errors.New("nil source")
	}
	fragments, err := source.Fragments(ctx)
	if err != nil {
		return err
	}
	for _, fragment := range fragments {
		if err := p.AddFragment(fragment); err != nil {
			return err
		}
	}

	return nil
}

// AddOperation records one programmatically described operation.
//
// path is in OpenAPI form ("/users/{id}"); the Fiber adapter converts from
// framework syntax before calling this.
func (p *Parser) AddOperation(method, path string, operation *Operation, provenance Provenance) error {
	if operation == nil {
		return fmt.Errorf("%s %s: nil operation", method, path)
	}
	normalizedMethod, err := normalizeMethod(method)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	normalizedPath, err := normalizePath(path)
	if err != nil {
		return err
	}
	if provenance.Kind == "" {
		provenance.Kind = KindManual
	}
	if provenance.Name == "" {
		provenance.Name = normalizedMethod + " " + normalizedPath
	}

	clone := operation.clone()
	clone.provenance = provenance
	declarePathParams(clone, normalizedPath)

	return p.AddFragment(Fragment{
		Provenance: provenance,
		Paths: map[string]*PathItem{
			normalizedPath: {Operations: map[string]*Operation{normalizedMethod: clone}},
		},
	})
}

// AddComponentSchema registers one reusable schema under a bare component name.
func (p *Parser) AddComponentSchema(name string, schema *Schema, provenance Provenance) error {
	if provenance.Kind == "" {
		provenance.Kind = KindManual
	}
	if provenance.Name == "" {
		provenance.Name = "schemas/" + name
	}

	return p.AddFragment(Fragment{
		Provenance: provenance,
		Components: &Components{Schemas: map[string]*Schema{name: schema}},
	})
}

// Fragments returns a snapshot of what the parser holds, for inspection and
// tests. The returned fragments are copies.
func (p *Parser) Fragments() []Fragment {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	out := make([]Fragment, len(p.fragments))
	for i, fragment := range p.fragments {
		out[i] = fragment.clone()
	}

	return out
}

// snapshot copies the fragment slice under a read lock. Everything downstream
// of it in Compile runs against the copy, so a concurrent registration cannot
// change a compile already in flight.
func (p *Parser) snapshot() ([]Fragment, parserOptions) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	out := make([]Fragment, len(p.fragments))
	for i, fragment := range p.fragments {
		out[i] = fragment.clone()
	}

	return out, p.options
}
