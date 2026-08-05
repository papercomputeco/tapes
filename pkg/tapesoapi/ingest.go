package tapesoapi

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DocOption adjusts how one ingested document is decomposed.
type DocOption func(*docOptions)

type docOptions struct {
	provenance        Provenance
	pathPrefix        string
	namespace         string
	operationIDPrefix string
	keepTags          map[string]struct{}
	authoritative     bool
	dropServers       bool
	dropInfo          bool
	dropExtensions    bool
}

// WithProvenance names the ingested document in conflict errors. Ingestion sets
// a sensible default (the file path or URL); this overrides it.
func WithProvenance(provenance Provenance) DocOption {
	return func(o *docOptions) { o.provenance = provenance }
}

// WithPathPrefix mounts every path in the document under a prefix.
//
// This and [WithComponentNamespace] are the aggregation workhorses: together
// they are what lets three independently authored documents compose into one
// gateway description without colliding.
func WithPathPrefix(prefix string) DocOption {
	return func(o *docOptions) { o.pathPrefix = strings.TrimSuffix(prefix, "/") }
}

// WithComponentNamespace prefixes every component name, and rewrites every
// document-local reference to match.
//
// Namespacing pre-empts collisions rather than resolving them. Two cassettes
// that each define a `Row` schema are not describing the same type, and merging
// them under one name would publish a schema neither of them wrote.
func WithComponentNamespace(namespace string) DocOption {
	return func(o *docOptions) { o.namespace = namespace }
}

// WithOperationIDPrefix prefixes every operationId in the document, giving one
// to any operation that arrived without one.
//
// This is the operation-level counterpart to [WithComponentNamespace], and it
// exists for the same reason: an operationId has to be unique across the whole
// document, and two independently authored inputs are perfectly free to have
// both named an operation `read`. Namespacing pre-empts that; the alternative is
// an aggregate that cannot be published as a valid contract.
//
// It is a real edit to a document's contract, so it belongs to aggregation and
// not to republication. The document a client fetches for one input alone is
// served verbatim, ids untouched — see the per-cassette endpoint in
// api/cassetterunner. The prefixed ids exist only in the merged document, where
// the unprefixed ones could not have coexisted anyway.
func WithOperationIDPrefix(prefix string) DocOption {
	return func(o *docOptions) { o.operationIDPrefix = prefix }
}

// WithTagFilter ingests only the operations carrying one of the given tags.
func WithTagFilter(keep ...string) DocOption {
	return func(o *docOptions) {
		o.keepTags = make(map[string]struct{}, len(keep))
		for _, tag := range keep {
			o.keepTags[tag] = struct{}{}
		}
	}
}

// WithAuthoritativeInfo marks this document's Info as the one that wins, rather
// than colliding with another document's.
func WithAuthoritativeInfo() DocOption {
	return func(o *docOptions) { o.authoritative = true }
}

// WithoutServers drops the document's servers.
//
// A document being merged into an aggregate usually describes an origin the
// aggregate does not serve — a cassette's own listener, which clients reach
// only through core's proxy — and carrying that origin through would send them
// somewhere they cannot go.
func WithoutServers() DocOption {
	return func(o *docOptions) { o.dropServers = true }
}

// WithoutInfo drops the document's Info, for merging a document into an
// aggregate that already has one.
func WithoutInfo() DocOption {
	return func(o *docOptions) { o.dropInfo = true }
}

// WithoutRootExtensions drops the document's root `x-` keys. It is how an
// aggregate avoids inheriting a per-document extension — a cassette manifest,
// say — that describes only one of its inputs.
func WithoutRootExtensions() DocOption {
	return func(o *docOptions) { o.dropExtensions = true }
}

func resolveDocOptions(defaultProvenance Provenance, options []DocOption) docOptions {
	resolved := docOptions{provenance: defaultProvenance}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	if resolved.provenance.Kind == "" {
		resolved.provenance.Kind = KindDocument
	}

	return resolved
}

// AddDocument ingests one OpenAPI document from bytes. JSON and YAML are both
// accepted.
func (p *Parser) AddDocument(ctx context.Context, data []byte, options ...DocOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	resolved := resolveDocOptions(Provenance{Kind: KindDocument, Name: "<bytes>"}, options)
	document, err := ParseYAML(data)
	if err != nil {
		return fmt.Errorf("%s: %w", resolved.provenance, err)
	}

	return p.addParsedDocument(document, resolved)
}

// AddDocumentReader ingests one OpenAPI document from a reader.
func (p *Parser) AddDocumentReader(ctx context.Context, reader io.Reader, options ...DocOption) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	return p.AddDocument(ctx, data, options...)
}

// AddDocumentFile ingests one OpenAPI document from disk.
func (p *Parser) AddDocumentFile(ctx context.Context, path string, options ...DocOption) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read OpenAPI document: %w", err)
	}
	options = append([]DocOption{WithProvenance(Provenance{Kind: KindDocument, Name: path})}, options...)

	return p.AddDocument(ctx, data, options...)
}

// AddDocumentFS ingests one OpenAPI document from an fs.FS, which is how an
// embedded contract is loaded without touching the filesystem at runtime.
func (p *Parser) AddDocumentFS(ctx context.Context, fsys fs.FS, path string, options ...DocOption) error {
	data, err := fs.ReadFile(fsys, path)
	if err != nil {
		return fmt.Errorf("read OpenAPI document: %w", err)
	}
	options = append([]DocOption{WithProvenance(Provenance{Kind: KindDocument, Name: path})}, options...)

	return p.AddDocument(ctx, data, options...)
}

// AddDocumentGlob ingests every document matching a shell pattern, in sorted
// order so a directory of specs compiles the same way on every machine.
func (p *Parser) AddDocumentGlob(ctx context.Context, pattern string, options ...DocOption) error {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob %q: %w", pattern, err)
	}
	if len(matches) == 0 {
		return fmt.Errorf("glob %q matched no files", pattern)
	}
	sort.Strings(matches)
	for _, match := range matches {
		if err := p.AddDocumentFile(ctx, match, options...); err != nil {
			return err
		}
	}

	return nil
}

// AddParsedDocument ingests an already-parsed document, for callers that hold
// one for another reason — the cassette runner reads a manifest out of the same
// document it publishes, and re-parsing it would be a second chance to disagree.
func (p *Parser) AddParsedDocument(document *Document, options ...DocOption) error {
	resolved := resolveDocOptions(Provenance{Kind: KindDocument, Name: "<document>"}, options)

	return p.addParsedDocument(document, resolved)
}

func (p *Parser) addParsedDocument(document *Document, resolved docOptions) error {
	fragment, err := document.Fragment(resolved.provenance)
	if err != nil {
		return err
	}
	if err := applyDocOptions(&fragment, resolved); err != nil {
		return fmt.Errorf("%s: %w", resolved.provenance, err)
	}

	return p.AddFragment(fragment)
}

// applyDocOptions rewrites a freshly decomposed fragment per the ingest
// options: filter, namespace, name, then prefix, in that order. Filtering first
// means a namespace is not spent on components only reachable from dropped
// operations, and prefixing last means the tag filter matched the paths the
// document actually declared.
//
// Naming before prefixing is the one ordering a caller can observe: an id
// synthesized here for an anonymous operation comes from the path as the
// document declared it, not from the path this ingest mounts it under. A caller
// that wants the mounted path in the id — because that is the path a generated
// client calls — rewrites the paths before ingesting rather than passing
// [WithPathPrefix]; the cassette runner does exactly that.
func applyDocOptions(fragment *Fragment, options docOptions) error {
	fragment.Authoritative = options.authoritative
	if options.dropServers {
		fragment.Servers = nil
	}
	if options.dropInfo {
		fragment.Info = nil
	}
	if options.dropExtensions {
		fragment.Extensions = nil
	}
	if len(options.keepTags) > 0 {
		filterByTag(fragment, options.keepTags)
	}
	if options.namespace != "" {
		namespaceComponents(fragment, options.namespace)
	}
	if options.operationIDPrefix != "" {
		prefixOperationIDs(fragment, options.operationIDPrefix)
	}
	if options.pathPrefix != "" {
		if err := prefixPaths(fragment, options.pathPrefix); err != nil {
			return err
		}
	}

	return nil
}

func filterByTag(fragment *Fragment, keep map[string]struct{}) {
	for path, item := range fragment.Paths {
		for method, operation := range item.Operations {
			if !hasAnyTag(operation.Tags, keep) {
				delete(item.Operations, method)
			}
		}
		if len(item.Operations) == 0 {
			delete(fragment.Paths, path)
		}
	}
	kept := fragment.Tags[:0]
	for _, tag := range fragment.Tags {
		if _, ok := keep[tag.Name]; ok {
			kept = append(kept, tag)
		}
	}
	fragment.Tags = kept
}

func hasAnyTag(tags []string, keep map[string]struct{}) bool {
	for _, tag := range tags {
		if _, ok := keep[tag]; ok {
			return true
		}
	}

	return false
}

func prefixPaths(fragment *Fragment, prefix string) error {
	if len(fragment.Paths) == 0 {
		return nil
	}
	prefixed := make(map[string]*PathItem, len(fragment.Paths))
	for _, path := range fragment.paths() {
		mounted, err := NormalizePath(joinPath(prefix, path))
		if err != nil {
			return err
		}
		if _, collision := prefixed[mounted]; collision {
			return fmt.Errorf("mounting under %q collides two paths onto %q", prefix, mounted)
		}
		prefixed[mounted] = fragment.Paths[path]
	}
	fragment.Paths = prefixed

	return nil
}

// namespaceComponents prefixes every component key and rewrites every local
// reference to match.
func namespaceComponents(fragment *Fragment, namespace string) {
	if fragment.Components == nil {
		renameRefs(fragment, nil)

		return
	}
	rewrites := map[string]string{}

	renameSchemas := func(prefix string, names []string) {
		for _, name := range names {
			rewrites[prefix+name] = prefix + namespace + name
		}
	}
	components := fragment.Components
	renameSchemas(componentsSchemaPrefix, sortedKeys(components.Schemas))
	renameSchemas(componentsResponsePrefix, sortedKeys(components.Responses))
	renameSchemas(componentsParameterPrefix, sortedKeys(components.Parameters))
	renameSchemas(componentsRequestBodyPrefix, sortedKeys(components.RequestBodies))
	renameSchemas(componentsHeaderPrefix, sortedKeys(components.Headers))
	renameSchemas(componentsExamplePrefix, sortedKeys(components.Examples))
	renameSchemas(componentsSecuritySchemePrefix, sortedKeys(components.SecuritySchemes))

	components.Schemas = renameKeys(components.Schemas, namespace)
	components.Responses = renameKeys(components.Responses, namespace)
	components.Parameters = renameKeys(components.Parameters, namespace)
	components.RequestBodies = renameKeys(components.RequestBodies, namespace)
	components.Headers = renameKeys(components.Headers, namespace)
	components.Examples = renameKeys(components.Examples, namespace)
	components.SecuritySchemes = renameKeys(components.SecuritySchemes, namespace)

	renameRefs(fragment, rewrites)

	// Security requirements name schemes by bare key rather than by ref, so
	// they are rewritten separately or an aggregate would require a scheme that
	// no longer exists under that name.
	renameSecuritySchemes(fragment, namespace)
}

func renameKeys[V any](in map[string]V, namespace string) map[string]V {
	if len(in) == 0 {
		return in
	}
	out := make(map[string]V, len(in))
	for name, value := range in {
		out[namespace+name] = value
	}

	return out
}

func renameSecuritySchemes(fragment *Fragment, namespace string) {
	rename := func(requirements []SecurityRequirement) {
		for _, requirement := range requirements {
			for scheme, scopes := range requirement {
				delete(requirement, scheme)
				requirement[namespace+scheme] = scopes
			}
		}
	}
	rename(fragment.Security)
	for _, item := range fragment.Paths {
		for _, operation := range item.Operations {
			rename(operation.Security)
		}
	}
}

// renameRefs walks every schema in a fragment and applies the rewrites.
func renameRefs(fragment *Fragment, rewrites map[string]string) {
	if len(rewrites) == 0 {
		return
	}
	visit := func(ref string) string {
		if replacement, ok := rewrites[ref]; ok {
			return replacement
		}

		return ref
	}
	for _, item := range fragment.Paths {
		walkPathItemRefs(item, visit)
	}
	for _, item := range fragment.Webhooks {
		walkPathItemRefs(item, visit)
	}
	if fragment.Components == nil {
		return
	}
	for _, schema := range fragment.Components.Schemas {
		schema.walkRefs(visit)
	}
	for _, response := range fragment.Components.Responses {
		walkResponseRefs(response, visit)
	}
	for _, parameter := range fragment.Components.Parameters {
		walkParameterRefs(parameter, visit)
	}
	for _, body := range fragment.Components.RequestBodies {
		walkRequestBodyRefs(body, visit)
	}
	for _, header := range fragment.Components.Headers {
		walkHeaderRefs(header, visit)
	}
	// Example values may themselves be Reference Objects; without this walk
	// a namespaced example that referenced a sibling would keep the
	// un-namespaced target and dangle in the aggregate (or resolve to an
	// unrelated cassette's component of the same name).
	for name, example := range fragment.Components.Examples {
		fragment.Components.Examples[name] = walkExampleRef(example, visit)
	}
}

// walkExampleRef rewrites the `$ref` of an example that is a Reference
// Object. Literal examples pass through untouched — `$ref` is the only key
// with reference semantics, and only at the top level of the value.
func walkExampleRef(example any, visit func(string) string) any {
	object, ok := example.(map[string]any)
	if !ok {
		return example
	}
	ref, ok := object["$ref"].(string)
	if !ok {
		return example
	}
	rewritten := make(map[string]any, len(object))
	maps.Copy(rewritten, object)
	rewritten["$ref"] = visit(ref)

	return rewritten
}

func walkPathItemRefs(item *PathItem, visit func(string) string) {
	if item == nil {
		return
	}
	walkExtensionRefs(item.Extensions, visit)
	for _, parameter := range item.Parameters {
		walkParameterRefs(parameter, visit)
	}
	for _, operation := range item.Operations {
		walkExtensionRefs(operation.Extensions, visit)
		for _, parameter := range operation.Parameters {
			walkParameterRefs(parameter, visit)
		}
		walkRequestBodyRefs(operation.RequestBody, visit)
		for _, response := range operation.Responses {
			walkResponseRefs(response, visit)
		}
	}
}

func walkParameterRefs(parameter *Parameter, visit func(string) string) {
	if parameter == nil {
		return
	}
	walkExtensionRefs(parameter.Extensions, visit)
	if parameter.Ref != "" {
		parameter.Ref = visit(parameter.Ref)

		return
	}
	parameter.Schema.walkRefs(visit)
}

func walkRequestBodyRefs(body *RequestBody, visit func(string) string) {
	if body == nil {
		return
	}
	walkExtensionRefs(body.Extensions, visit)
	if body.Ref != "" {
		body.Ref = visit(body.Ref)

		return
	}
	walkContentRefs(body.Content, visit)
}

func walkResponseRefs(response *Response, visit func(string) string) {
	if response == nil {
		return
	}
	walkExtensionRefs(response.Extensions, visit)
	if response.Ref != "" {
		response.Ref = visit(response.Ref)

		return
	}
	walkContentRefs(response.Content, visit)
	for _, header := range response.Headers {
		walkHeaderRefs(header, visit)
	}
}

func walkHeaderRefs(header *Header, visit func(string) string) {
	if header == nil {
		return
	}
	if header.Ref != "" {
		header.Ref = visit(header.Ref)

		return
	}
	header.Schema.walkRefs(visit)
}

func walkContentRefs(content map[string]*MediaType, visit func(string) string) {
	for _, entry := range content {
		if entry != nil {
			entry.Schema.walkRefs(visit)
			walkExtensionRefs(entry.Extensions, visit)
			for name, example := range entry.Examples {
				entry.Examples[name] = walkExampleRef(example, visit)
			}
		}
	}
}
