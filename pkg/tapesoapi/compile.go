package tapesoapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// CompiledDoc is an immutable compiled OpenAPI document.
//
// It is safe to share across goroutines and cheap to serve repeatedly: the
// rendered bytes and the fingerprint are computed once, at compile time.
type CompiledDoc struct {
	version     Version
	tree        map[string]any
	json        []byte
	fingerprint string
	warnings    []string

	// schemas is the component schemas as IR rather than as rendered bytes, kept
	// so [CompiledDoc.ValidateInstance] can check a payload against the same
	// schema the document published. Reparsing the output to get them back would
	// be a second parser reading this package's own writing.
	schemas map[string]*Schema
}

// CompileOption adjusts one compile.
type CompileOption func(*compileOptions)

type compileOptions struct {
	target        Version
	skipValidate  bool
	downgradeLoss bool
	lintRules     []LintRule
}

// WithTarget selects the version to render. The default is [V30].
func WithTarget(version Version) CompileOption {
	return func(o *compileOptions) { o.target = version }
}

// WithoutValidation skips structural validation. It is an escape hatch for
// serving a known-imperfect upstream document rather than failing the request,
// not a way to land one in a generated contract.
func WithoutValidation() CompileOption {
	return func(o *compileOptions) { o.skipValidate = true }
}

// WithDowngradeLossy permits rendering 3.1-only constructs to a 3.0 target by
// approximating them, instead of failing. Without it, a downgrade that would
// drop meaning is an error naming the construct and the document it came from.
func WithDowngradeLossy() CompileOption {
	return func(o *compileOptions) { o.downgradeLoss = true }
}

// WithLint replaces the lint rules run after validation.
func WithLint(rules ...LintRule) CompileOption {
	return func(o *compileOptions) { o.lintRules = rules }
}

// Compile merges every fragment into one validated document.
//
// The pipeline is snapshot, merge, resolve, render, validate, freeze — and it
// performs no I/O, so it is safe to call on a request path. Compiling the same
// fragments twice produces byte-identical output.
func (p *Parser) Compile(ctx context.Context, options ...CompileOption) (*CompiledDoc, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	resolved := compileOptions{target: V30, lintRules: DefaultLintRules()}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	if !resolved.target.Valid() {
		return nil, fmt.Errorf("unsupported compile target %q", resolved.target)
	}

	fragments, parserOptions := p.snapshot()

	// Every type reflected through this parser becomes a component, whether it
	// was reflected at route-registration time or moments ago. Folding the
	// registry in here rather than at each Reflect call means a type used by
	// twelve operations is registered once and cannot be contributed twelve
	// times with twelve provenances.
	if parserOptions.reflector != nil {
		if components := parserOptions.reflector.Components(); len(components) > 0 {
			fragments = append(fragments, Fragment{
				Provenance: Provenance{Kind: KindReflect, Name: "go types"},
				Components: &Components{Schemas: components},
			})
		}
	}

	document, err := mergeFragments(fragments, parserOptions.conflictPolicy)
	if err != nil {
		return nil, err
	}

	applyParserDefaults(document, parserOptions)

	if err := checkVersionCompatibility(document, resolved); err != nil {
		return nil, err
	}
	if err := resolveReferences(document); err != nil {
		return nil, err
	}

	tree, err := render(document, resolved.target)
	if err != nil {
		return nil, err
	}

	encoded, err := json.MarshalIndent(tree, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode compiled document: %w", err)
	}

	if !resolved.skipValidate {
		// Structure before lint: a lint rule is entitled to assume the document
		// is well formed, and a finding about a document that is not yet valid
		// is noise in front of the reason it will not build.
		if err := validateStructure(document, resolved.target); err != nil {
			return nil, err
		}
		if err := runLint(document, resolved.lintRules); err != nil {
			return nil, err
		}
	}

	sum := sha256.Sum256(encoded)

	schemas := make(map[string]*Schema, len(document.components.Schemas))
	for name, schema := range document.components.Schemas {
		schemas[name] = schema.clone()
	}

	return &CompiledDoc{
		version:     resolved.target,
		tree:        tree,
		json:        encoded,
		fingerprint: "sha256:" + hex.EncodeToString(sum[:]),
		warnings:    document.warnings,
		schemas:     schemas,
	}, nil
}

// applyParserDefaults layers the parser-level Info and servers over what the
// fragments contributed. Parser-level settings win because they describe the
// aggregate, and the fragments describe its parts.
func applyParserDefaults(document *merged, options parserOptions) {
	if options.info != nil {
		document.info = options.info
		document.infoSource = Provenance{Kind: KindManual, Name: "parser"}
	}
	if len(options.servers) > 0 {
		document.servers = dedupeServers(append(append([]Server(nil), options.servers...), document.servers...))
	}
	if document.info == nil {
		// A document with no Info is invalid, and failing here would make an
		// empty parser un-compilable — which the aggregate endpoint needs to
		// do before any cassette has resolved.
		document.info = &Info{Title: "API", Version: "0.0.0"}
	}
	if document.info.Version == "" {
		document.info.Version = "0.0.0"
	}
	if document.info.Title == "" {
		document.info.Title = "API"
	}
}

// checkVersionCompatibility refuses a compile that would silently drop meaning.
func checkVersionCompatibility(document *merged, options compileOptions) error {
	if options.downgradeLoss {
		return nil
	}
	if options.target != V30 {
		return nil
	}

	var problems []string
	if len(document.webhooks) > 0 {
		problems = append(problems, fmt.Sprintf("webhooks (%d) have no OpenAPI 3.0 equivalent", len(document.webhooks)))
	}
	for _, name := range sortedKeys(document.components.Schemas) {
		for _, feature := range document.components.Schemas[name].uses31Only() {
			problems = append(problems, fmt.Sprintf("schemas/%s uses 3.1-only %q", name, feature))
		}
	}
	for _, path := range sortedKeys(document.paths) {
		item := document.paths[path]
		for _, method := range item.Methods() {
			operation := item.Operations[method]
			for _, feature := range operationUses31Only(operation) {
				problems = append(problems, fmt.Sprintf("%s %s uses 3.1-only %q (from %s)",
					method, path, feature, operation.provenance))
			}
		}
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)

	return fmt.Errorf(
		"compiling to OpenAPI %s would lose meaning:\n  - %s\ncompile to %s, or pass WithDowngradeLossy to approximate",
		V30, strings.Join(dedupeStrings(problems), "\n  - "), V31)
}

func operationUses31Only(operation *Operation) []string {
	var found []string
	for _, parameter := range operation.Parameters {
		found = append(found, parameter.Schema.uses31Only()...)
	}
	if operation.RequestBody != nil {
		for _, name := range sortedKeys(operation.RequestBody.Content) {
			found = append(found, operation.RequestBody.Content[name].Schema.uses31Only()...)
		}
	}
	for _, status := range sortedKeys(operation.Responses) {
		response := operation.Responses[status]
		for _, name := range sortedKeys(response.Content) {
			found = append(found, response.Content[name].Schema.uses31Only()...)
		}
	}
	sort.Strings(found)

	return dedupeStrings(found)
}

// resolveReferences verifies that every document-local reference targets
// something that survived the merge.
//
// Only local references are checked, and no I/O is performed: a reference into
// another document is that document's business, and following one would make
// Compile fetch a URL chosen by whoever wrote the input. A dangling *local* ref
// is the classic aggregation failure — a namespaced component whose referrer
// was not rewritten — and catching it here is what stops it from reaching a
// client generator as a crash.
func resolveReferences(document *merged) error {
	available := map[string]struct{}{}
	record := func(prefix string, names []string) {
		for _, name := range names {
			available[prefix+name] = struct{}{}
		}
	}
	record(componentsSchemaPrefix, sortedKeys(document.components.Schemas))
	record(componentsResponsePrefix, sortedKeys(document.components.Responses))
	record(componentsParameterPrefix, sortedKeys(document.components.Parameters))
	record(componentsRequestBodyPrefix, sortedKeys(document.components.RequestBodies))
	record(componentsHeaderPrefix, sortedKeys(document.components.Headers))
	record(componentsExamplePrefix, sortedKeys(document.components.Examples))
	record(componentsSecuritySchemePrefix, sortedKeys(document.components.SecuritySchemes))

	var dangling []string
	seen := map[string]struct{}{}
	check := func(where string) func(string) string {
		return func(ref string) string {
			if !isLocalRef(ref) {
				return ref
			}
			if _, ok := available[ref]; !ok {
				key := where + ": " + ref
				if _, reported := seen[key]; !reported {
					seen[key] = struct{}{}
					dangling = append(dangling, fmt.Sprintf("%s references %s, which no fragment defines", where, ref))
				}
			}

			return ref
		}
	}

	for _, path := range sortedKeys(document.paths) {
		item := document.paths[path]
		for _, method := range item.Methods() {
			walkOperationRefs(item.Operations[method], check(method+" "+path))
		}
		for _, parameter := range item.Parameters {
			walkParameterRefs(parameter, check(path))
		}
	}
	for _, name := range sortedKeys(document.components.Schemas) {
		document.components.Schemas[name].walkRefs(check("schemas/" + name))
	}
	for _, name := range sortedKeys(document.components.Responses) {
		walkResponseRefs(document.components.Responses[name], check("responses/"+name))
	}
	for _, name := range sortedKeys(document.components.Parameters) {
		walkParameterRefs(document.components.Parameters[name], check("parameters/"+name))
	}
	for _, name := range sortedKeys(document.components.RequestBodies) {
		walkRequestBodyRefs(document.components.RequestBodies[name], check("requestBodies/"+name))
	}

	if len(dangling) == 0 {
		return nil
	}
	sort.Strings(dangling)

	return fmt.Errorf("unresolved references:\n  - %s", strings.Join(dangling, "\n  - "))
}

func walkOperationRefs(operation *Operation, visit func(string) string) {
	if operation == nil {
		return
	}
	for _, parameter := range operation.Parameters {
		walkParameterRefs(parameter, visit)
	}
	walkRequestBodyRefs(operation.RequestBody, visit)
	for _, status := range sortedKeys(operation.Responses) {
		walkResponseRefs(operation.Responses[status], visit)
	}
}

// Version reports which OpenAPI version this document was rendered to.
func (d *CompiledDoc) Version() Version { return d.version }

// MarshalJSON returns the document as indented JSON.
func (d *CompiledDoc) MarshalJSON() ([]byte, error) {
	if d == nil {
		return nil, errors.New("nil compiled document")
	}

	return append([]byte(nil), d.json...), nil
}

// JSON returns the rendered JSON bytes.
func (d *CompiledDoc) JSON() []byte {
	if d == nil {
		return nil
	}

	return append([]byte(nil), d.json...)
}

// YAML renders the document as YAML. Keys sort the same way as in JSON, so the
// two encodings describe the same document in the same order.
func (d *CompiledDoc) YAML() ([]byte, error) {
	if d == nil {
		return nil, errors.New("nil compiled document")
	}

	return yaml.JSONToYAML(d.json)
}

// Fingerprint is a content hash of the rendered document, for ETags and
// change detection.
func (d *CompiledDoc) Fingerprint() string {
	if d == nil {
		return ""
	}

	return d.fingerprint
}

// Warnings reports non-fatal merge outcomes — the conflicts a first-wins or
// last-wins policy resolved by picking. Empty under the default policy, which
// fails instead of picking.
func (d *CompiledDoc) Warnings() []string {
	if d == nil {
		return nil
	}

	return append([]string(nil), d.warnings...)
}

// Tree returns the rendered document as a generic tree. It is the escape hatch
// for callers that need to post-process the output, and it returns a copy so
// they cannot mutate the compiled document.
func (d *CompiledDoc) Tree() map[string]any {
	if d == nil {
		return nil
	}
	var copied map[string]any
	// Round-tripping through the rendered bytes is the cheapest correct deep
	// copy here, and it cannot fail: those bytes came from this tree.
	_ = json.Unmarshal(d.json, &copied)

	return copied
}

// Paths returns the document's paths in sorted order.
func (d *CompiledDoc) Paths() []string {
	if d == nil {
		return nil
	}
	paths, _ := d.tree["paths"].(map[string]any)

	return sortedKeys(paths)
}

// Operations returns the methods this document describes per path, uppercased
// and sorted.
//
// It is the accessor for the callers that need the served surface as a set
// rather than as a document: a coverage check comparing a router's route table
// against what got published. The alternative is walking [CompiledDoc.Tree] and
// re-deriving which keys under a path item are methods and which are metadata
// (`summary`, `parameters`, `$ref`), and a consumer that got that list wrong
// would report a phantom operation or miss a real one.
func (d *CompiledDoc) Operations() map[string][]string {
	if d == nil {
		return nil
	}
	paths, _ := d.tree["paths"].(map[string]any)
	out := make(map[string][]string, len(paths))
	for _, path := range sortedKeys(paths) {
		item, ok := paths[path].(map[string]any)
		if !ok {
			continue
		}
		methods := make([]string, 0, len(item))
		for _, key := range sortedKeys(item) {
			if !isHTTPMethod(key) {
				continue
			}
			methods = append(methods, strings.ToUpper(key))
		}
		out[path] = methods
	}

	return out
}
