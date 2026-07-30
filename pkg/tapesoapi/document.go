package tapesoapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"
)

// Status reports how current a cached document is.
type Status string

// The states a cached document can be in.
const (
	Fresh   Status = "fresh"
	Stale   Status = "stale"
	Missing Status = "missing"
)

// Document is a parsed OpenAPI document held as a generic tree.
//
// It is deliberately *not* the IR. A document fetched from a cassette is
// republished to clients close to verbatim, and round-tripping it through a
// typed model would silently drop every field this package does not know about
// — including the parts of a future OpenAPI revision. The generic tree is what
// lets core rewrite exactly the paths it must and leave everything else alone.
//
// Use [Document.Fragment] to move a document into the IR for merging.
type Document struct {
	root map[string]any
}

// Parse decodes exactly one JSON object, preserving JSON numbers.
//
// Duplicate keys are rejected rather than last-one-wins. A document that
// declares the same path twice is ambiguous, and picking a winner would make
// core's published surface depend on Go's map iteration.
func Parse(data []byte) (*Document, error) {
	if err := rejectDuplicateKeys(data); err != nil {
		return nil, fmt.Errorf("decode OpenAPI document: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil {
		return nil, fmt.Errorf("decode OpenAPI document: %w", err)
	}
	if root == nil {
		return nil, errors.New("decode OpenAPI document: expected a JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return nil, errors.New("decode OpenAPI document: trailing JSON value")
	} else if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode OpenAPI document trailing data: %w", err)
	}

	return &Document{root: root}, nil
}

// ParseYAML decodes a YAML or JSON document. JSON is valid YAML, so this
// accepts both; [Parse] is the stricter reader used for documents arriving over
// the wire, where duplicate-key ambiguity is a security-relevant surprise
// rather than a typo in a checked-in file.
func ParseYAML(data []byte) (*Document, error) {
	converted, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("decode OpenAPI document as YAML: %w", err)
	}

	return Parse(converted)
}

func rejectDuplicateKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing token %v", token)
		}

		return err
	}

	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()

		return err
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()

		return err
	default:
		return fmt.Errorf("unexpected delimiter %q", delimiter)
	}
}

// Extension returns a root extension encoded as JSON.
func (document *Document) Extension(key string) ([]byte, bool, error) {
	if document == nil {
		return nil, false, errors.New("nil OpenAPI document")
	}
	value, ok := document.root[key]
	if !ok {
		return nil, false, nil
	}
	encoded, err := json.Marshal(value)

	return encoded, true, err
}

// Version reports the version the document declares.
func (document *Document) Version() (Version, error) {
	if document == nil {
		return "", errors.New("nil OpenAPI document")
	}
	declared, _ := document.root["openapi"].(string)

	return ParseVersion(declared)
}

// Paths returns the declared path keys in stable order.
func (document *Document) Paths() ([]string, error) {
	paths, ok := document.root["paths"].(map[string]any)
	if !ok {
		return nil, errors.New("OpenAPI document has no paths object")
	}
	keys := make([]string, 0, len(paths))
	for path := range paths {
		keys = append(keys, path)
	}
	sort.Strings(keys)

	return keys, nil
}

// RewritePrefix returns a copy with every path moved from sourcePrefix to
// targetPrefix and with servers removed so paths resolve against the publisher.
// Servers are stripped at every level they can appear — root, path item, and
// operation — because any surviving override would point generated clients at
// the cassette's private listener instead of the public proxy that
// republishes it.
//
// A path outside sourcePrefix fails the whole rewrite rather than being carried
// over: an operator approves a cassette by name before ever seeing its
// document, and a document that claims a path outside its own prefix is
// claiming surface it was not granted.
// serversKey is the OpenAPI field RewritePrefix strips at every level; see
// that method for why republication may not let any of them survive.
const serversKey = "servers"

func (document *Document) RewritePrefix(sourcePrefix, targetPrefix string) (*Document, error) {
	paths, err := document.Paths()
	if err != nil {
		return nil, err
	}
	root := make(map[string]any, len(document.root))
	for key, value := range document.root {
		if key != serversKey {
			root[key] = value
		}
	}
	rewritten := make(map[string]any, len(paths))
	declared, _ := document.root["paths"].(map[string]any)
	for _, path := range paths {
		if !segmentPrefix(sourcePrefix, path) {
			return nil, fmt.Errorf("path %q is outside %s", path, sourcePrefix)
		}
		rewritten[targetPrefix+strings.TrimPrefix(path, sourcePrefix)] = stripNestedServers(declared[path])
	}
	root["paths"] = rewritten

	return &Document{root: root}, nil
}

// stripNestedServers removes `servers` from a path item and from each of its
// operations. OpenAPI allows the override at both levels, and republication
// is exactly the situation the override must not survive: the proxy is the
// only server the aggregate may name.
func stripNestedServers(value any) any {
	item, ok := value.(map[string]any)
	if !ok {
		return value
	}
	out := make(map[string]any, len(item))
	for key, entry := range item {
		if key == serversKey {
			continue
		}
		if operation, ok := entry.(map[string]any); ok {
			if _, overrides := operation[serversKey]; overrides {
				trimmed := make(map[string]any, len(operation))
				for opKey, opValue := range operation {
					if opKey != serversKey {
						trimmed[opKey] = opValue
					}
				}
				entry = trimmed
			}
		}
		out[key] = entry
	}

	return out
}

func segmentPrefix(prefix, path string) bool {
	return path == prefix || strings.HasPrefix(path, strings.TrimSuffix(prefix, "/")+"/")
}

// Marshal returns stable indented JSON.
func (document *Document) Marshal() ([]byte, error) {
	if document == nil {
		return nil, errors.New("marshal OpenAPI document: nil document")
	}

	return json.MarshalIndent(document.root, "", "  ")
}

// Fragment decomposes the document into the version-neutral IR.
//
// Decomposing at ingest — rather than holding loaded documents and merging them
// at the end — is deliberate: it forces every source through one normalization,
// and it means a malformed document fails at the Add call that supplied it,
// where the file name is still in hand, instead of at a Compile that cannot say
// which of its inputs was wrong.
func (document *Document) Fragment(provenance Provenance) (Fragment, error) {
	if document == nil {
		return Fragment{}, errors.New("nil OpenAPI document")
	}
	encoded, err := json.Marshal(document.root)
	if err != nil {
		return Fragment{}, fmt.Errorf("%s: re-encode document: %w", provenance, err)
	}

	var wire wireDocument
	if err := json.Unmarshal(encoded, &wire); err != nil {
		return Fragment{}, fmt.Errorf("%s: %w", provenance, err)
	}

	version, err := ParseVersion(wire.OpenAPI)
	if err != nil {
		return Fragment{}, fmt.Errorf("%s: %w", provenance, err)
	}

	fragment := Fragment{
		Provenance: provenance,
		Version:    version,
		Security:   wire.Security,
		Extensions: wire.Extensions,
	}
	if wire.Info != nil {
		fragment.Info = &Info{
			Title:          wire.Info.Title,
			Description:    wire.Info.Description,
			TermsOfService: wire.Info.TermsOfService,
			Version:        wire.Info.Version,
			Contact:        wire.Info.Contact,
			Extensions:     wire.Info.Extensions,
		}
		if wire.Info.License != nil {
			fragment.Info.License = &License{
				Name:       wire.Info.License.Name,
				URL:        wire.Info.License.URL,
				Identifier: wire.Info.License.Identifier,
			}
		}
	}
	for _, server := range wire.Servers {
		fragment.Servers = append(fragment.Servers, Server(server))
	}
	for _, tag := range wire.Tags {
		fragment.Tags = append(fragment.Tags, Tag(tag))
	}
	if fragment.Paths, err = convertPathItems(wire.Paths, provenance); err != nil {
		return Fragment{}, err
	}
	// Webhook keys are names, not paths — `orderShipped`, not `/orders`. Running
	// them through path normalization would reject every well-formed 3.1
	// document that declares one.
	if fragment.Webhooks, err = convertWebhooks(wire.Webhooks, provenance); err != nil {
		return Fragment{}, err
	}
	if fragment.Components, err = convertComponents(wire.Components); err != nil {
		return Fragment{}, fmt.Errorf("%s: components: %w", provenance, err)
	}

	return fragment, nil
}

func convertPathItems(in map[string]*wirePathItem, provenance Provenance) (map[string]*PathItem, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]*PathItem, len(in))
	for path, item := range in {
		normalized, err := normalizePath(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", provenance, err)
		}
		if _, duplicate := out[normalized]; duplicate {
			return nil, fmt.Errorf("%s: paths %q and its normalized form collide", provenance, path)
		}
		converted, err := convertPathItem(item, provenance, normalized)
		if err != nil {
			return nil, err
		}
		out[normalized] = converted
	}

	return out, nil
}

// convertWebhooks converts the 3.1 webhooks map, whose keys are event names
// rather than paths.
func convertWebhooks(in map[string]*wirePathItem, provenance Provenance) (map[string]*PathItem, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]*PathItem, len(in))
	for name, item := range in {
		converted, err := convertPathItem(item, provenance, "webhook "+name)
		if err != nil {
			return nil, err
		}
		out[name] = converted
	}

	return out, nil
}

func convertPathItem(in *wirePathItem, provenance Provenance, path string) (*PathItem, error) {
	if in == nil {
		return &PathItem{}, nil
	}
	out := &PathItem{
		Ref:         in.Ref,
		Summary:     in.Summary,
		Description: in.Description,
		Extensions:  in.Extensions,
		Operations:  map[string]*Operation{},
	}
	for _, server := range in.Servers {
		out.Servers = append(out.Servers, Server(server))
	}
	parameters, err := convertParameters(in.Parameters)
	if err != nil {
		return nil, fmt.Errorf("%s: %s: %w", provenance, path, err)
	}
	out.Parameters = parameters

	for method, operation := range in.operations() {
		converted, err := convertOperation(operation)
		if err != nil {
			return nil, fmt.Errorf("%s: %s %s: %w", provenance, method, path, err)
		}
		converted.provenance = provenance
		out.Operations[method] = converted
	}

	return out, nil
}

func convertOperation(in *wireOperation) (*Operation, error) {
	out := &Operation{
		OperationID:  in.OperationID,
		Summary:      in.Summary,
		Description:  in.Description,
		Tags:         in.Tags,
		Deprecated:   in.Deprecated,
		Security:     in.Security,
		ExternalDocs: in.ExternalDocs,
		Extensions:   in.Extensions,
	}
	for _, server := range in.Servers {
		out.Servers = append(out.Servers, Server(server))
	}
	parameters, err := convertParameters(in.Parameters)
	if err != nil {
		return nil, err
	}
	out.Parameters = parameters

	if in.RequestBody != nil {
		body, err := convertRequestBody(in.RequestBody)
		if err != nil {
			return nil, err
		}
		out.RequestBody = body
	}
	if len(in.Responses) > 0 {
		out.Responses = make(map[string]*Response, len(in.Responses))
		for status, response := range in.Responses {
			converted, err := convertResponse(response)
			if err != nil {
				return nil, fmt.Errorf("response %s: %w", status, err)
			}
			out.Responses[status] = converted
		}
	}

	return out, nil
}

func convertParameters(in []*wireParameter) ([]*Parameter, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]*Parameter, 0, len(in))
	for _, parameter := range in {
		converted, err := convertParameter(parameter)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}

	return out, nil
}

func convertParameter(in *wireParameter) (*Parameter, error) {
	if in == nil {
		return nil, errors.New("nil parameter")
	}
	if in.Ref != "" {
		return &Parameter{Ref: in.Ref}, nil
	}
	schema, err := in.Schema.schema()
	if err != nil {
		return nil, fmt.Errorf("parameter %q: %w", in.Name, err)
	}

	return &Parameter{
		Name:        in.Name,
		In:          ParameterIn(in.In),
		Description: in.Description,
		Required:    in.Required,
		Deprecated:  in.Deprecated,
		Schema:      schema,
		Example:     in.Example,
		Style:       in.Style,
		Explode:     in.Explode,
		Extensions:  in.Extensions,
	}, nil
}

func convertRequestBody(in *wireRequestBody) (*RequestBody, error) {
	if in.Ref != "" {
		return &RequestBody{Ref: in.Ref}, nil
	}
	content, err := convertContent(in.Content)
	if err != nil {
		return nil, err
	}

	return &RequestBody{
		Description: in.Description,
		Required:    in.Required,
		Content:     content,
		Extensions:  in.Extensions,
	}, nil
}

func convertResponse(in *wireResponse) (*Response, error) {
	if in == nil {
		return &Response{}, nil
	}
	if in.Ref != "" {
		return &Response{Ref: in.Ref}, nil
	}
	content, err := convertContent(in.Content)
	if err != nil {
		return nil, err
	}
	out := &Response{
		Description: in.Description,
		Content:     content,
		Extensions:  in.Extensions,
	}
	if len(in.Headers) > 0 {
		out.Headers = make(map[string]*Header, len(in.Headers))
		for name, header := range in.Headers {
			converted, err := convertHeader(header)
			if err != nil {
				return nil, fmt.Errorf("header %q: %w", name, err)
			}
			out.Headers[name] = converted
		}
	}

	return out, nil
}

func convertHeader(in *wireHeader) (*Header, error) {
	if in == nil {
		return nil, errors.New("nil header")
	}
	if in.Ref != "" {
		return &Header{Ref: in.Ref}, nil
	}
	schema, err := in.Schema.schema()
	if err != nil {
		return nil, err
	}

	return &Header{
		Description: in.Description,
		Required:    in.Required,
		Deprecated:  in.Deprecated,
		Schema:      schema,
	}, nil
}

func convertContent(in map[string]*wireMediaType) (map[string]*MediaType, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make(map[string]*MediaType, len(in))
	for mediaType, entry := range in {
		if entry == nil {
			out[mediaType] = &MediaType{}

			continue
		}
		schema, err := entry.Schema.schema()
		if err != nil {
			return nil, fmt.Errorf("content %q: %w", mediaType, err)
		}
		converted := &MediaType{
			Schema:     schema,
			Example:    entry.Example,
			Examples:   entry.Examples,
			Extensions: entry.Extensions,
		}
		if len(entry.Encoding) > 0 {
			converted.Encoding = make(map[string]*Encoding, len(entry.Encoding))
			for name, encoding := range entry.Encoding {
				headers := map[string]*Header{}
				for headerName, header := range encoding.Headers {
					convertedHeader, err := convertHeader(header)
					if err != nil {
						return nil, err
					}
					headers[headerName] = convertedHeader
				}
				converted.Encoding[name] = &Encoding{
					ContentType: encoding.ContentType,
					Style:       encoding.Style,
					Explode:     encoding.Explode,
					Headers:     headers,
				}
			}
		}
		out[mediaType] = converted
	}

	return out, nil
}

func convertComponents(in *wireComponents) (*Components, error) {
	if in == nil {
		return nil, nil
	}
	out := &Components{Examples: in.Examples}
	var err error

	if len(in.Schemas) > 0 {
		out.Schemas = make(map[string]*Schema, len(in.Schemas))
		for name, schema := range in.Schemas {
			if out.Schemas[name], err = schema.schema(); err != nil {
				return nil, fmt.Errorf("schemas/%s: %w", name, err)
			}
		}
	}
	if len(in.Responses) > 0 {
		out.Responses = make(map[string]*Response, len(in.Responses))
		for name, response := range in.Responses {
			if out.Responses[name], err = convertResponse(response); err != nil {
				return nil, fmt.Errorf("responses/%s: %w", name, err)
			}
		}
	}
	if len(in.Parameters) > 0 {
		out.Parameters = make(map[string]*Parameter, len(in.Parameters))
		for name, parameter := range in.Parameters {
			if out.Parameters[name], err = convertParameter(parameter); err != nil {
				return nil, fmt.Errorf("parameters/%s: %w", name, err)
			}
		}
	}
	if len(in.RequestBodies) > 0 {
		out.RequestBodies = make(map[string]*RequestBody, len(in.RequestBodies))
		for name, body := range in.RequestBodies {
			if out.RequestBodies[name], err = convertRequestBody(body); err != nil {
				return nil, fmt.Errorf("requestBodies/%s: %w", name, err)
			}
		}
	}
	if len(in.Headers) > 0 {
		out.Headers = make(map[string]*Header, len(in.Headers))
		for name, header := range in.Headers {
			if out.Headers[name], err = convertHeader(header); err != nil {
				return nil, fmt.Errorf("headers/%s: %w", name, err)
			}
		}
	}
	if len(in.SecuritySchemes) > 0 {
		out.SecuritySchemes = make(map[string]*SecurityScheme, len(in.SecuritySchemes))
		for name, scheme := range in.SecuritySchemes {
			out.SecuritySchemes[name] = &SecurityScheme{
				Type:             scheme.Type,
				Description:      scheme.Description,
				Name:             scheme.Name,
				In:               scheme.In,
				Scheme:           scheme.Scheme,
				BearerFormat:     scheme.BearerFormat,
				OpenIDConnectURL: scheme.OpenIDConnectURL,
				Flows:            scheme.Flows,
				Extensions:       scheme.Extensions,
			}
		}
	}

	return out, nil
}
