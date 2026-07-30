package tapesoapi

import (
	"sort"
	"strings"
)

// Component reference prefixes. Every component section this package handles
// gets one, because namespacing an ingested document has to rewrite refs into
// each of them, not just schemas.
const (
	componentsSchemaPrefix         = "#/components/schemas/"
	componentsResponsePrefix       = "#/components/responses/"
	componentsParameterPrefix      = "#/components/parameters/"
	componentsRequestBodyPrefix    = "#/components/requestBodies/"
	componentsHeaderPrefix         = "#/components/headers/"
	componentsExamplePrefix        = "#/components/examples/"
	componentsSecuritySchemePrefix = "#/components/securitySchemes/"
)

// The specification spends the word "default" on two unrelated things, and both
// come up often enough here to name.
const (
	// defaultResponseKey is the response-map key reserved for outcomes not
	// listed by status code. It is the one key in that map that is not a number,
	// which is why ordering, validation, and rendering each make an exception
	// for it.
	defaultResponseKey = "default"

	// defaultKeyword is the `default` field of a schema or a server variable —
	// a value, not an outcome. It doubles as the struct-tag name that sets one.
	defaultKeyword = "default"
)

// Info describes the API as a whole.
type Info struct {
	Title          string
	Description    string
	TermsOfService string
	Version        string
	Contact        *Contact
	License        *License
	Extensions     map[string]any
}

// Contact is the API's contact information.
type Contact struct {
	Name  string
	URL   string
	Email string
}

// License is the API's license.
type License struct {
	Name string
	URL  string

	// Identifier is an SPDX expression. It is 3.1-only; rendering to 3.0 drops
	// it in favour of Name, which 3.0 requires anyway.
	Identifier string
}

// Server is one base URL the API is served from.
type Server struct {
	URL         string
	Description string
	Variables   map[string]*ServerVariable
}

// ServerVariable is a substitution in a server URL template.
type ServerVariable struct {
	Default     string
	Enum        []string
	Description string
}

// Tag groups operations.
type Tag struct {
	Name         string
	Description  string
	ExternalDocs *ExternalDocs
}

// ExternalDocs points at documentation outside the spec.
type ExternalDocs struct {
	Description string
	URL         string
}

// ParameterIn is where a parameter is carried.
type ParameterIn string

// The parameter locations OpenAPI defines.
const (
	InPath   ParameterIn = "path"
	InQuery  ParameterIn = "query"
	InHeader ParameterIn = "header"
	InCookie ParameterIn = "cookie"
)

// Parameter is one operation input.
type Parameter struct {
	// Ref makes this a reference to a component parameter; the other fields
	// are ignored when it is set.
	Ref string

	Name        string
	In          ParameterIn
	Description string
	Required    bool
	Deprecated  bool
	Schema      *Schema
	Example     any
	Style       string
	Explode     *bool
	Extensions  map[string]any
}

// MediaType is one content-type entry of a body.
type MediaType struct {
	Schema     *Schema
	Example    any
	Examples   map[string]any
	Encoding   map[string]*Encoding
	Extensions map[string]any
}

// Encoding describes how a request-body property is serialized.
type Encoding struct {
	ContentType string
	Style       string
	Explode     *bool
	Headers     map[string]*Header
}

// RequestBody is an operation's input payload.
type RequestBody struct {
	Ref         string
	Description string
	Required    bool
	Content     map[string]*MediaType
	Extensions  map[string]any
}

// Header is a response header.
type Header struct {
	Ref         string
	Description string
	Required    bool
	Deprecated  bool
	Schema      *Schema
}

// Response is one operation outcome.
type Response struct {
	Ref string

	// Description is required by the spec for every response object. An empty
	// one is filled in at compile time rather than failing, because a missing
	// description is a documentation gap, not a structural defect.
	Description string
	Content     map[string]*MediaType
	Headers     map[string]*Header
	Extensions  map[string]any
}

// SecurityRequirement names schemes an operation requires, with their scopes.
// The map is a disjunction of conjunctions exactly as OpenAPI defines it.
type SecurityRequirement map[string][]string

// SecurityScheme declares an authentication mechanism.
type SecurityScheme struct {
	Type             string
	Description      string
	Name             string
	In               string
	Scheme           string
	BearerFormat     string
	OpenIDConnectURL string
	Flows            map[string]any
	Extensions       map[string]any
}

// Operation is one method on one path.
type Operation struct {
	OperationID  string
	Summary      string
	Description  string
	Tags         []string
	Deprecated   bool
	Parameters   []*Parameter
	RequestBody  *RequestBody
	Responses    map[string]*Response
	Security     []SecurityRequirement
	Servers      []Server
	ExternalDocs *ExternalDocs
	Extensions   map[string]any

	// provenance records where this operation came from, for conflict errors.
	// It is unexported because it is bookkeeping the compiler sets, not part of
	// the document a caller describes.
	provenance Provenance
}

// PathItem is every operation on one path, plus what they share.
type PathItem struct {
	Ref         string
	Summary     string
	Description string
	Servers     []Server

	// Parameters apply to every operation on this path.
	Parameters []*Parameter

	// Operations is keyed by uppercase HTTP method.
	Operations map[string]*Operation

	Extensions map[string]any
}

// httpMethods is the set of path-item keys OpenAPI defines as operations.
//
// A path item also holds `summary`, `description`, `servers`, `parameters`, and
// `$ref` — plus any `x-` extension — so "a key under a path" and "a method" are
// not the same thing, and a reader that assumed they were would report the
// shared parameter list as an operation.
var httpMethods = map[string]bool{
	"get": true, "put": true, "post": true, "delete": true,
	"options": true, "head": true, "patch": true, "trace": true,
}

// isHTTPMethod reports whether a path-item key names an operation. The check is
// case-insensitive because the IR keys methods uppercase and the rendered
// document keys them lowercase.
func isHTTPMethod(key string) bool {
	return httpMethods[strings.ToLower(key)]
}

// Methods returns this item's methods in a stable order.
func (p *PathItem) Methods() []string {
	if p == nil {
		return nil
	}
	methods := make([]string, 0, len(p.Operations))
	for method := range p.Operations {
		methods = append(methods, method)
	}
	sort.Strings(methods)

	return methods
}

// Components is the reusable-object section of a document.
type Components struct {
	Schemas         map[string]*Schema
	Responses       map[string]*Response
	Parameters      map[string]*Parameter
	RequestBodies   map[string]*RequestBody
	Headers         map[string]*Header
	Examples        map[string]any
	SecuritySchemes map[string]*SecurityScheme
}

// IsEmpty reports whether there is nothing to render.
func (c *Components) IsEmpty() bool {
	return c == nil ||
		len(c.Schemas)+len(c.Responses)+len(c.Parameters)+len(c.RequestBodies)+
			len(c.Headers)+len(c.Examples)+len(c.SecuritySchemes) == 0
}

// JSON returns a single-entry application/json content map over schema. It is
// the shorthand almost every operation in a JSON API needs.
func JSON(schema *Schema) map[string]*MediaType {
	return map[string]*MediaType{"application/json": {Schema: schema}}
}

// Content returns a single-entry content map for an arbitrary media type.
func Content(mediaType string, schema *Schema) map[string]*MediaType {
	return map[string]*MediaType{mediaType: {Schema: schema}}
}

// Text returns a text/plain content map.
func Text(schema *Schema) map[string]*MediaType {
	return map[string]*MediaType{"text/plain": {Schema: schema}}
}
