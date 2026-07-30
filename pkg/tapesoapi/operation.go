package tapesoapi

import "strconv"

// OperationBuilder describes one operation fluently.
//
// It exists because the alternative — writing struct literals four levels deep
// to say "this returns a User" — is what pushed the previous generation of Go
// OpenAPI tooling into doc comments. A builder keeps the description in Go,
// where the compiler checks the types being described.
//
//	op := tapesoapi.NewOperation("getUser").
//		Summary("Fetch a user by ID").
//		Tag("users").
//		PathParam("id", tapesoapi.String(tapesoapi.Format("uuid"))).
//		QueryParam("expand", tapesoapi.String()).
//		JSONResponse(200, "the user", userSchema)
type OperationBuilder struct {
	operation *Operation
}

// NewOperation starts an operation with the given operationId.
//
// The id is required rather than optional because downstream generators need
// one — progenitor, which builds paper's Rust client from the compiled
// contract, hard-fails without it — and a synthesized id changes whenever the
// path does, silently renaming a client method.
func NewOperation(operationID string) *OperationBuilder {
	return &OperationBuilder{operation: &Operation{
		OperationID: operationID,
		Responses:   map[string]*Response{},
	}}
}

// Summary sets the short description.
func (b *OperationBuilder) Summary(text string) *OperationBuilder {
	b.operation.Summary = text

	return b
}

// Description sets the long description.
func (b *OperationBuilder) Description(text string) *OperationBuilder {
	b.operation.Description = text

	return b
}

// Tag adds one or more tags.
func (b *OperationBuilder) Tag(tags ...string) *OperationBuilder {
	b.operation.Tags = append(b.operation.Tags, tags...)

	return b
}

// Deprecated marks the operation deprecated.
func (b *OperationBuilder) Deprecated() *OperationBuilder {
	b.operation.Deprecated = true

	return b
}

// Extension sets a vendor extension on the operation.
func (b *OperationBuilder) Extension(key string, value any) *OperationBuilder {
	if b.operation.Extensions == nil {
		b.operation.Extensions = map[string]any{}
	}
	b.operation.Extensions[extensionKey(key)] = value

	return b
}

// ParamOption adjusts a parameter after its schema is set.
type ParamOption func(*Parameter)

// ParamDescription documents a parameter.
func ParamDescription(text string) ParamOption {
	return func(p *Parameter) { p.Description = text }
}

// ParamRequired marks a parameter required. Path parameters are required
// implicitly; this is for the query and header ones that are not.
func ParamRequired() ParamOption { return func(p *Parameter) { p.Required = true } }

// ParamExample sets an example value for a parameter.
func ParamExample(value any) ParamOption { return func(p *Parameter) { p.Example = value } }

// ParamDeprecated marks a parameter deprecated.
func ParamDeprecated() ParamOption { return func(p *Parameter) { p.Deprecated = true } }

// ParamStyle sets the serialization style, for the array and object parameters
// where the default is ambiguous.
func ParamStyle(style string, explode bool) ParamOption {
	return func(p *Parameter) {
		p.Style = style
		p.Explode = &explode
	}
}

func (b *OperationBuilder) param(in ParameterIn, name string, schema *Schema, opts []ParamOption) *OperationBuilder {
	parameter := &Parameter{
		Name:   name,
		In:     in,
		Schema: schema,
		// A path parameter that is not required is not expressible in OpenAPI,
		// so it is set here rather than left to the caller to remember.
		Required: in == InPath,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(parameter)
		}
	}
	if in == InPath {
		parameter.Required = true
	}
	b.operation.Parameters = append(b.operation.Parameters, parameter)

	return b
}

// PathParam declares a path template parameter.
func (b *OperationBuilder) PathParam(name string, schema *Schema, opts ...ParamOption) *OperationBuilder {
	return b.param(InPath, name, schema, opts)
}

// QueryParam declares a query-string parameter. Optional unless
// [ParamRequired] is passed.
func (b *OperationBuilder) QueryParam(name string, schema *Schema, opts ...ParamOption) *OperationBuilder {
	return b.param(InQuery, name, schema, opts)
}

// HeaderParam declares a request-header parameter.
func (b *OperationBuilder) HeaderParam(name string, schema *Schema, opts ...ParamOption) *OperationBuilder {
	return b.param(InHeader, name, schema, opts)
}

// CookieParam declares a cookie parameter.
func (b *OperationBuilder) CookieParam(name string, schema *Schema, opts ...ParamOption) *OperationBuilder {
	return b.param(InCookie, name, schema, opts)
}

// RequestBody sets the request body from a content map.
func (b *OperationBuilder) RequestBody(description string, required bool, content map[string]*MediaType) *OperationBuilder {
	b.operation.RequestBody = &RequestBody{
		Description: description,
		Required:    required,
		Content:     content,
	}

	return b
}

// JSONBody sets a required application/json request body.
func (b *OperationBuilder) JSONBody(description string, schema *Schema) *OperationBuilder {
	return b.RequestBody(description, true, JSON(schema))
}

// OptionalJSONBody sets an optional application/json request body.
func (b *OperationBuilder) OptionalJSONBody(description string, schema *Schema) *OperationBuilder {
	return b.RequestBody(description, false, JSON(schema))
}

// Response records an outcome under a status key.
func (b *OperationBuilder) Response(status int, response *Response) *OperationBuilder {
	return b.ResponseKey(strconv.Itoa(status), response)
}

// ResponseKey records an outcome under an arbitrary key, for "default" and the
// `4XX` wildcard forms a numeric status cannot express.
func (b *OperationBuilder) ResponseKey(key string, response *Response) *OperationBuilder {
	if b.operation.Responses == nil {
		b.operation.Responses = map[string]*Response{}
	}
	b.operation.Responses[key] = response

	return b
}

// JSONResponse records an application/json outcome.
func (b *OperationBuilder) JSONResponse(status int, description string, schema *Schema) *OperationBuilder {
	return b.Response(status, &Response{Description: description, Content: JSON(schema)})
}

// ContentResponse records an outcome with an arbitrary media type.
func (b *OperationBuilder) ContentResponse(status int, description, mediaType string, schema *Schema) *OperationBuilder {
	return b.Response(status, &Response{Description: description, Content: Content(mediaType, schema)})
}

// EmptyResponse records an outcome with no body, such as a 204.
func (b *OperationBuilder) EmptyResponse(status int, description string) *OperationBuilder {
	return b.Response(status, &Response{Description: description})
}

// Security adds a security requirement. Repeated calls are alternatives: any
// one of them satisfies the operation, which is how OpenAPI reads a list.
func (b *OperationBuilder) Security(requirement SecurityRequirement) *OperationBuilder {
	b.operation.Security = append(b.operation.Security, requirement)

	return b
}

// Build returns the described operation.
func (b *OperationBuilder) Build() *Operation { return b.operation.clone() }

// declarePathParams fills in any `{param}` the path declares and the operation
// does not.
//
// Both versions require every path template parameter to be described, and the
// route pattern already proves each one exists — so making the caller restate
// it adds a way to be wrong without adding any information. A string is the
// honest default: the router does not constrain it either.
//
// It runs where the path and the operation first meet, so every caller gets it:
// a hand-built operation, a Fiber route registration, and an ingested document
// alike.
func declarePathParams(operation *Operation, path string) {
	declared := make(map[string]struct{}, len(operation.Parameters))
	for _, parameter := range operation.Parameters {
		if parameter.In == InPath {
			declared[parameter.Name] = struct{}{}
		}
	}
	for _, name := range PathParams(path) {
		if _, ok := declared[name]; ok {
			continue
		}
		declared[name] = struct{}{}
		operation.Parameters = append(operation.Parameters, &Parameter{
			Name:     name,
			In:       InPath,
			Required: true,
			Schema:   String(),
		})
	}
}

// ResponseHeader attaches a header to an already-recorded response.
func (b *OperationBuilder) ResponseHeader(status int, name string, header *Header) *OperationBuilder {
	key := strconv.Itoa(status)
	response, ok := b.operation.Responses[key]
	if !ok {
		response = &Response{}
		b.operation.Responses[key] = response
	}
	if response.Headers == nil {
		response.Headers = map[string]*Header{}
	}
	response.Headers[name] = header

	return b
}
