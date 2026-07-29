package oasfiber

import (
	oas "github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// DocBuilder describes the operation a route implements.
//
// It is a thin wrapper over [oas.OperationBuilder], adding only what a route
// registration needs on top of a plain operation: a method binding for the
// multi-verb mounts, and the router-level tags.
//
// A path parameter the caller does not describe is filled in as a required
// string when the operation reaches the parser, which knows the path — so a
// route rename cannot leave behind a stale parameter list.
type DocBuilder struct {
	builder *oas.OperationBuilder

	// method is set only by [DocFor], for the app.All routes where one
	// registration describes several methods.
	method string
}

// Doc starts a route description with the given operationId.
func Doc(operationID string) *DocBuilder {
	return &DocBuilder{builder: oas.NewOperation(operationID)}
}

// DocFor starts a route description bound to one method, for use with
// [Router.All] where a single registration serves several verbs.
func DocFor(method, operationID string) *DocBuilder {
	doc := Doc(operationID)
	doc.method = method

	return doc
}

// Summary sets the short description.
func (d *DocBuilder) Summary(text string) *DocBuilder {
	d.builder.Summary(text)

	return d
}

// Description sets the long description.
func (d *DocBuilder) Description(text string) *DocBuilder {
	d.builder.Description(text)

	return d
}

// Tag adds tags.
func (d *DocBuilder) Tag(tags ...string) *DocBuilder {
	d.builder.Tag(tags...)

	return d
}

// Deprecated marks the operation deprecated.
func (d *DocBuilder) Deprecated() *DocBuilder {
	d.builder.Deprecated()

	return d
}

// Extension sets a vendor extension.
func (d *DocBuilder) Extension(key string, value any) *DocBuilder {
	d.builder.Extension(key, value)

	return d
}

// PathParam documents a path parameter. Only needed to give one a schema,
// description, or example — an undescribed path parameter is filled in as a
// required string.
func (d *DocBuilder) PathParam(name string, schema *oas.Schema, opts ...oas.ParamOption) *DocBuilder {
	d.builder.PathParam(name, schema, opts...)

	return d
}

// QueryParam documents a query parameter.
func (d *DocBuilder) QueryParam(name string, schema *oas.Schema, opts ...oas.ParamOption) *DocBuilder {
	d.builder.QueryParam(name, schema, opts...)

	return d
}

// HeaderParam documents a request header.
func (d *DocBuilder) HeaderParam(name string, schema *oas.Schema, opts ...oas.ParamOption) *DocBuilder {
	d.builder.HeaderParam(name, schema, opts...)

	return d
}

// JSONBody documents a required application/json request body.
func (d *DocBuilder) JSONBody(description string, schema *oas.Schema) *DocBuilder {
	d.builder.JSONBody(description, schema)

	return d
}

// OptionalJSONBody documents an optional application/json request body.
func (d *DocBuilder) OptionalJSONBody(description string, schema *oas.Schema) *DocBuilder {
	d.builder.OptionalJSONBody(description, schema)

	return d
}

// JSONResponse documents an application/json outcome.
func (d *DocBuilder) JSONResponse(status int, description string, schema *oas.Schema) *DocBuilder {
	d.builder.JSONResponse(status, description, schema)

	return d
}

// ContentResponse documents an outcome with an explicit media type.
func (d *DocBuilder) ContentResponse(status int, description, mediaType string, schema *oas.Schema) *DocBuilder {
	d.builder.ContentResponse(status, description, mediaType, schema)

	return d
}

// EmptyResponse documents an outcome with no body.
func (d *DocBuilder) EmptyResponse(status int, description string) *DocBuilder {
	d.builder.EmptyResponse(status, description)

	return d
}

// Response documents an outcome directly.
func (d *DocBuilder) Response(status int, response *oas.Response) *DocBuilder {
	d.builder.Response(status, response)

	return d
}

// Security adds a security requirement.
func (d *DocBuilder) Security(requirement oas.SecurityRequirement) *DocBuilder {
	d.builder.Security(requirement)

	return d
}

// build finalizes the operation, applying router-level tags.
func (d *DocBuilder) build(routerTags []string) *oas.Operation {
	if len(routerTags) > 0 {
		d.builder.Tag(routerTags...)
	}

	return d.builder.Build()
}

// Method reports the method a [DocFor] doc is bound to, empty for a plain
// [Doc].
func (d *DocBuilder) Method() string { return d.method }
