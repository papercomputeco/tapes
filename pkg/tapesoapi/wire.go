package tapesoapi

import (
	"encoding/json"
	"fmt"
	"strings"
)

// This file decodes the on-the-wire form of an OpenAPI document into the
// version-neutral IR. Both 3.0 and 3.1 land here; the two places they disagree
// about how to say the same thing — nullability and exclusive bounds — are
// reconciled into the IR's single representation, so nothing downstream of
// ingestion has to ask which version a fragment came from.

// extensionsFrom pulls the `x-` keys out of a raw object. Every wire type calls
// it from UnmarshalJSON, which is what preserves vendor extensions through a
// round trip instead of silently dropping the parts of a document this package
// does not model — including x-tapes-cassette, the manifest a cassette embeds.
func extensionsFrom(data []byte) map[string]any {
	var all map[string]any
	if err := json.Unmarshal(data, &all); err != nil {
		return nil
	}
	var out map[string]any
	for key, value := range all {
		if !strings.HasPrefix(key, "x-") {
			continue
		}
		if out == nil {
			out = map[string]any{}
		}
		out[key] = value
	}

	return out
}

type wireDocument struct {
	OpenAPI    string                   `json:"openapi"`
	Info       *wireInfo                `json:"info"`
	Servers    []wireServer             `json:"servers"`
	Tags       []wireTag                `json:"tags"`
	Security   []SecurityRequirement    `json:"security"`
	Paths      map[string]*wirePathItem `json:"paths"`
	Webhooks   map[string]*wirePathItem `json:"webhooks"`
	Components *wireComponents          `json:"components"`
	Extensions map[string]any           `json:"-"`
}

func (w *wireDocument) UnmarshalJSON(data []byte) error {
	type alias wireDocument
	if err := json.Unmarshal(data, (*alias)(w)); err != nil { //nolint:musttag // the local alias carries the same json tags; musttag cannot see through it
		return err
	}
	w.Extensions = extensionsFrom(data)

	return nil
}

type wireInfo struct {
	Title          string         `json:"title"`
	Description    string         `json:"description"`
	TermsOfService string         `json:"termsOfService"`
	Version        string         `json:"version"`
	Contact        *Contact       `json:"contact"`
	License        *wireLicense   `json:"license"`
	Extensions     map[string]any `json:"-"`
}

func (w *wireInfo) UnmarshalJSON(data []byte) error {
	type alias wireInfo
	if err := json.Unmarshal(data, (*alias)(w)); err != nil { //nolint:musttag // the local alias carries the same json tags; musttag cannot see through it
		return err
	}
	w.Extensions = extensionsFrom(data)

	return nil
}

type wireLicense struct {
	Name       string `json:"name"`
	URL        string `json:"url"`
	Identifier string `json:"identifier"`
}

type wireServer struct {
	URL         string                     `json:"url"`
	Description string                     `json:"description"`
	Variables   map[string]*ServerVariable `json:"variables"`
}

type wireTag struct {
	Name         string        `json:"name"`
	Description  string        `json:"description"`
	ExternalDocs *ExternalDocs `json:"externalDocs"`
}

type wirePathItem struct {
	Ref         string           `json:"$ref"`
	Summary     string           `json:"summary"`
	Description string           `json:"description"`
	Servers     []wireServer     `json:"servers"`
	Parameters  []*wireParameter `json:"parameters"`

	Get     *wireOperation `json:"get"`
	Put     *wireOperation `json:"put"`
	Post    *wireOperation `json:"post"`
	Delete  *wireOperation `json:"delete"`
	Options *wireOperation `json:"options"`
	Head    *wireOperation `json:"head"`
	Patch   *wireOperation `json:"patch"`
	Trace   *wireOperation `json:"trace"`

	Extensions map[string]any `json:"-"`
}

func (w *wirePathItem) UnmarshalJSON(data []byte) error {
	type alias wirePathItem
	if err := json.Unmarshal(data, (*alias)(w)); err != nil { //nolint:musttag // the local alias carries the same json tags; musttag cannot see through it
		return err
	}
	w.Extensions = extensionsFrom(data)

	return nil
}

func (w *wirePathItem) operations() map[string]*wireOperation {
	out := map[string]*wireOperation{}
	for method, operation := range map[string]*wireOperation{
		"GET": w.Get, "PUT": w.Put, "POST": w.Post, "DELETE": w.Delete,
		"OPTIONS": w.Options, "HEAD": w.Head, "PATCH": w.Patch, "TRACE": w.Trace,
	} {
		if operation != nil {
			out[method] = operation
		}
	}

	return out
}

type wireOperation struct {
	OperationID  string                   `json:"operationId"`
	Summary      string                   `json:"summary"`
	Description  string                   `json:"description"`
	Tags         []string                 `json:"tags"`
	Deprecated   bool                     `json:"deprecated"`
	Parameters   []*wireParameter         `json:"parameters"`
	RequestBody  *wireRequestBody         `json:"requestBody"`
	Responses    map[string]*wireResponse `json:"responses"`
	Security     []SecurityRequirement    `json:"security"`
	Servers      []wireServer             `json:"servers"`
	ExternalDocs *ExternalDocs            `json:"externalDocs"`
	Extensions   map[string]any           `json:"-"`
}

func (w *wireOperation) UnmarshalJSON(data []byte) error {
	type alias wireOperation
	if err := json.Unmarshal(data, (*alias)(w)); err != nil { //nolint:musttag // the local alias carries the same json tags; musttag cannot see through it
		return err
	}
	w.Extensions = extensionsFrom(data)

	return nil
}

type wireParameter struct {
	Ref         string         `json:"$ref"`
	Name        string         `json:"name"`
	In          string         `json:"in"`
	Description string         `json:"description"`
	Required    bool           `json:"required"`
	Deprecated  bool           `json:"deprecated"`
	Schema      *wireSchema    `json:"schema"`
	Example     any            `json:"example"`
	Style       string         `json:"style"`
	Explode     *bool          `json:"explode"`
	Extensions  map[string]any `json:"-"`
}

func (w *wireParameter) UnmarshalJSON(data []byte) error {
	type alias wireParameter
	if err := json.Unmarshal(data, (*alias)(w)); err != nil {
		return err
	}
	w.Extensions = extensionsFrom(data)

	return nil
}

type wireRequestBody struct {
	Ref         string                    `json:"$ref"`
	Description string                    `json:"description"`
	Required    bool                      `json:"required"`
	Content     map[string]*wireMediaType `json:"content"`
	Extensions  map[string]any            `json:"-"`
}

func (w *wireRequestBody) UnmarshalJSON(data []byte) error {
	type alias wireRequestBody
	if err := json.Unmarshal(data, (*alias)(w)); err != nil {
		return err
	}
	w.Extensions = extensionsFrom(data)

	return nil
}

type wireResponse struct {
	Ref         string                    `json:"$ref"`
	Description string                    `json:"description"`
	Content     map[string]*wireMediaType `json:"content"`
	Headers     map[string]*wireHeader    `json:"headers"`
	Extensions  map[string]any            `json:"-"`
}

func (w *wireResponse) UnmarshalJSON(data []byte) error {
	type alias wireResponse
	if err := json.Unmarshal(data, (*alias)(w)); err != nil {
		return err
	}
	w.Extensions = extensionsFrom(data)

	return nil
}

type wireHeader struct {
	Ref         string      `json:"$ref"`
	Description string      `json:"description"`
	Required    bool        `json:"required"`
	Deprecated  bool        `json:"deprecated"`
	Schema      *wireSchema `json:"schema"`
}

type wireMediaType struct {
	Schema     *wireSchema              `json:"schema"`
	Example    any                      `json:"example"`
	Examples   map[string]any           `json:"examples"`
	Encoding   map[string]*wireEncoding `json:"encoding"`
	Extensions map[string]any           `json:"-"`
}

func (w *wireMediaType) UnmarshalJSON(data []byte) error {
	type alias wireMediaType
	if err := json.Unmarshal(data, (*alias)(w)); err != nil {
		return err
	}
	w.Extensions = extensionsFrom(data)

	return nil
}

type wireEncoding struct {
	ContentType string                 `json:"contentType"`
	Style       string                 `json:"style"`
	Explode     *bool                  `json:"explode"`
	Headers     map[string]*wireHeader `json:"headers"`
}

type wireComponents struct {
	Schemas         map[string]*wireSchema      `json:"schemas"`
	Responses       map[string]*wireResponse    `json:"responses"`
	Parameters      map[string]*wireParameter   `json:"parameters"`
	RequestBodies   map[string]*wireRequestBody `json:"requestBodies"`
	Headers         map[string]*wireHeader      `json:"headers"`
	Examples        map[string]any              `json:"examples"`
	SecuritySchemes map[string]*wireSecurity    `json:"securitySchemes"`
}

type wireSecurity struct {
	Type             string         `json:"type"`
	Description      string         `json:"description"`
	Name             string         `json:"name"`
	In               string         `json:"in"`
	Scheme           string         `json:"scheme"`
	BearerFormat     string         `json:"bearerFormat"`
	OpenIDConnectURL string         `json:"openIdConnectUrl"`
	Flows            map[string]any `json:"flows"`
	Extensions       map[string]any `json:"-"`
}

func (w *wireSecurity) UnmarshalJSON(data []byte) error {
	type alias wireSecurity
	if err := json.Unmarshal(data, (*alias)(w)); err != nil {
		return err
	}
	w.Extensions = extensionsFrom(data)

	return nil
}

// wireSchema holds the union of 3.0 and 3.1 spellings. The fields that differ
// between versions are decoded as raw JSON and reconciled in schema(), because
// their *type* changes across versions, not just their meaning.
type wireSchema struct {
	Ref string `json:"$ref"`

	// Type is a string in 3.0 and may be an array in 3.1.
	Type json.RawMessage `json:"type"`

	Format      string `json:"format"`
	Title       string `json:"title"`
	Description string `json:"description"`

	// Nullable is 3.0-only; 3.1 expresses it through the type union.
	Nullable bool `json:"nullable"`

	Default  any   `json:"default"`
	Example  any   `json:"example"`
	Examples []any `json:"examples"`
	Enum     []any `json:"enum"`
	Const    any   `json:"const"`

	Minimum *float64 `json:"minimum"`
	Maximum *float64 `json:"maximum"`

	// Exclusive bounds are booleans in 3.0 (modifying minimum/maximum) and
	// numbers in 3.1 (standing alone).
	ExclusiveMinimum json.RawMessage `json:"exclusiveMinimum"`
	ExclusiveMaximum json.RawMessage `json:"exclusiveMaximum"`

	MultipleOf *float64 `json:"multipleOf"`

	MinLength *uint64 `json:"minLength"`
	MaxLength *uint64 `json:"maxLength"`
	Pattern   string  `json:"pattern"`

	Items       *wireSchema `json:"items"`
	MinItems    *uint64     `json:"minItems"`
	MaxItems    *uint64     `json:"maxItems"`
	UniqueItems bool        `json:"uniqueItems"`

	Properties    map[string]*wireSchema `json:"properties"`
	Required      []string               `json:"required"`
	MinProperties *uint64                `json:"minProperties"`
	MaxProperties *uint64                `json:"maxProperties"`

	// AdditionalProperties is a schema or a boolean.
	AdditionalProperties json.RawMessage `json:"additionalProperties"`

	OneOf         []*wireSchema  `json:"oneOf"`
	AnyOf         []*wireSchema  `json:"anyOf"`
	AllOf         []*wireSchema  `json:"allOf"`
	Not           *wireSchema    `json:"not"`
	Discriminator *Discriminator `json:"discriminator"`

	ReadOnly   bool `json:"readOnly"`
	WriteOnly  bool `json:"writeOnly"`
	Deprecated bool `json:"deprecated"`

	Extensions map[string]any `json:"-"`

	// hasConst distinguishes `const: null` from an absent const.
	hasConst bool
}

func (w *wireSchema) UnmarshalJSON(data []byte) error {
	type alias wireSchema
	if err := json.Unmarshal(data, (*alias)(w)); err != nil { //nolint:musttag // the local alias carries the same json tags; musttag cannot see through it
		return err
	}
	w.Extensions = extensionsFrom(data)

	var present map[string]json.RawMessage
	if err := json.Unmarshal(data, &present); err == nil {
		_, w.hasConst = present["const"]
	}

	return nil
}

// schema converts the wire form into the IR.
func (w *wireSchema) schema() (*Schema, error) {
	if w == nil {
		return nil, nil
	}
	if w.Ref != "" {
		return &Schema{Ref: w.Ref}, nil
	}

	out := &Schema{
		Format:        w.Format,
		Title:         w.Title,
		Description:   w.Description,
		Nullable:      w.Nullable,
		Default:       w.Default,
		Example:       w.Example,
		Examples:      w.Examples,
		Enum:          w.Enum,
		Const:         w.Const,
		HasConst:      w.hasConst,
		Minimum:       w.Minimum,
		Maximum:       w.Maximum,
		MultipleOf:    w.MultipleOf,
		MinLength:     w.MinLength,
		MaxLength:     w.MaxLength,
		Pattern:       w.Pattern,
		MinItems:      w.MinItems,
		MaxItems:      w.MaxItems,
		UniqueItems:   w.UniqueItems,
		Required:      w.Required,
		MinProperties: w.MinProperties,
		MaxProperties: w.MaxProperties,
		Discriminator: w.Discriminator,
		ReadOnly:      w.ReadOnly,
		WriteOnly:     w.WriteOnly,
		Deprecated:    w.Deprecated,
		Extensions:    w.Extensions,
	}

	if err := w.decodeType(out); err != nil {
		return nil, err
	}
	if err := w.decodeExclusiveBounds(out); err != nil {
		return nil, err
	}
	if err := w.decodeAdditionalProperties(out); err != nil {
		return nil, err
	}

	var err error
	if out.Items, err = w.Items.schema(); err != nil {
		return nil, err
	}
	if out.Not, err = w.Not.schema(); err != nil {
		return nil, err
	}
	if out.OneOf, err = wireSchemas(w.OneOf); err != nil {
		return nil, err
	}
	if out.AnyOf, err = wireSchemas(w.AnyOf); err != nil {
		return nil, err
	}
	if out.AllOf, err = wireSchemas(w.AllOf); err != nil {
		return nil, err
	}
	if len(w.Properties) > 0 {
		out.Properties = make(map[string]*Schema, len(w.Properties))
		for name, property := range w.Properties {
			converted, err := property.schema()
			if err != nil {
				return nil, fmt.Errorf("property %q: %w", name, err)
			}
			out.Properties[name] = converted
		}
	}

	return out, nil
}

// decodeType reads either 3.0's single type or 3.1's type union, folding a
// "null" member of the union into the IR's Nullable flag.
func (w *wireSchema) decodeType(out *Schema) error {
	if len(w.Type) == 0 || string(w.Type) == "null" {
		return nil
	}
	var single string
	if err := json.Unmarshal(w.Type, &single); err == nil {
		out.Type = SchemaType(single)

		return nil
	}
	var union []string
	if err := json.Unmarshal(w.Type, &union); err != nil {
		return fmt.Errorf("schema type must be a string or an array of strings, got %s", w.Type)
	}
	for _, member := range union {
		if member == string(TypeNull) {
			out.Nullable = true

			continue
		}
		if out.Type != "" && out.Type != SchemaType(member) {
			// A union of two non-null types has no 3.0 equivalent and no
			// single IR type. Modelling it as anyOf keeps it lossless.
			return fmt.Errorf(
				"schema type union %s mixes types; express it as anyOf so it can render to either version", w.Type)
		}
		out.Type = SchemaType(member)
	}

	return nil
}

// decodeExclusiveBounds normalizes both spellings onto the IR's numeric form.
//
// 3.0 says `minimum: 5, exclusiveMinimum: true`; 3.1 says
// `exclusiveMinimum: 5`. The numeric form is stored because it is the lossless
// one — the boolean form is recoverable from it, but recovering it needs the
// sibling bound, which is exactly the coupling the IR should not carry.
func (w *wireSchema) decodeExclusiveBounds(out *Schema) error {
	decode := func(raw json.RawMessage, inclusive **float64, exclusive **float64, name string) error {
		if len(raw) == 0 {
			return nil
		}
		var flag bool
		if err := json.Unmarshal(raw, &flag); err == nil {
			if !flag {
				return nil
			}
			if *inclusive == nil {
				return fmt.Errorf("%s is true but the matching bound is absent", name)
			}
			*exclusive = *inclusive
			*inclusive = nil

			return nil
		}
		var bound float64
		if err := json.Unmarshal(raw, &bound); err != nil {
			return fmt.Errorf("%s must be a boolean (3.0) or a number (3.1), got %s", name, raw)
		}
		*exclusive = &bound

		return nil
	}
	if err := decode(w.ExclusiveMinimum, &out.Minimum, &out.ExclusiveMinimum, "exclusiveMinimum"); err != nil {
		return err
	}

	return decode(w.ExclusiveMaximum, &out.Maximum, &out.ExclusiveMaximum, "exclusiveMaximum")
}

func (w *wireSchema) decodeAdditionalProperties(out *Schema) error {
	if len(w.AdditionalProperties) == 0 {
		return nil
	}
	var allowed bool
	if err := json.Unmarshal(w.AdditionalProperties, &allowed); err == nil {
		out.AdditionalPropertiesAllowed = &allowed

		return nil
	}
	var nested wireSchema
	if err := json.Unmarshal(w.AdditionalProperties, &nested); err != nil {
		return fmt.Errorf("additionalProperties must be a boolean or a schema: %w", err)
	}
	converted, err := nested.schema()
	if err != nil {
		return err
	}
	out.AdditionalProperties = converted

	return nil
}

func wireSchemas(in []*wireSchema) ([]*Schema, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]*Schema, len(in))
	for i, schema := range in {
		converted, err := schema.schema()
		if err != nil {
			return nil, err
		}
		out[i] = converted
	}

	return out, nil
}
