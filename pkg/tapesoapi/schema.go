package tapesoapi

import (
	"maps"
	"sort"
	"strings"
)

// SchemaType is a JSON Schema primitive type.
type SchemaType string

// The JSON Schema primitive types. Null is only nameable as a type of its own
// in 3.1; in 3.0 the IR's Nullable flag carries the same meaning.
const (
	TypeString  SchemaType = "string"
	TypeNumber  SchemaType = "number"
	TypeInteger SchemaType = "integer"
	TypeBoolean SchemaType = "boolean"
	TypeArray   SchemaType = "array"
	TypeObject  SchemaType = "object"
	TypeNull    SchemaType = "null"
)

// Schema is the version-neutral schema IR.
//
// It stores the union of 3.0 and 3.1 semantics and renders down to whichever
// version is targeted. Two fields carry the whole version story:
//
//   - Nullable renders as `nullable: true` in 3.0 and as a `"null"` member of
//     the type union in 3.1.
//   - ExclusiveMinimum/ExclusiveMaximum are held in 3.1's numeric form, because
//     it is the lossless one: 3.0's boolean form is derivable from it (emit the
//     bound as `minimum` and the flag as `exclusiveMinimum`), while the reverse
//     needs the sibling bound to reconstruct.
//
// A Schema with Ref set is a reference and every other field is ignored, which
// mirrors how a `$ref` behaves in 3.0.
type Schema struct {
	// Ref is a document-local reference such as "#/components/schemas/User".
	Ref string

	Type   SchemaType
	Format string

	Title       string
	Description string

	// Nullable widens the type to admit null. Held as a flag rather than as a
	// type union so the IR does not have to commit to a version's spelling.
	Nullable bool

	Default  any
	Example  any
	Examples []any
	Enum     []any

	// Const is 3.1-only. Compiling a document that uses it to V30 is an error
	// unless the compile lowers it, which it does by emitting a single-member
	// enum — the closest 3.0 equivalent.
	Const    any
	HasConst bool

	// Numeric constraints.
	Minimum          *float64
	Maximum          *float64
	ExclusiveMinimum *float64
	ExclusiveMaximum *float64
	MultipleOf       *float64

	// String constraints.
	MinLength *uint64
	MaxLength *uint64
	Pattern   string

	// Array constraints.
	Items       *Schema
	MinItems    *uint64
	MaxItems    *uint64
	UniqueItems bool

	// Object constraints.
	Properties    map[string]*Schema
	Required      []string
	MinProperties *uint64
	MaxProperties *uint64

	// AdditionalProperties is the schema extra properties must satisfy.
	AdditionalProperties *Schema

	// AdditionalPropertiesAllowed is the boolean form of the same keyword. A
	// nil value leaves the keyword unset, which is not the same as `true`:
	// unset lets a consumer apply its own default, and false forbids extras.
	AdditionalPropertiesAllowed *bool

	// Composition.
	OneOf         []*Schema
	AnyOf         []*Schema
	AllOf         []*Schema
	Not           *Schema
	Discriminator *Discriminator

	ReadOnly   bool
	WriteOnly  bool
	Deprecated bool

	// Extensions are `x-` vendor keys rendered verbatim.
	Extensions map[string]any
}

// Discriminator selects an implementing schema from a payload field.
type Discriminator struct {
	PropertyName string
	Mapping      map[string]string
}

// Ref returns a schema that is a reference to a document-local component.
func Ref(ref string) *Schema { return &Schema{Ref: ref} }

// SchemaRef returns a reference to a component schema by bare name.
func SchemaRef(name string) *Schema { return &Schema{Ref: componentsSchemaPrefix + name} }

// SchemaOption mutates a schema under construction. It is the shared vocabulary
// of the primitive constructors, so `String(Format("uuid"))` and
// `Integer(Minimum(0))` read the same way.
type SchemaOption func(*Schema)

func newSchema(t SchemaType, opts ...SchemaOption) *Schema {
	s := &Schema{Type: t}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}

	return s
}

// String returns a string schema.
func String(opts ...SchemaOption) *Schema { return newSchema(TypeString, opts...) }

// Integer returns an integer schema.
func Integer(opts ...SchemaOption) *Schema { return newSchema(TypeInteger, opts...) }

// Number returns a number schema.
func Number(opts ...SchemaOption) *Schema { return newSchema(TypeNumber, opts...) }

// Boolean returns a boolean schema.
func Boolean(opts ...SchemaOption) *Schema { return newSchema(TypeBoolean, opts...) }

// Object returns an object schema with the given properties.
func Object(properties map[string]*Schema, opts ...SchemaOption) *Schema {
	s := newSchema(TypeObject, opts...)
	if len(properties) > 0 {
		if s.Properties == nil {
			s.Properties = make(map[string]*Schema, len(properties))
		}
		maps.Copy(s.Properties, properties)
	}

	return s
}

// Array returns an array schema over items.
func Array(items *Schema, opts ...SchemaOption) *Schema {
	s := newSchema(TypeArray, opts...)
	s.Items = items

	return s
}

// AnyValue returns a schema that constrains nothing — the "any JSON value"
// schema, rendered as an empty object.
func AnyValue() *Schema { return &Schema{} }

// Format sets the format annotation.
func Format(format string) SchemaOption { return func(s *Schema) { s.Format = format } }

// Description sets the schema description.
func Description(text string) SchemaOption { return func(s *Schema) { s.Description = text } }

// Title sets the schema title.
func Title(text string) SchemaOption { return func(s *Schema) { s.Title = text } }

// Example sets an example value.
func Example(value any) SchemaOption { return func(s *Schema) { s.Example = value } }

// Default sets the default value.
func Default(value any) SchemaOption { return func(s *Schema) { s.Default = value } }

// Enum restricts the schema to a fixed set of values.
func Enum(values ...any) SchemaOption { return func(s *Schema) { s.Enum = values } }

// Nullable widens the schema to admit null.
func Nullable() SchemaOption { return func(s *Schema) { s.Nullable = true } }

// Deprecated marks the schema deprecated.
func Deprecated() SchemaOption { return func(s *Schema) { s.Deprecated = true } }

// ReadOnly marks the schema as response-only.
func ReadOnly() SchemaOption { return func(s *Schema) { s.ReadOnly = true } }

// WriteOnly marks the schema as request-only.
func WriteOnly() SchemaOption { return func(s *Schema) { s.WriteOnly = true } }

// Minimum sets an inclusive lower bound.
func Minimum(v float64) SchemaOption { return func(s *Schema) { s.Minimum = &v } }

// Maximum sets an inclusive upper bound.
func Maximum(v float64) SchemaOption { return func(s *Schema) { s.Maximum = &v } }

// ExclusiveMinimum sets an exclusive lower bound.
func ExclusiveMinimum(v float64) SchemaOption { return func(s *Schema) { s.ExclusiveMinimum = &v } }

// ExclusiveMaximum sets an exclusive upper bound.
func ExclusiveMaximum(v float64) SchemaOption { return func(s *Schema) { s.ExclusiveMaximum = &v } }

// MultipleOf constrains the value to multiples of v.
func MultipleOf(v float64) SchemaOption { return func(s *Schema) { s.MultipleOf = &v } }

// MinLength sets the minimum string length.
func MinLength(v uint64) SchemaOption { return func(s *Schema) { s.MinLength = &v } }

// MaxLength sets the maximum string length.
func MaxLength(v uint64) SchemaOption { return func(s *Schema) { s.MaxLength = &v } }

// Pattern sets a regular expression the string must match.
func Pattern(expr string) SchemaOption { return func(s *Schema) { s.Pattern = expr } }

// MinItems sets the minimum array length.
func MinItems(v uint64) SchemaOption { return func(s *Schema) { s.MinItems = &v } }

// MaxItems sets the maximum array length.
func MaxItems(v uint64) SchemaOption { return func(s *Schema) { s.MaxItems = &v } }

// UniqueItems requires array members to be distinct.
func UniqueItems() SchemaOption { return func(s *Schema) { s.UniqueItems = true } }

// Required marks object properties as required.
func Required(names ...string) SchemaOption {
	return func(s *Schema) { s.Required = append(s.Required, names...) }
}

// Property adds one object property.
func Property(name string, schema *Schema) SchemaOption {
	return func(s *Schema) {
		if s.Properties == nil {
			s.Properties = map[string]*Schema{}
		}
		s.Properties[name] = schema
	}
}

// AdditionalProperties constrains extra properties to a schema, which is how a
// free-form map is described.
func AdditionalProperties(schema *Schema) SchemaOption {
	return func(s *Schema) { s.AdditionalProperties = schema }
}

// NoAdditionalProperties forbids properties beyond those declared.
func NoAdditionalProperties() SchemaOption {
	return func(s *Schema) {
		allowed := false
		s.AdditionalPropertiesAllowed = &allowed
	}
}

// Extension sets a vendor extension on the schema. The key is prefixed with
// `x-` if it is not already.
func Extension(key string, value any) SchemaOption {
	return func(s *Schema) {
		if s.Extensions == nil {
			s.Extensions = map[string]any{}
		}
		s.Extensions[extensionKey(key)] = value
	}
}

// OneOf returns a schema satisfied by exactly one of the alternatives.
func OneOf(alternatives ...*Schema) *Schema { return &Schema{OneOf: alternatives} }

// AnyOf returns a schema satisfied by at least one of the alternatives.
func AnyOf(alternatives ...*Schema) *Schema { return &Schema{AnyOf: alternatives} }

// AllOf returns a schema satisfied by all of the members, which is how this
// package expresses composition over a referenced component.
func AllOf(members ...*Schema) *Schema { return &Schema{AllOf: members} }

// MapOf returns an object schema whose values all satisfy value — the shape a
// Go map reflects to.
func MapOf(value *Schema) *Schema {
	return &Schema{Type: TypeObject, AdditionalProperties: value}
}

func extensionKey(key string) string {
	if strings.HasPrefix(key, "x-") {
		return key
	}

	return "x-" + key
}

// clone returns a deep copy, so a schema handed to a builder cannot be mutated
// through a reference the caller kept.
func (s *Schema) clone() *Schema {
	if s == nil {
		return nil
	}
	out := *s
	out.Enum = append([]any(nil), s.Enum...)
	out.Examples = append([]any(nil), s.Examples...)
	out.Required = append([]string(nil), s.Required...)
	out.Minimum = clonePtr(s.Minimum)
	out.Maximum = clonePtr(s.Maximum)
	out.ExclusiveMinimum = clonePtr(s.ExclusiveMinimum)
	out.ExclusiveMaximum = clonePtr(s.ExclusiveMaximum)
	out.MultipleOf = clonePtr(s.MultipleOf)
	out.MinLength = clonePtr(s.MinLength)
	out.MaxLength = clonePtr(s.MaxLength)
	out.MinItems = clonePtr(s.MinItems)
	out.MaxItems = clonePtr(s.MaxItems)
	out.MinProperties = clonePtr(s.MinProperties)
	out.MaxProperties = clonePtr(s.MaxProperties)
	out.AdditionalPropertiesAllowed = clonePtr(s.AdditionalPropertiesAllowed)
	out.Items = s.Items.clone()
	out.Not = s.Not.clone()
	out.AdditionalProperties = s.AdditionalProperties.clone()
	out.OneOf = cloneSchemas(s.OneOf)
	out.AnyOf = cloneSchemas(s.AnyOf)
	out.AllOf = cloneSchemas(s.AllOf)
	if s.Properties != nil {
		out.Properties = make(map[string]*Schema, len(s.Properties))
		for name, property := range s.Properties {
			out.Properties[name] = property.clone()
		}
	}
	if s.Discriminator != nil {
		discriminator := *s.Discriminator
		if s.Discriminator.Mapping != nil {
			discriminator.Mapping = make(map[string]string, len(s.Discriminator.Mapping))
			maps.Copy(discriminator.Mapping, s.Discriminator.Mapping)
		}
		out.Discriminator = &discriminator
	}
	out.Extensions = cloneAnyMap(s.Extensions)

	return &out
}

func cloneSchemas(in []*Schema) []*Schema {
	if in == nil {
		return nil
	}
	out := make([]*Schema, len(in))
	for i, schema := range in {
		out[i] = schema.clone()
	}

	return out
}

func clonePtr[T any](in *T) *T {
	if in == nil {
		return nil
	}
	value := *in

	return &value
}

// cloneAnyMap deep-copies an extension tree.
//
// The copy has to be deep. Extensions are rewritten in place by
// [walkAnyRefs] when a document is namespaced, and a shallow copy would share
// its nested maps with the fragment it was cloned from — so namespacing one
// ingested document would reach back and rewrite the caller's original.
func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneAny(value)
	}

	return out
}

func cloneAny(in any) any {
	switch typed := in.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case []any:
		out := make([]any, len(typed))
		for index, value := range typed {
			out[index] = cloneAny(value)
		}

		return out
	default:
		// Everything else a decoded JSON tree holds — strings, numbers, bools,
		// nil — is immutable, so sharing it is safe.
		return in
	}
}

// walkRefs visits every $ref string in the schema tree, replacing it with the
// value the visitor returns. It is what lets component namespacing rewrite an
// ingested document's internal references.
func (s *Schema) walkRefs(visit func(string) string) {
	if s == nil {
		return
	}
	if s.Ref != "" {
		s.Ref = visit(s.Ref)

		return
	}
	s.Items.walkRefs(visit)
	s.Not.walkRefs(visit)
	s.AdditionalProperties.walkRefs(visit)
	for _, group := range [][]*Schema{s.OneOf, s.AnyOf, s.AllOf} {
		for _, member := range group {
			member.walkRefs(visit)
		}
	}
	for _, property := range s.Properties {
		property.walkRefs(visit)
	}
	if s.Discriminator != nil {
		for key, target := range s.Discriminator.Mapping {
			s.Discriminator.Mapping[key] = visit(target)
		}
	}
	walkExtensionRefs(s.Extensions, visit)
}

// walkExtensionRefs rewrites references buried in a vendor extension.
//
// Extensions are held as an opaque tree because this package does not model
// them — but "does not model" is not "does not contain references". A cassette
// that puts a `$ref` inside an `x-` key and then gets its components namespaced
// would otherwise end up pointing at a name that no longer exists, and the
// dangling ref would be in the one part of the document nothing was checking.
func walkExtensionRefs(extensions map[string]any, visit func(string) string) {
	for _, value := range extensions {
		walkAnyRefs(value, visit)
	}
}

// walkAnyRefs rewrites every "$ref" string in an arbitrary JSON tree, in place.
func walkAnyRefs(node any, visit func(string) string) {
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			if key == "$ref" {
				if ref, ok := value.(string); ok {
					typed[key] = visit(ref)

					continue
				}
			}
			walkAnyRefs(value, visit)
		}
	case []any:
		for _, value := range typed {
			walkAnyRefs(value, visit)
		}
	}
}

// uses31Only reports the 3.1-only constructs present in the schema, so a
// downgrade to 3.0 can name what it would lose rather than silently dropping it.
func (s *Schema) uses31Only() []string {
	if s == nil {
		return nil
	}
	var found []string
	if s.HasConst {
		found = append(found, "const")
	}
	if s.Type == TypeNull {
		// 3.0 has no null type, only the nullable flag — which cannot express
		// "null and nothing else". The 3.0 render widens it instead.
		found = append(found, "null type")
	}
	if len(s.Examples) > 0 {
		found = append(found, "examples")
	}
	for _, child := range s.children() {
		found = append(found, child.uses31Only()...)
	}
	sort.Strings(found)

	return dedupeStrings(found)
}

func (s *Schema) children() []*Schema {
	if s == nil {
		return nil
	}
	children := make([]*Schema, 0, len(s.Properties)+len(s.OneOf)+len(s.AnyOf)+len(s.AllOf)+3)
	children = append(children, s.Items, s.Not, s.AdditionalProperties)
	children = append(children, s.OneOf...)
	children = append(children, s.AnyOf...)
	children = append(children, s.AllOf...)
	for _, name := range sortedKeys(s.Properties) {
		children = append(children, s.Properties[name])
	}

	return children
}

func dedupeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := in[:1]
	for _, value := range in[1:] {
		if out[len(out)-1] != value {
			out = append(out, value)
		}
	}

	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

// isLocalRef reports whether a reference points inside this document.
//
// A reference that does not is left exactly as written. This package resolves
// nothing over the network — a document arriving from a cassette core does not
// control must not be able to direct core to fetch a URL — so a remote or
// file reference is not core's to check, and rewriting or refusing it would be
// pretending to an authority this package does not have.
func isLocalRef(ref string) bool { return strings.HasPrefix(ref, "#/") }
