package tapesoapi

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Reflector turns Go types into schemas and accumulates the named ones as
// reusable components.
//
// It is an interface so a caller with unusual types can substitute its own
// derivation without forking the package; [NewReflector] is the default.
type Reflector interface {
	// Reflect returns the schema for a value's type. Named struct types are
	// registered as components and referenced, so a type used by ten operations
	// is described once.
	Reflect(value any) (*Schema, error)

	// ReflectType is Reflect for a type with no value in hand.
	ReflectType(t reflect.Type) (*Schema, error)

	// Components returns every registered component schema, keyed by bare name.
	Components() map[string]*Schema
}

// TypeDocs supplies prose that reflection cannot see.
//
// Go's runtime carries no doc comments, so a purely reflective schema is
// structurally complete and completely undocumented. The generator reads the
// comments out of the source with [gosource.Load] and hands them here, which
// keeps documentation next to the field it describes rather than duplicated
// into a struct tag.
type TypeDocs interface {
	// TypeDoc returns the doc comment for a named type.
	TypeDoc(pkgPath, typeName string) string

	// FieldDoc returns the doc comment for one field of a named type.
	FieldDoc(pkgPath, typeName, fieldName string) string
}

// ReflectorOption configures the default reflector.
type ReflectorOption func(*reflector)

// WithDocs attaches doc comments to reflected schemas.
func WithDocs(docs TypeDocs) ReflectorOption {
	return func(r *reflector) { r.docs = docs }
}

// WithPointersNullable marks pointer fields nullable.
//
// Off by default. A Go pointer usually means "optional in the payload", which
// OpenAPI already expresses by leaving the field out of `required`; rendering
// every pointer as `nullable: true` would tell a client generator to wrap types
// in an option *and* admit an explicit null, which is not what most of these
// handlers do.
func WithPointersNullable() ReflectorOption {
	return func(r *reflector) { r.pointersNullable = true }
}

// WithTypeNamer overrides how a Go type becomes a component name.
func WithTypeNamer(namer func(reflect.Type) string) ReflectorOption {
	return func(r *reflector) { r.namer = namer }
}

// reflector is the default Reflector.
type reflector struct {
	mutex sync.Mutex

	// components holds every registered schema by its component name.
	components map[string]*Schema

	// names maps a Go type to the component name it was registered under, so a
	// second sighting of the type reuses the name rather than re-deriving it.
	names map[reflect.Type]string

	// claimed tracks which component names are taken, so two different types
	// with the same bare name get deterministically distinct names instead of
	// silently overwriting each other.
	claimed map[string]reflect.Type

	docs             TypeDocs
	pointersNullable bool
	namer            func(reflect.Type) string
}

// NewReflector returns the default Go-type-to-schema reflector.
func NewReflector(options ...ReflectorOption) Reflector {
	r := &reflector{
		components: map[string]*Schema{},
		names:      map[reflect.Type]string{},
		claimed:    map[string]reflect.Type{},
		namer:      defaultTypeName,
	}
	for _, option := range options {
		if option != nil {
			option(r)
		}
	}

	return r
}

// Reflect implements Reflector.
func (r *reflector) Reflect(value any) (*Schema, error) {
	if value == nil {
		return AnyValue(), nil
	}
	if t, ok := value.(reflect.Type); ok {
		return r.ReflectType(t)
	}

	return r.ReflectType(reflect.TypeOf(value))
}

// ReflectType implements Reflector.
func (r *reflector) ReflectType(t reflect.Type) (*Schema, error) {
	if t == nil {
		return AnyValue(), nil
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.reflect(t, fieldTags{})
}

// Components implements Reflector.
func (r *reflector) Components() map[string]*Schema {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	out := make(map[string]*Schema, len(r.components))
	for name, schema := range r.components {
		out[name] = schema.clone()
	}

	return out
}

var (
	timeType        = reflect.TypeFor[time.Time]()
	durationType    = reflect.TypeFor[time.Duration]()
	rawMessageType  = reflect.TypeFor[json.RawMessage]()
	jsonNumberType  = reflect.TypeFor[json.Number]()
	byteSliceType   = reflect.TypeFor[[]byte]()
	textMarshalType = reflect.TypeOf((*interface{ MarshalText() ([]byte, error) })(nil)).Elem()
)

// reflect derives a schema for t, applying any tag overrides from the field
// that referenced it. The caller holds the mutex.
func (r *reflector) reflect(t reflect.Type, tags fieldTags) (*Schema, error) {
	if override := tags.typeOverride(); override != nil {
		return override, nil
	}

	nullable := false
	for t.Kind() == reflect.Pointer {
		nullable = r.pointersNullable
		t = t.Elem()
	}

	schema, err := r.reflectConcrete(t)
	if err != nil {
		return nil, err
	}
	if nullable && schema.Ref == "" {
		schema.Nullable = true
	}
	tags.apply(schema)

	return schema, nil
}

func (r *reflector) reflectConcrete(t reflect.Type) (*Schema, error) {
	// Types with a JSON identity of their own are described directly. Reflecting
	// into time.Time's unexported fields, for instance, would produce an object
	// schema for something that serializes as a string.
	if schema := wellKnownSchema(t); schema != nil {
		return schema, nil
	}

	switch t.Kind() { //nolint:exhaustive // the default reports every kind with no JSON shape
	case reflect.Bool:
		return Boolean(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return Integer(Format("int32")), nil
	case reflect.Int64:
		return Integer(Format("int64")), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return Integer(Format("int32"), Minimum(0)), nil
	case reflect.Uint64:
		return Integer(Format("int64"), Minimum(0)), nil
	case reflect.Float32:
		return Number(Format("float")), nil
	case reflect.Float64:
		return Number(Format("double")), nil
	case reflect.String:
		return String(), nil
	case reflect.Slice, reflect.Array:
		if t == byteSliceType {
			return String(Format("byte")), nil
		}
		items, err := r.reflect(t.Elem(), fieldTags{})
		if err != nil {
			return nil, err
		}

		return Array(items), nil
	case reflect.Map:
		if t.Key().Kind() != reflect.String && !t.Key().Implements(textMarshalType) {
			return nil, fmt.Errorf("map key type %s does not serialize as a JSON object key", t.Key())
		}
		value, err := r.reflect(t.Elem(), fieldTags{})
		if err != nil {
			return nil, err
		}

		return MapOf(value), nil
	case reflect.Struct:
		return r.reflectStruct(t)
	case reflect.Interface:
		// An interface carries no static shape; `any` is the honest schema.
		return AnyValue(), nil
	case reflect.Pointer:
		return r.reflect(t.Elem(), fieldTags{})
	default:
		return nil, fmt.Errorf("cannot derive an OpenAPI schema for %s (kind %s)", t, t.Kind())
	}
}

func wellKnownSchema(t reflect.Type) *Schema {
	switch t {
	case timeType:
		return String(Format("date-time"))
	case durationType:
		return Integer(Format("int64"), Description("duration in nanoseconds"))
	case rawMessageType:
		return AnyValue()
	case jsonNumberType:
		return Number()
	}

	// A type that marshals itself to text serializes as a JSON string whatever
	// its Go shape is — google/uuid.UUID is an array of 16 bytes, and reflecting
	// into it would describe an array of integers no client ever sees.
	if t.Implements(textMarshalType) || reflect.PointerTo(t).Implements(textMarshalType) {
		if t.Kind() != reflect.String {
			schema := String()
			if strings.EqualFold(t.Name(), "UUID") {
				schema.Format = "uuid"
			}

			return schema
		}
	}

	return nil
}

// reflectStruct describes a struct, registering named ones as components.
func (r *reflector) reflectStruct(t reflect.Type) (*Schema, error) {
	// An anonymous struct has no name to register under, so it is inlined.
	if t.Name() == "" {
		return r.structSchema(t)
	}
	if name, known := r.names[t]; known {
		return SchemaRef(name), nil
	}

	name := r.claimName(t)
	// The name is claimed and the reference returned *before* the body is
	// derived, because a self-referential type would otherwise recurse forever.
	r.names[t] = name
	r.components[name] = &Schema{Type: TypeObject}

	schema, err := r.structSchema(t)
	if err != nil {
		delete(r.names, t)
		delete(r.components, name)
		delete(r.claimed, name)

		return nil, err
	}
	if r.docs != nil {
		schema.Description = firstNonEmpty(schema.Description, r.docs.TypeDoc(t.PkgPath(), t.Name()))
	}
	r.components[name] = schema

	return SchemaRef(name), nil
}

// claimName picks a stable component name for t.
//
// Two different types with the same bare name — `api.Status` and
// `cassette.Status` — cannot share one component. The loser keeps a suffixed
// name rather than the package-qualified one because a generated client turns
// the component key into a type identifier, and `Status2` reads better than
// `GithubComPapercomputecoTapesPkgCassetteStatus`. Which one is the loser is
// decided by registration order, which merge makes deterministic.
func (r *reflector) claimName(t reflect.Type) string {
	base := r.namer(t)
	if owner, taken := r.claimed[base]; !taken || owner == t {
		r.claimed[base] = t

		return base
	}
	for suffix := 2; ; suffix++ {
		candidate := base + strconv.Itoa(suffix)
		if owner, taken := r.claimed[candidate]; !taken || owner == t {
			r.claimed[candidate] = t

			return candidate
		}
	}
}

func defaultTypeName(t reflect.Type) string { return t.Name() }

func (r *reflector) structSchema(t reflect.Type) (*Schema, error) {
	schema := &Schema{Type: TypeObject, Properties: map[string]*Schema{}}

	if err := r.collectFields(t, schema, nil); err != nil {
		return nil, err
	}
	if len(schema.Properties) == 0 {
		schema.Properties = nil
	}

	return schema, nil
}

// collectFields walks a struct's fields, flattening embedded ones the way
// encoding/json does.
func (r *reflector) collectFields(t reflect.Type, schema *Schema, owner reflect.Type) error {
	if owner == nil {
		owner = t
	}
	for field := range t.Fields() {
		if !field.IsExported() && !field.Anonymous {
			continue
		}

		name, options := parseJSONTag(field)
		if name == "-" {
			continue
		}

		// An embedded struct with no JSON name of its own promotes its fields
		// into the parent, matching encoding/json. Without this, every response
		// type built by embedding would describe a nested object that never
		// appears on the wire.
		if field.Anonymous && name == "" {
			embedded := field.Type
			for embedded.Kind() == reflect.Pointer {
				embedded = embedded.Elem()
			}
			if embedded.Kind() == reflect.Struct && wellKnownSchema(embedded) == nil {
				if err := r.collectFields(embedded, schema, owner); err != nil {
					return err
				}

				continue
			}
		}
		if !field.IsExported() {
			continue
		}
		if name == "" {
			name = field.Name
		}

		tags := parseFieldTags(field)
		property, err := r.reflect(field.Type, tags)
		if err != nil {
			return fmt.Errorf("field %s.%s: %w", t.Name(), field.Name, err)
		}
		if r.docs != nil && property.Ref == "" {
			property.Description = firstNonEmpty(
				property.Description, r.docs.FieldDoc(owner.PkgPath(), owner.Name(), field.Name))
		}
		schema.Properties[name] = property

		if tags.required || (!options.omitempty && tags.explicitRequired) {
			schema.Required = append(schema.Required, name)
		}
	}

	return nil
}

type jsonTagOptions struct {
	omitempty bool
	str       bool
}

func parseJSONTag(field reflect.StructField) (string, jsonTagOptions) {
	tag, ok := field.Tag.Lookup("json")
	if !ok {
		return "", jsonTagOptions{}
	}
	name, rest, _ := strings.Cut(tag, ",")
	var options jsonTagOptions
	for option := range strings.SplitSeq(rest, ",") {
		switch option {
		case "omitempty":
			options.omitempty = true
		case "string":
			options.str = true
		}
	}

	return name, options
}

// fieldTags are the `oas:"..."` overrides on one struct field.
//
// The tag is a comma-separated list of `key=value` pairs and bare flags:
//
//	Amount int `json:"amount" oas:"min=0,format=int64,required"`
//
// Values may not contain commas. Descriptions therefore come from doc comments
// rather than from tags — which is the better place for them anyway.
type fieldTags struct {
	values           map[string]string
	flags            map[string]struct{}
	required         bool
	explicitRequired bool
}

func parseFieldTags(field reflect.StructField) fieldTags {
	tags := fieldTags{values: map[string]string{}, flags: map[string]struct{}{}}
	raw, ok := field.Tag.Lookup("oas")
	if !ok {
		return tags
	}
	for part := range strings.SplitSeq(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, hasValue := strings.Cut(part, "=")
		if hasValue {
			tags.values[strings.TrimSpace(key)] = strings.TrimSpace(value)

			continue
		}
		tags.flags[key] = struct{}{}
	}
	_, tags.required = tags.flags["required"]
	tags.explicitRequired = tags.required

	return tags
}

// typeOverride returns a schema stated outright by the tag, for the fields
// whose Go type says nothing useful — a json.RawMessage that always carries an
// object, say.
func (t fieldTags) typeOverride() *Schema {
	declared, ok := t.values["type"]
	if !ok {
		return nil
	}
	// `type=array:object` describes an array of objects, which is the one
	// nested shape a flat tag needs to express.
	outer, inner, nested := strings.Cut(declared, ":")
	schema := &Schema{Type: SchemaType(outer)}
	if nested {
		schema.Items = &Schema{Type: SchemaType(inner)}
	}
	t.apply(schema)

	return schema
}

// apply layers the tag's constraints onto a derived schema.
func (t fieldTags) apply(schema *Schema) {
	if schema == nil || schema.Ref != "" {
		return
	}
	for key, value := range t.values {
		switch key {
		case "format":
			schema.Format = value
		case "title":
			schema.Title = value
		case "pattern":
			schema.Pattern = value
		case "example":
			schema.Example = coerce(value, schema.Type)
		case defaultKeyword:
			schema.Default = coerce(value, schema.Type)
		case "enum":
			// Pipe-separated, because the tag itself is comma-separated.
			for member := range strings.SplitSeq(value, "|") {
				schema.Enum = append(schema.Enum, coerce(member, schema.Type))
			}
		case "min":
			setFloatFromTag(value, &schema.Minimum)
		case "max":
			setFloatFromTag(value, &schema.Maximum)
		case "exclusiveMin":
			setFloatFromTag(value, &schema.ExclusiveMinimum)
		case "exclusiveMax":
			setFloatFromTag(value, &schema.ExclusiveMaximum)
		case "multipleOf":
			setFloatFromTag(value, &schema.MultipleOf)
		case "minLength":
			setUintFromTag(value, &schema.MinLength)
		case "maxLength":
			setUintFromTag(value, &schema.MaxLength)
		case "minItems":
			setUintFromTag(value, &schema.MinItems)
		case "maxItems":
			setUintFromTag(value, &schema.MaxItems)
		}
	}
	for flag := range t.flags {
		switch flag {
		case "nullable":
			schema.Nullable = true
		case "readOnly":
			schema.ReadOnly = true
		case "writeOnly":
			schema.WriteOnly = true
		case "deprecated":
			schema.Deprecated = true
		case "uniqueItems":
			schema.UniqueItems = true
		}
	}
}

// coerce parses a tag value into the JSON type the schema declares, so
// `example=42` on an integer is the number 42 rather than the string "42".
func coerce(value string, schemaType SchemaType) any {
	switch schemaType { //nolint:exhaustive // every other type takes the tag value as the string it already is
	case TypeInteger:
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
	case TypeNumber:
		if parsed, err := strconv.ParseFloat(value, 64); err == nil {
			return parsed
		}
	case TypeBoolean:
		if parsed, err := strconv.ParseBool(value); err == nil {
			return parsed
		}
	}

	return value
}

func setFloatFromTag(value string, target **float64) {
	if parsed, err := strconv.ParseFloat(value, 64); err == nil {
		*target = &parsed
	}
}

func setUintFromTag(value string, target **uint64) {
	if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
		*target = &parsed
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}

	return ""
}
