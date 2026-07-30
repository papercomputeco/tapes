package tapesoapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// This file validates a decoded JSON *value* against a schema, which is a
// different job from validating a document — that one is structure.go.
//
// It exists because "the contract describes what is served" is only a claim
// until something checks a real payload against it. `tapes dev check-openapi`
// runs captured wire against the published component schema, and it is how a
// json.RawMessage rendering as a byte array instead of an object gets caught
// before a generated client chokes on it.
//
// Scope is deliberately the assertive subset of JSON Schema that OpenAPI 3.0
// uses. `format` is not asserted: OpenAPI treats it as an annotation, readers
// disagree about which formats they enforce, and a validator that rejected a
// `date-time` a real server emits would be reporting its own opinion as a
// contract violation.

// InstanceViolation is one place a value disagreed with its schema.
type InstanceViolation struct {
	// Pointer is an RFC 6901 JSON Pointer to the offending value, so a finding
	// in a nested array names the element rather than the document.
	Pointer string

	// Message says what was expected and what was there.
	Message string
}

func (v InstanceViolation) String() string {
	where := v.Pointer
	if where == "" {
		where = "(root)"
	}

	return where + ": " + v.Message
}

// InstanceError reports every violation found in one value.
//
// Collect-all, like the other errors here: someone checking a captured payload
// against a contract wants the whole disagreement, not its first line.
type InstanceError struct {
	// Schema names the schema the value was checked against.
	Schema string

	// Violations are the findings, in document order.
	Violations []InstanceViolation
}

func (e *InstanceError) Error() string {
	if len(e.Violations) == 1 {
		return fmt.Sprintf("does not satisfy %s: %s", e.Schema, e.Violations[0])
	}
	lines := make([]string, 0, len(e.Violations))
	for _, violation := range e.Violations {
		lines = append(lines, violation.String())
	}

	return fmt.Sprintf("%d ways this does not satisfy %s:\n  - %s",
		len(e.Violations), e.Schema, strings.Join(lines, "\n  - "))
}

// ComponentSchema returns a compiled component schema by bare name.
//
// The returned schema is a copy, so a caller cannot reach into the compiled
// document through it.
func (d *CompiledDoc) ComponentSchema(name string) (*Schema, bool) {
	if d == nil {
		return nil, false
	}
	schema, ok := d.schemas[name]
	if !ok {
		return nil, false
	}

	return schema.clone(), true
}

// ComponentSchemas returns the names of every component schema, sorted.
func (d *CompiledDoc) ComponentSchemas() []string {
	if d == nil {
		return nil
	}

	return sortedKeys(d.schemas)
}

// ValidateInstance checks a decoded JSON value against one of this document's
// component schemas.
//
// value is what encoding/json produced — maps, slices, strings, bools,
// float64 or json.Number, nil. Both number representations are accepted
// because a caller that decoded with UseNumber did so to keep the digits it was
// given, and losing that here would defeat the point.
//
// References inside the schema resolve against this document's components, so a
// composite response validates all the way down without the caller flattening
// anything first.
func (d *CompiledDoc) ValidateInstance(schemaName string, value any) error {
	if d == nil {
		return errors.New("nil compiled document")
	}
	schema, ok := d.schemas[schemaName]
	if !ok {
		return fmt.Errorf("this document defines no component schema %q", schemaName)
	}

	check := &instance{resolve: func(ref string) *Schema {
		name, local := strings.CutPrefix(ref, componentsSchemaPrefix)
		if !local {
			return nil
		}

		return d.schemas[name]
	}}
	check.value("", schema, value)

	if len(check.violations) == 0 {
		return nil
	}

	return &InstanceError{Schema: schemaName, Violations: check.violations}
}

// instance accumulates violations across one walk of a value.
type instance struct {
	resolve    func(ref string) *Schema
	violations []InstanceViolation
}

func (i *instance) reportf(pointer, format string, args ...any) {
	i.violations = append(i.violations, InstanceViolation{
		Pointer: pointer,
		Message: fmt.Sprintf(format, args...),
	})
}

// pointer appends one token to a JSON Pointer, escaping per RFC 6901.
func pointer(base, token string) string {
	token = strings.ReplaceAll(token, "~", "~0")
	token = strings.ReplaceAll(token, "/", "~1")

	return base + "/" + token
}

// value checks one value against one schema, recursing into whatever it holds.
func (i *instance) value(at string, schema *Schema, value any) {
	if schema == nil {
		return
	}
	if schema.Ref != "" {
		target := i.resolve(schema.Ref)
		if target == nil {
			// An unresolvable ref here is a document defect, not a payload one.
			// resolveReferences would have caught a dangling local ref at
			// compile, so this is an external one: say so rather than pass the
			// value silently.
			i.reportf(at, "schema references %s, which this document cannot resolve", schema.Ref)

			return
		}
		i.value(at, target, value)

		return
	}

	if value == nil {
		if !schema.Nullable && schema.Type != "" && schema.Type != TypeNull {
			i.reportf(at, "is null, but the schema requires %s", schema.Type)
		}

		return
	}

	i.enumAndConst(at, schema, value)
	i.composition(at, schema, value)

	switch schema.Type {
	case TypeString:
		i.stringValue(at, schema, value)
	case TypeInteger, TypeNumber:
		i.numberValue(at, schema, value)
	case TypeBoolean:
		if _, ok := value.(bool); !ok {
			i.reportf(at, "should be a boolean, got %s", describe(value))
		}
	case TypeArray:
		i.arrayValue(at, schema, value)
	case TypeObject:
		i.objectValue(at, schema, value)
	case TypeNull:
		i.reportf(at, "should be null, got %s", describe(value))
	case "":
		// An untyped schema still constrains through its keywords, and those
		// only apply to a value of the matching kind — so infer the kind from
		// the value rather than skipping the constraints entirely.
		i.untyped(at, schema, value)
	}
}

// untyped applies the keyword groups a value's own kind makes meaningful. This
// is what lets a schema that only says `properties` still check them.
func (i *instance) untyped(at string, schema *Schema, value any) {
	switch value.(type) {
	case string:
		i.stringValue(at, schema, value)
	case float64, json.Number, int, int64:
		i.numberValue(at, schema, value)
	case []any:
		i.arrayValue(at, schema, value)
	case map[string]any:
		i.objectValue(at, schema, value)
	}
}

func (i *instance) stringValue(at string, schema *Schema, value any) {
	text, ok := value.(string)
	if !ok {
		i.reportf(at, "should be a string, got %s", describe(value))

		return
	}
	// Length is counted in characters, not bytes: the spec counts code points,
	// and a byte count would reject a valid multi-byte string. Ranging a string
	// yields runes, so this counts them without a conversion to justify.
	var length uint64
	for range text {
		length++
	}
	if schema.MinLength != nil && length < *schema.MinLength {
		i.reportf(at, "is %d characters, below minLength %d", length, *schema.MinLength)
	}
	if schema.MaxLength != nil && length > *schema.MaxLength {
		i.reportf(at, "is %d characters, above maxLength %d", length, *schema.MaxLength)
	}
	if schema.Pattern == "" {
		return
	}
	expression, err := regexp.Compile(schema.Pattern)
	if err != nil {
		// structure.go reports the uncompilable pattern itself. Repeating it
		// per value would bury the payload findings.
		return
	}
	if !expression.MatchString(text) {
		i.reportf(at, "does not match pattern %q", schema.Pattern)
	}
}

func (i *instance) numberValue(at string, schema *Schema, value any) {
	number, ok := asNumber(value)
	if !ok {
		i.reportf(at, "should be a %s, got %s", orNumber(schema.Type), describe(value))

		return
	}
	if schema.Type == TypeInteger && number != math.Trunc(number) {
		i.reportf(at, "should be an integer, got %v", number)
	}
	if schema.Minimum != nil && number < *schema.Minimum {
		i.reportf(at, "is %v, below minimum %v", number, *schema.Minimum)
	}
	if schema.Maximum != nil && number > *schema.Maximum {
		i.reportf(at, "is %v, above maximum %v", number, *schema.Maximum)
	}
	if schema.ExclusiveMinimum != nil && number <= *schema.ExclusiveMinimum {
		i.reportf(at, "is %v, not above exclusiveMinimum %v", number, *schema.ExclusiveMinimum)
	}
	if schema.ExclusiveMaximum != nil && number >= *schema.ExclusiveMaximum {
		i.reportf(at, "is %v, not below exclusiveMaximum %v", number, *schema.ExclusiveMaximum)
	}
	if schema.MultipleOf != nil && *schema.MultipleOf > 0 {
		quotient := number / *schema.MultipleOf
		if math.Abs(quotient-math.Round(quotient)) > 1e-9 {
			i.reportf(at, "is %v, not a multiple of %v", number, *schema.MultipleOf)
		}
	}
}

func (i *instance) arrayValue(at string, schema *Schema, value any) {
	items, ok := value.([]any)
	if !ok {
		i.reportf(at, "should be an array, got %s", describe(value))

		return
	}
	count := uint64(len(items))
	if schema.MinItems != nil && count < *schema.MinItems {
		i.reportf(at, "has %d items, below minItems %d", count, *schema.MinItems)
	}
	if schema.MaxItems != nil && count > *schema.MaxItems {
		i.reportf(at, "has %d items, above maxItems %d", count, *schema.MaxItems)
	}
	if schema.UniqueItems {
		seen := map[string]int{}
		for index, item := range items {
			key := canonical(item)
			if first, duplicate := seen[key]; duplicate {
				i.reportf(pointer(at, strconv.Itoa(index)),
					"repeats the value at index %d, but uniqueItems is set", first)

				continue
			}
			seen[key] = index
		}
	}
	for index, item := range items {
		i.value(pointer(at, strconv.Itoa(index)), schema.Items, item)
	}
}

func (i *instance) objectValue(at string, schema *Schema, value any) {
	object, ok := value.(map[string]any)
	if !ok {
		i.reportf(at, "should be an object, got %s", describe(value))

		return
	}
	count := uint64(len(object))
	if schema.MinProperties != nil && count < *schema.MinProperties {
		i.reportf(at, "has %d properties, below minProperties %d", count, *schema.MinProperties)
	}
	if schema.MaxProperties != nil && count > *schema.MaxProperties {
		i.reportf(at, "has %d properties, above maxProperties %d", count, *schema.MaxProperties)
	}

	for _, name := range schema.Required {
		if _, present := object[name]; !present {
			i.reportf(at, "is missing required property %q", name)
		}
	}

	// Iterated over the schema's properties and then the value's extras, both
	// in sorted order, so two runs over the same payload report in the same
	// order.
	for _, name := range sortedKeys(schema.Properties) {
		if held, present := object[name]; present {
			i.value(pointer(at, name), schema.Properties[name], held)
		}
	}
	for _, name := range sortedKeys(object) {
		if _, declared := schema.Properties[name]; declared {
			continue
		}
		switch {
		case schema.AdditionalProperties != nil:
			i.value(pointer(at, name), schema.AdditionalProperties, object[name])
		case schema.AdditionalPropertiesAllowed != nil && !*schema.AdditionalPropertiesAllowed:
			i.reportf(pointer(at, name), "is not a declared property and additionalProperties is false")
		}
	}
}

func (i *instance) enumAndConst(at string, schema *Schema, value any) {
	if schema.HasConst && canonical(value) != canonical(schema.Const) {
		i.reportf(at, "should be the const value %s, got %s", canonical(schema.Const), canonical(value))
	}
	if len(schema.Enum) == 0 {
		return
	}
	encoded := canonical(value)
	allowed := make([]string, 0, len(schema.Enum))
	for _, member := range schema.Enum {
		if canonical(member) == encoded {
			return
		}
		allowed = append(allowed, canonical(member))
	}
	sort.Strings(allowed)
	i.reportf(at, "is %s, which is not one of %s", encoded, strings.Join(allowed, ", "))
}

// composition checks allOf, anyOf, oneOf, and not.
//
// anyOf and oneOf are checked with a throwaway accumulator per branch, because
// a branch that fails is not a finding: only the combinator's own verdict is.
// Reporting each rejected branch would turn one mismatch into one message per
// alternative.
func (i *instance) composition(at string, schema *Schema, value any) {
	for _, member := range schema.AllOf {
		i.value(at, member, value)
	}

	if len(schema.AnyOf) > 0 && i.matching(schema.AnyOf, value) == 0 {
		i.reportf(at, "satisfies none of the %d anyOf alternatives", len(schema.AnyOf))
	}
	if len(schema.OneOf) > 0 {
		switch matched := i.matching(schema.OneOf, value); matched {
		case 1:
		case 0:
			i.reportf(at, "satisfies none of the %d oneOf alternatives", len(schema.OneOf))
		default:
			i.reportf(at, "satisfies %d of the %d oneOf alternatives, which must be exactly one",
				matched, len(schema.OneOf))
		}
	}
	if schema.Not != nil {
		if branch := (&instance{resolve: i.resolve}); func() bool {
			branch.value(at, schema.Not, value)

			return len(branch.violations) == 0
		}() {
			i.reportf(at, "satisfies the `not` schema, which forbids it")
		}
	}
}

// matching counts how many alternatives a value satisfies.
func (i *instance) matching(alternatives []*Schema, value any) int {
	matched := 0
	for _, alternative := range alternatives {
		branch := &instance{resolve: i.resolve}
		branch.value("", alternative, value)
		if len(branch.violations) == 0 {
			matched++
		}
	}

	return matched
}

// asNumber accepts every shape encoding/json can produce for a number,
// including json.Number from a decoder using UseNumber.
func asNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case json.Number:
		number, err := typed.Float64()

		return number, err == nil
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	default:
		return 0, false
	}
}

// orNumber names the expected kind in a message, defaulting to "number" for an
// untyped schema that nonetheless carries numeric keywords.
func orNumber(t SchemaType) SchemaType {
	if t == "" {
		return TypeNumber
	}

	return t
}

// describe names the JSON kind of a value, for a message that says what was
// there instead of what was wanted.
func describe(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case string:
		return "a string"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	case json.Number:
		return "a number (" + typed.String() + ")"
	default:
		if _, ok := asNumber(value); ok {
			return "a number"
		}

		return "a " + reflect.TypeOf(value).String()
	}
}

// canonical renders a value for comparison and for messages.
//
// JSON encoding sorts object keys, so two values that differ only in key order
// compare equal — which is what enum, const, and uniqueItems all need.
func canonical(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}

	return string(encoded)
}
