package tapesoapi

import (
	"fmt"
	"maps"
)

// schema renders one IR schema to the target version.
//
// Three keywords are the whole 3.0-vs-3.1 story, and all three are handled
// here:
//
//   - nullability: `nullable: true` in 3.0, a `"null"` member of the type union
//     in 3.1;
//   - exclusive bounds: a boolean modifying `minimum`/`maximum` in 3.0, a
//     standalone number in 3.1;
//   - `const`: 3.1-only, approximated in 3.0 as a single-member enum.
//
// Everything else is spelled identically in both, which is why the IR can stay
// version-neutral and this function can stay the only place that branches.
func (r renderer) schema(schema *Schema) (map[string]any, error) {
	if schema == nil {
		return map[string]any{}, nil
	}
	if schema.Ref != "" {
		return map[string]any{"$ref": schema.Ref}, nil
	}

	out := map[string]any{}
	if err := r.renderSchemaType(schema, out); err != nil {
		return nil, err
	}

	setString(out, "format", schema.Format)
	setString(out, "title", schema.Title)
	setString(out, "description", schema.Description)
	setString(out, "pattern", schema.Pattern)

	if schema.Default != nil {
		out[defaultKeyword] = schema.Default
	}
	if schema.Example != nil {
		out["example"] = schema.Example
	}
	if len(schema.Examples) > 0 && r.target == V31 {
		out["examples"] = schema.Examples
	}
	if len(schema.Enum) > 0 {
		out["enum"] = schema.Enum
	}
	r.renderConst(schema, out)
	r.renderNumericBounds(schema, out)

	setUint(out, "minLength", schema.MinLength)
	setUint(out, "maxLength", schema.MaxLength)
	setUint(out, "minItems", schema.MinItems)
	setUint(out, "maxItems", schema.MaxItems)
	setUint(out, "minProperties", schema.MinProperties)
	setUint(out, "maxProperties", schema.MaxProperties)
	if schema.MultipleOf != nil {
		out["multipleOf"] = *schema.MultipleOf
	}
	if schema.UniqueItems {
		out["uniqueItems"] = true
	}

	switch {
	case schema.Items != nil:
		items, err := r.schema(schema.Items)
		if err != nil {
			return nil, fmt.Errorf("items: %w", err)
		}
		out["items"] = items
	case schema.Type == TypeArray && r.target == V30:
		// 3.0 requires `items` on an array; 3.1 does not. An unconstrained
		// element schema is what the 3.1 document meant, and it is the only
		// thing 3.0 can say that stays valid.
		out["items"] = map[string]any{}
	}

	if len(schema.Properties) > 0 {
		properties := map[string]any{}
		for _, name := range sortedKeys(schema.Properties) {
			property, err := r.schema(schema.Properties[name])
			if err != nil {
				return nil, fmt.Errorf("properties/%s: %w", name, err)
			}
			properties[name] = property
		}
		out["properties"] = properties
	}
	if len(schema.Required) > 0 {
		out["required"] = dedupeStrings(sortedCopy(schema.Required))
	}
	if schema.AdditionalProperties != nil {
		additional, err := r.schema(schema.AdditionalProperties)
		if err != nil {
			return nil, fmt.Errorf("additionalProperties: %w", err)
		}
		out["additionalProperties"] = additional
	} else if schema.AdditionalPropertiesAllowed != nil {
		out["additionalProperties"] = *schema.AdditionalPropertiesAllowed
	}

	for key, group := range map[string][]*Schema{
		"oneOf": schema.OneOf, "anyOf": schema.AnyOf, "allOf": schema.AllOf,
	} {
		if len(group) == 0 {
			continue
		}
		rendered := make([]any, 0, len(group))
		for _, member := range group {
			value, err := r.schema(member)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			rendered = append(rendered, value)
		}
		out[key] = rendered
	}
	if schema.Not != nil {
		not, err := r.schema(schema.Not)
		if err != nil {
			return nil, fmt.Errorf("not: %w", err)
		}
		out["not"] = not
	}
	if schema.Discriminator != nil {
		discriminator := map[string]any{"propertyName": schema.Discriminator.PropertyName}
		if len(schema.Discriminator.Mapping) > 0 {
			discriminator["mapping"] = schema.Discriminator.Mapping
		}
		out["discriminator"] = discriminator
	}

	if schema.ReadOnly {
		out["readOnly"] = true
	}
	if schema.WriteOnly {
		out["writeOnly"] = true
	}
	if schema.Deprecated {
		out["deprecated"] = true
	}
	maps.Copy(out, schema.Extensions)

	return out, nil
}

// renderSchemaType writes the type keyword and folds nullability into it the
// way the target version spells it.
func (r renderer) renderSchemaType(schema *Schema, out map[string]any) error {
	if schema.Type == "" {
		// A schema with no type but nullable is only expressible in 3.1, and
		// only as a bare "null" type — which would forbid every other value.
		// 3.0 has nothing to say here, so nullability on an untyped schema is
		// simply not rendered: an untyped schema already admits null.
		return nil
	}

	switch r.target {
	case V31:
		if schema.Nullable {
			out["type"] = []any{string(schema.Type), string(TypeNull)}

			return nil
		}
		out["type"] = string(schema.Type)

		return nil
	case V30:
		if schema.Type == TypeNull {
			// 3.0 has no null type. The nearest valid document says "untyped,
			// and null is allowed", which admits everything rather than only
			// null — lossy, and the reason a null-typed schema needs
			// WithDowngradeLossy to reach a 3.0 target at all. Emitting
			// `type: "null"` instead would produce a document no 3.0 reader
			// accepts, which is worse than a wide one.
			out["nullable"] = true

			return nil
		}
		out["type"] = string(schema.Type)
		if schema.Nullable {
			out["nullable"] = true
		}

		return nil
	default:
		return fmt.Errorf("cannot render schema to version %q", r.target)
	}
}

// renderNumericBounds writes minimum/maximum and the exclusive forms.
//
// The IR holds exclusive bounds as numbers (3.1's form). Rendering to 3.0 turns
// each back into the bound-plus-flag pair, which is lossless in that direction;
// it is the reverse — reconstructing a number from a bare `true` — that needs
// the sibling field, and the IR is shaped to never have to.
func (r renderer) renderNumericBounds(schema *Schema, out map[string]any) {
	if schema.Minimum != nil {
		out["minimum"] = *schema.Minimum
	}
	if schema.Maximum != nil {
		out["maximum"] = *schema.Maximum
	}

	switch r.target {
	case V31:
		if schema.ExclusiveMinimum != nil {
			out["exclusiveMinimum"] = *schema.ExclusiveMinimum
		}
		if schema.ExclusiveMaximum != nil {
			out["exclusiveMaximum"] = *schema.ExclusiveMaximum
		}
	case V30:
		// An unsupported target never reaches here: Compile rejects one, and
		// renderSchemaType errors on it above.
		if schema.ExclusiveMinimum != nil {
			out["minimum"] = *schema.ExclusiveMinimum
			out["exclusiveMinimum"] = true
		}
		if schema.ExclusiveMaximum != nil {
			out["maximum"] = *schema.ExclusiveMaximum
			out["exclusiveMaximum"] = true
		}
	}
}

// renderConst emits 3.1's const, or its closest 3.0 equivalent.
//
// A compile to 3.0 only reaches here with WithDowngradeLossy set — otherwise
// checkVersionCompatibility has already refused and named the schema. The
// single-member enum is the approximation, and it is exact for validation
// purposes; what it loses is the authoring intent that the value is fixed.
func (r renderer) renderConst(schema *Schema, out map[string]any) {
	if !schema.HasConst {
		return
	}
	if r.target == V31 {
		out["const"] = schema.Const

		return
	}
	if _, alreadyEnumerated := out["enum"]; !alreadyEnumerated {
		out["enum"] = []any{schema.Const}
	}
}

func setUint(out map[string]any, key string, value *uint64) {
	if value != nil {
		out[key] = *value
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}

	return out
}
