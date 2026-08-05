package tapesoapi

import (
	"errors"
	"fmt"
	"strings"
)

// StandaloneRequestSchema returns one operation request schema as JSON Schema
// 2020-12, with every reachable OpenAPI component bundled under $defs. The raw
// document tree is used so JSON Schema keywords that OpenAPI itself does not
// interpret are preserved for the standalone consumer.
func (document *Document) StandaloneRequestSchema(method, path, mediaType string) (map[string]any, error) {
	if document == nil {
		return nil, errors.New("nil OpenAPI document")
	}
	paths, ok := document.root["paths"].(map[string]any)
	if !ok {
		return nil, errors.New("OpenAPI document has no paths object")
	}
	item, ok := paths[path].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("path %q does not exist", path)
	}
	operation, ok := item[strings.ToLower(method)].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s %s does not exist", strings.ToUpper(method), path)
	}
	body, ok := operation["requestBody"].(map[string]any)
	if !ok {
		return nil, errors.New("operation has no inline requestBody")
	}
	content, ok := body["content"].(map[string]any)
	if !ok {
		return nil, errors.New("requestBody has no content object")
	}
	media, ok := content[mediaType].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("requestBody has no %q media type", mediaType)
	}
	schema, ok := media["schema"].(map[string]any)
	if !ok {
		return nil, errors.New("requestBody media type has no schema object")
	}

	components := map[string]any{}
	if object, ok := document.root["components"].(map[string]any); ok {
		components, _ = object["schemas"].(map[string]any)
	}

	return standaloneSchema(schema, components)
}

func standaloneSchema(schema map[string]any, components map[string]any) (map[string]any, error) {
	if _, exists := schema["$defs"]; exists {
		return nil, errors.New("top-level $defs is not supported")
	}
	definitions := make(map[string]any)
	var rewrite func(map[string]any) (map[string]any, error)
	rewrite = func(current map[string]any) (map[string]any, error) {
		out := cloneAnyMap(current)
		if reference, present := current["$ref"]; present {
			value, ok := reference.(string)
			if !ok {
				return nil, errors.New("$ref must be a string")
			}
			token, ok := strings.CutPrefix(value, componentsSchemaPrefix)
			if !ok {
				return nil, fmt.Errorf("only local %s references are supported", componentsSchemaPrefix)
			}
			name, err := decodeJSONPointerToken(token)
			if err != nil {
				return nil, fmt.Errorf("reference %q: %w", value, err)
			}
			component, ok := components[name].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("reference %q does not exist", value)
			}
			if _, seen := definitions[name]; !seen {
				definitions[name] = nil
				definitions[name], err = rewrite(component)
				if err != nil {
					return nil, err
				}
			}
			out["$ref"] = "#/$defs/" + token
		}

		for _, key := range []string{"additionalItems", "additionalProperties", "contains", "contentSchema", "else", "if", "items", "not", "propertyNames", "then", "unevaluatedItems", "unevaluatedProperties"} {
			if child, ok := current[key].(map[string]any); ok {
				rewritten, err := rewrite(child)
				if err != nil {
					return nil, err
				}
				out[key] = rewritten
			}
		}
		for _, key := range []string{"allOf", "anyOf", "oneOf", "prefixItems"} {
			children, ok := current[key].([]any)
			if !ok {
				continue
			}
			rewritten := append([]any(nil), children...)
			for index, child := range children {
				object, ok := child.(map[string]any)
				if !ok {
					continue
				}
				var err error
				rewritten[index], err = rewrite(object)
				if err != nil {
					return nil, err
				}
			}
			out[key] = rewritten
		}
		for _, key := range []string{"$defs", "definitions", "dependentSchemas", "patternProperties", "properties"} {
			children, ok := current[key].(map[string]any)
			if !ok {
				continue
			}
			rewritten := cloneAnyMap(children)
			for name, child := range children {
				object, ok := child.(map[string]any)
				if !ok {
					continue
				}
				var err error
				rewritten[name], err = rewrite(object)
				if err != nil {
					return nil, err
				}
			}
			out[key] = rewritten
		}

		return out, nil
	}

	standalone, err := rewrite(schema)
	if err != nil {
		return nil, err
	}
	if len(definitions) > 0 {
		standalone["$defs"] = definitions
	}
	standalone["$schema"] = "https://json-schema.org/draft/2020-12/schema"

	return standalone, nil
}

func decodeJSONPointerToken(token string) (string, error) {
	var decoded strings.Builder
	for index := 0; index < len(token); index++ {
		if token[index] != '~' {
			decoded.WriteByte(token[index])
			continue
		}
		if index+1 >= len(token) {
			return "", errors.New("invalid JSON Pointer escape")
		}
		index++
		switch token[index] {
		case '0':
			decoded.WriteByte('~')
		case '1':
			decoded.WriteByte('/')
		default:
			return "", errors.New("invalid JSON Pointer escape")
		}
	}

	return decoded.String(), nil
}
