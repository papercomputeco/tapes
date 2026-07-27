// Package openapi provides generic parsing, rewriting, and merging primitives
// for OpenAPI JSON documents.
package openapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"sort"
	"strings"
)

// Status reports how current a cached document is.
type Status string

const (
	Fresh   Status = "fresh"
	Stale   Status = "stale"
	Missing Status = "missing"
)

// Document is a parsed OpenAPI JSON object. Its contents are retained as a
// generic tree so vendor extensions and fields unknown to this package survive.
type Document struct {
	root map[string]any
}

// Parse decodes exactly one JSON object while preserving JSON numbers.
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
func (document *Document) RewritePrefix(sourcePrefix, targetPrefix string) (*Document, error) {
	paths, err := document.Paths()
	if err != nil {
		return nil, err
	}
	root := make(map[string]any, len(document.root))
	for key, value := range document.root {
		if key != "servers" {
			root[key] = value
		}
	}
	rewritten := make(map[string]any, len(paths))
	declared := document.root["paths"].(map[string]any)
	for _, path := range paths {
		if !segmentPrefix(sourcePrefix, path) {
			return nil, fmt.Errorf("path %q is outside %s", path, sourcePrefix)
		}
		rewritten[targetPrefix+strings.TrimPrefix(path, sourcePrefix)] = declared[path]
	}
	root["paths"] = rewritten
	return &Document{root: root}, nil
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

// Merge combines documents into one OpenAPI description. Namespaces are paired
// with documents and are used for component keys and local component refs.
func Merge(title, version string, documents map[string]*Document) ([]byte, error) {
	merged := map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title": title, "version": version,
			"description": "Aggregated API surface.",
		},
		"paths": map[string]any{},
	}
	components := map[string]any{}
	names := make([]string, 0, len(documents))
	for name := range documents {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		document := documents[name]
		if document == nil {
			continue
		}
		namespace := strings.ReplaceAll(name, "-", "_") + "_"
		rewritten := rewriteRefs(document.root, namespace).(map[string]any)
		if paths, ok := rewritten["paths"].(map[string]any); ok {
			maps.Copy(merged["paths"].(map[string]any), paths)
		}
		sections, ok := rewritten["components"].(map[string]any)
		if !ok {
			continue
		}
		for section, values := range sections {
			entries, ok := values.(map[string]any)
			if !ok {
				continue
			}
			target, ok := components[section].(map[string]any)
			if !ok {
				target = map[string]any{}
				components[section] = target
			}
			for key, value := range entries {
				target[namespace+key] = value
			}
		}
	}
	if len(components) > 0 {
		merged["components"] = components
	}
	return json.MarshalIndent(merged, "", "  ")
}

func rewriteRefs(node any, namespace string) any {
	switch typed := node.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, value := range typed {
			if key == "$ref" {
				if reference, ok := value.(string); ok {
					result[key] = namespaceRef(reference, namespace)
					continue
				}
			}
			result[key] = rewriteRefs(value, namespace)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, value := range typed {
			result[index] = rewriteRefs(value, namespace)
		}
		return result
	default:
		return node
	}
}

func namespaceRef(reference, namespace string) string {
	const prefix = "#/components/"
	if !strings.HasPrefix(reference, prefix) {
		return reference
	}
	rest := strings.TrimPrefix(reference, prefix)
	section, key, found := strings.Cut(rest, "/")
	if !found {
		return reference
	}
	return prefix + section + "/" + namespace + key
}
