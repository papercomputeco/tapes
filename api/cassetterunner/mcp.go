package cassetterunner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// MCPExtension marks a cassette OpenAPI operation as an MCP tool.
const MCPExtension = "x-tapes-mcp"

const maxMCPTools = 128

var mcpToolNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// MCPTool is one admitted cassette operation exposed through Tapes' MCP server.
type MCPTool struct {
	Name        string
	Title       string
	Description string
	Method      string
	Path        string
	InputSchema map[string]any
	Annotations MCPToolAnnotations
}

// MCPToolAnnotations are MCP's behavioral hints. They are descriptive, not authorization.
type MCPToolAnnotations struct {
	DestructiveHint *bool
	IdempotentHint  bool
	OpenWorldHint   *bool
	ReadOnlyHint    bool
}

type mcpExtension struct {
	Name        string               `json:"name"`
	Annotations mcpAnnotationsObject `json:"annotations"`
}

type mcpAnnotationsObject struct {
	DestructiveHint *bool `json:"destructiveHint,omitempty"`
	IdempotentHint  bool  `json:"idempotentHint,omitempty"`
	OpenWorldHint   *bool `json:"openWorldHint,omitempty"`
	ReadOnlyHint    bool  `json:"readOnlyHint,omitempty"`
}

// extractMCPTools validates and extracts tools from a republished cassette document.
func extractMCPTools(document *tapesoapi.Document, name string) ([]MCPTool, error) {
	fragment, err := document.Fragment(tapesoapi.Provenance{Kind: tapesoapi.KindDocument, Name: name})
	if err != nil {
		return nil, fmt.Errorf("read MCP tool declarations: %w", err)
	}
	if _, misplaced := fragment.Extensions[MCPExtension]; misplaced {
		return nil, fmt.Errorf("%s must be declared on an operation", MCPExtension)
	}

	components := map[string]*tapesoapi.Schema{}
	if fragment.Components != nil {
		components = fragment.Components.Schemas
	}
	paths, err := document.Paths()
	if err != nil {
		return nil, err
	}
	tools := make([]MCPTool, 0)
	seen := make(map[string]struct{})
	for _, path := range paths {
		normalized, err := tapesoapi.NormalizePath(path)
		if err != nil {
			return nil, err
		}
		item := fragment.Paths[normalized]
		if item == nil {
			continue
		}
		if _, misplaced := item.Extensions[MCPExtension]; misplaced {
			return nil, fmt.Errorf("%s %s: %s must be declared on an operation", http.MethodPost, path, MCPExtension)
		}
		for _, method := range item.Methods() {
			operation := item.Operations[method]
			if operation == nil {
				continue
			}
			extension, advertised := operation.Extensions[MCPExtension]
			if !advertised {
				continue
			}
			tool, err := parseMCPTool(document, fragment.Version, components, name, path, method, item.Parameters, operation, extension)
			if err != nil {
				return nil, err
			}
			if _, duplicate := seen[tool.Name]; duplicate {
				return nil, fmt.Errorf("%s %s: MCP tool name %q is declared more than once", method, path, tool.Name)
			}
			seen[tool.Name] = struct{}{}
			tools = append(tools, tool)
			if len(tools) > maxMCPTools {
				return nil, fmt.Errorf("cassette declares more than %d MCP tools", maxMCPTools)
			}
		}
	}

	return tools, nil
}

func parseMCPTool(document *tapesoapi.Document, version tapesoapi.Version, components map[string]*tapesoapi.Schema, cassetteName, path, method string, pathParameters []*tapesoapi.Parameter, operation *tapesoapi.Operation, rawExtension any) (MCPTool, error) {
	location := method + " " + path
	if method != http.MethodPost {
		return MCPTool{}, fmt.Errorf("%s: %s is supported only on POST operations", location, MCPExtension)
	}
	if version != tapesoapi.V31 {
		return MCPTool{}, fmt.Errorf("%s: %s requires OpenAPI 3.1 JSON Schema", location, MCPExtension)
	}
	if strings.Contains(path, "{") || len(pathParameters) > 0 || len(operation.Parameters) > 0 {
		return MCPTool{}, fmt.Errorf("%s: %s operations must not declare path or operation parameters; put all arguments in the JSON body", location, MCPExtension)
	}

	var extension mcpExtension
	encodedExtension, err := json.Marshal(rawExtension)
	if err != nil {
		return MCPTool{}, fmt.Errorf("%s: encode %s: %w", location, MCPExtension, err)
	}
	if err := json.Unmarshal(encodedExtension, &extension); err != nil {
		return MCPTool{}, fmt.Errorf("%s: decode %s: %w", location, MCPExtension, err)
	}
	if extension.Name == "" || !mcpToolNamePattern.MatchString(extension.Name) {
		return MCPTool{}, fmt.Errorf("%s: %s.name must contain only ASCII letters, digits, dots, dashes, or underscores", location, MCPExtension)
	}
	fullName := cassetteName + "." + extension.Name
	if len(fullName) > 128 {
		return MCPTool{}, fmt.Errorf("%s: MCP tool name %q exceeds 128 bytes", location, fullName)
	}
	if operation.RequestBody == nil || operation.RequestBody.Ref != "" || !operation.RequestBody.Required {
		return MCPTool{}, fmt.Errorf("%s: %s requires an inline, required application/json requestBody", location, MCPExtension)
	}
	mediaType, media := jsonMediaType(operation.RequestBody.Content)
	if media == nil || media.Schema == nil {
		return MCPTool{}, fmt.Errorf("%s: %s requires an application/json request schema", location, MCPExtension)
	}
	if !objectSchema(media.Schema, components, map[string]bool{}) {
		return MCPTool{}, fmt.Errorf("%s: %s input schema: root schema must resolve to an object", location, MCPExtension)
	}
	input, err := document.StandaloneRequestSchema(method, path, mediaType)
	if err != nil {
		return MCPTool{}, fmt.Errorf("%s: %s input schema: %w", location, MCPExtension, err)
	}
	// MCP requires an explicit root object type even when the schema reaches it
	// through a reference.
	input["type"] = "object"
	if err := resolveInputSchema(input); err != nil {
		return MCPTool{}, fmt.Errorf("%s: %s input schema: %w", location, MCPExtension, err)
	}
	if err := validateMCPResponses(operation.Responses, components); err != nil {
		return MCPTool{}, fmt.Errorf("%s: %s response: %w", location, MCPExtension, err)
	}

	description := operation.Description
	if description == "" {
		description = operation.Summary
	}
	return MCPTool{
		Name:        fullName,
		Title:       operation.Summary,
		Description: description,
		Method:      http.MethodPost,
		Path:        path,
		InputSchema: input,
		Annotations: MCPToolAnnotations{
			DestructiveHint: extension.Annotations.DestructiveHint,
			IdempotentHint:  extension.Annotations.IdempotentHint,
			OpenWorldHint:   extension.Annotations.OpenWorldHint,
			ReadOnlyHint:    extension.Annotations.ReadOnlyHint,
		},
	}, nil
}

func jsonMediaType(content map[string]*tapesoapi.MediaType) (string, *tapesoapi.MediaType) {
	for _, declared := range sortedKeys(content) {
		mediaType, _, err := mime.ParseMediaType(declared)
		if err == nil && mediaType == "application/json" {
			return declared, content[declared]
		}
	}

	return "", nil
}

func objectSchema(schema *tapesoapi.Schema, components map[string]*tapesoapi.Schema, seen map[string]bool) bool {
	if schema == nil {
		return false
	}
	if schema.Type == tapesoapi.TypeObject {
		return true
	}
	token, ok := strings.CutPrefix(schema.Ref, "#/components/schemas/")
	if !ok {
		return false
	}
	name := strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
	if seen[name] {
		return false
	}
	seen[name] = true

	return objectSchema(components[name], components, seen)
}

func resolveInputSchema(input map[string]any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	var schema jsonschema.Schema
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err := decoder.Decode(&schema); err != nil {
		return err
	}
	_, err = schema.Resolve(&jsonschema.ResolveOptions{ValidateDefaults: true})

	return err
}

func validateMCPResponses(responses map[string]*tapesoapi.Response, components map[string]*tapesoapi.Schema) error {
	found := false
	for _, status := range sortedKeys(responses) {
		code, err := strconv.Atoi(status)
		if status != "2XX" && (err != nil || code < http.StatusOK || code >= http.StatusMultipleChoices) {
			continue
		}
		found = true
		response := responses[status]
		if response == nil || response.Ref != "" {
			return fmt.Errorf("%s must be an inline application/json object", status)
		}
		_, media := jsonMediaType(response.Content)
		if media == nil || !objectSchema(media.Schema, components, map[string]bool{}) {
			return fmt.Errorf("%s must declare an application/json object schema", status)
		}
	}
	if !found {
		return errors.New("must declare at least one 2xx application/json object response")
	}

	return nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	return keys
}
