package main

import (
	"bytes"
	"encoding/json"
	"strings"
)

// openAPIDocument renders this cassette's OpenAPI document.
//
// Every path is written under /api/<name>, which is what core's prefix
// admission requires (DESIGN §10.2): a fetched spec that declares an operation
// outside its own prefix is refused whole, because the operator approved the
// cassette before ever seeing its spec. Building the document from the runtime
// name rather than hardcoding "hello-world" means the same image installed
// under a second name publishes a correct spec for that name too.
//
// It is assembled by hand rather than generated because the point of this
// example is the contract, and a generator would put a build step between the
// reader and the thing being demonstrated.
func openAPIDocument(name string) []byte {
	prefix := "/api/" + name
	document := map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title":       "Hello World Cassette",
			"description": "The smallest API that is still a tapes cassette.",
			"version":     "0.0.1",
		},
		"x-tapes-cassette": map[string]any{
			"kind": "cassette/v1alpha1",
			"cassette": map[string]any{
				"name":         name,
				"version":      "0.0.1",
				"display_name": "Hello World",
				"description":  "The smallest API that is still a tapes cassette.",
				"license":      "Apache-2.0",
				"homepage":     "https://github.com/papercomputeco/tapes",
				"image":        "tapes/hello-world-cassette:0.0.1",
				"port":         9999,
			},
			"depends": map[string]any{
				"core":  "v1",
				"views": []string{},
			},
			"api": map[string]any{
				"health":      "/ping",
				"openapi":     "/openapi",
				"prefix_path": "api",
			},
			"tables": []map[string]any{{"name": "hello"}},
			"config": []map[string]any{{
				"key":         "greeting",
				"type":        "string",
				"default":     "Hello",
				"description": "Greeting returned by the cassette.",
			}},
		},
		"paths": map[string]any{
			prefix + "/hello": map[string]any{
				"get": map[string]any{
					"operationId": "getHello",
					"summary":     "Greet, and read back every stored row",
					"tags":        []string{name},
					"responses": map[string]any{
						"200": jsonResponse("The greeting and the contents of the hello table", map[string]any{
							"type": "object",
							"properties": map[string]any{
								"message":  map[string]any{"type": "string"},
								"greeting": map[string]any{"type": "string"},
								"cassette": map[string]any{"type": "string"},
								"store": map[string]any{
									"type":        "string",
									"enum":        []string{"memory", "postgres"},
									"description": "Which backing store answered. 'memory' is not durable.",
								},
								"rows": map[string]any{
									"type":  "array",
									"items": map[string]any{"$ref": "#/components/schemas/HelloRow"},
								},
							},
						}),
					},
				},
				"post": map[string]any{
					"operationId": "createHello",
					"summary":     "Write one row to the hello table",
					"tags":        []string{name},
					"requestBody": map[string]any{
						"required": false,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"type": "object",
									"properties": map[string]any{
										"hello": map[string]any{"type": "string", "default": "hello"},
										"world": map[string]any{"type": "string", "default": "world"},
									},
								},
							},
						},
					},
					"responses": map[string]any{
						"201": jsonResponse("The row that was written",
							map[string]any{"$ref": "#/components/schemas/HelloRow"}),
					},
				},
			},
		},
		"components": map[string]any{
			"schemas": map[string]any{
				"HelloRow": map[string]any{
					"type":     "object",
					"required": []string{"id", "hello", "world", "created_at"},
					"properties": map[string]any{
						"id":         map[string]any{"type": "integer", "format": "int64"},
						"hello":      map[string]any{"type": "string"},
						"world":      map[string]any{"type": "string"},
						"created_at": map[string]any{"type": "string", "format": "date-time"},
					},
				},
			},
		},
	}

	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetIndent("", "  ")
	// A map cannot fail to marshal here — every value is a literal built above
	// — and returning an error would force error handling into a handler that
	// has nothing useful to do with it.
	_ = encoder.Encode(document)

	return bytes.TrimRight(buffer.Bytes(), "\n")
}

func jsonResponse(description string, schema map[string]any) map[string]any {
	return map[string]any{
		"description": description,
		"content": map[string]any{
			"application/json": map[string]any{"schema": schema},
		},
	}
}

// RoutePrefix is the prefix this cassette serves under, exported for tests.
func RoutePrefix(name string) string { return "/api/" + strings.TrimPrefix(name, "/") }
