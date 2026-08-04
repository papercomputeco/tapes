// Command mcp-tool is the smallest cassette that advertises an MCP tool.
package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	defaultListen = "127.0.0.1:9999"
	defaultName   = "mcp-tool"
)

type pingRequest struct {
	Ping string `json:"ping"`
}

func main() {
	listen := os.Getenv("CASSETTE_LISTEN")
	if listen == "" {
		listen = defaultListen
	}
	name := os.Getenv("CASSETTE_NAME")
	if name == "" {
		name = defaultName
	}

	log.Printf("mcp-tool cassette listening on %s", listen)
	server := &http.Server{
		Addr:              listen,
		Handler:           routes(name),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}

func routes(name string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /openapi", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, openAPIDocument(name))
	})
	mux.HandleFunc("POST /api/"+name+"/ping", handlePing)
	return mux
}

func handlePing(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()

	var body pingRequest
	if err := decoder.Decode(&body); err != nil || body.Ping != "ping" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": `body must be {"ping":"ping"}`})
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must contain one JSON object"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"pong": "pong"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func openAPIDocument(name string) map[string]any {
	object := func(properties map[string]any, required string) map[string]any {
		return map[string]any{
			"type":                 "object",
			"properties":           properties,
			"required":             []string{required},
			"additionalProperties": false,
		}
	}

	return map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title":   "MCP Tool Cassette",
			"version": "0.0.1",
		},
		"x-tapes-cassette": map[string]any{
			"kind": "cassette/v1alpha1",
			"cassette": map[string]any{
				"name":         name,
				"version":      "0.0.1",
				"display_name": "MCP Tool",
				"description":  "A minimal cassette-advertised MCP tool.",
			},
			"depends": map[string]any{"core": "v1", "views": []string{}},
			"api": map[string]any{
				"health":      "/ping",
				"openapi":     "/openapi",
				"prefix_path": "api",
			},
		},
		"paths": map[string]any{
			"/api/" + name + "/ping": map[string]any{
				"post": map[string]any{
					"operationId": "ping",
					"summary":     "Reply pong to ping",
					"x-tapes-mcp": map[string]any{
						"name": "ping",
						"annotations": map[string]any{
							"readOnlyHint":   true,
							"idempotentHint": true,
							"openWorldHint":  false,
						},
					},
					"requestBody": map[string]any{
						"required": true,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": object(map[string]any{
									"ping": map[string]any{"type": "string", "const": "ping"},
								}, "ping"),
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Pong",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": object(map[string]any{
										"pong": map[string]any{"type": "string", "const": "pong"},
									}, "pong"),
								},
							},
						},
					},
				},
			},
		},
	}
}
