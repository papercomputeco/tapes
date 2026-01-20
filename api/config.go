// Package api provides an HTTP API server for inspecting and managing the Merkle DAG.
package api

// Config is the API server configuration.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":8081")
	ListenAddr string
}
