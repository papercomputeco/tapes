// Package api provides an HTTP API server over the derived
// sessions/traces/spans read model.
package api

import (
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// Config is the API server configuration.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":8081")
	ListenAddr string

	// Pricing is the model pricing table used by /v1/sessions/summary to
	// compute per-session cost. When nil, sessions.DefaultPricing() is used.
	Pricing sessions.PricingTable

	// EnableWebUI serves the minimal browser UI at /. It is disabled by default
	// so API-only servers do not expose a human-facing development UI unless
	// explicitly requested.
	EnableWebUI bool

	// ContractVersions is the set of tapes contracts this server serves. A
	// cassette whose depends.core falls outside the set is refused at
	// admission, and the newest entry is what the discovery document
	// advertises as current. Empty means DefaultContractVersions().
	ContractVersions []cassette.ContractVersion
}
