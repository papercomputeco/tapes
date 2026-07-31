// Package api provides an HTTP API server over the derived
// sessions/traces/spans read model.
package api

import (
	"context"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/embeddings"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/spanembed"
)

// SpanSearcher performs vector-similarity search over span
// embeddings. *spanembed.Store implements it; tests substitute fakes.
type SpanSearcher interface {
	Search(ctx context.Context, orgID string, embedding []float32, topK int) ([]spanembed.Hit, error)
}

// Config is the API server configuration.
type Config struct {
	// ListenAddr is the address to listen on (e.g., ":8081")
	ListenAddr string

	// Embedder for converting query text to vectors (optional, enables MCP server)
	Embedder embeddings.Embedder

	// SpanSearcher enables GET /v1/search/spans — semantic search over
	// the span projection's embeddings (optional). Requires Embedder.
	SpanSearcher SpanSearcher

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

	// SkillLLM* configure the LLM used by POST /v1/skills/generate. They are
	// populated from the search/embedding credential so skill extraction
	// reuses the same shared key the platform already mounts for search —
	// no separate provider key. An empty Provider/APIKey falls back to the
	// generator's env/credentials resolution at call time.
	SkillLLMProvider string
	SkillLLMModel    string
	SkillLLMAPIKey   string
	SkillLLMBaseURL  string
}
