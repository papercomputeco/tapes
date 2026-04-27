// Package capture holds the shared streaming-reduction library consumed by
// both the local tapes proxy and the cloud tapes-extproc sidecar. A Reducer
// turns the raw bytes of a single LLM turn — streaming (SSE, NDJSON) or
// one-shot (application/json) — into a canonical *llm.ChatResponse suitable
// for handing to tapes-ingest.
//
// Callers construct reducers explicitly via NewXxxReducer constructors and
// dispatch them by provider name themselves; the package deliberately holds
// no global registry so import-order and init() side effects stay out of the
// call graph.
package capture

import (
	"context"
	"io"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Reducer produces a canonical *llm.ChatResponse from the raw request and
// response bodies for a single turn. Implementations must be stateless at the
// package level; per-turn state is held in local variables for the duration of
// a single Reduce call.
type Reducer interface {
	// Reduce consumes the raw request body and the raw response body and
	// produces a canonical ChatResponse. The reqBody is supplied for context
	// (the reducer may enrich the response with metadata the wire format
	// doesn't carry); passing nil is valid when no request-side context is
	// available. contentType is the upstream response's Content-Type, used to
	// disambiguate streaming (text/event-stream, application/x-ndjson) from
	// non-streaming (application/json) reduction paths.
	//
	// Errors are reserved for unrecoverable parse failures or malformed
	// envelopes. Partial captures (mid-stream errors, EOF before terminal
	// frame, per-block parse failures) should return a ChatResponse with
	// diagnostic metadata in Extra rather than an error — callers want to
	// see what happened instead of silence.
	//
	// Per-turn memory is not bounded inside the reducer. Content grows with
	// the upstream's output, ceiling'd in practice by the caller's
	// max_tokens. Callers that need a hard ceiling should impose one
	// themselves; sidecar deployments track turn size via metrics rather
	// than enforcing a cap so the full tape is preserved.
	Reduce(ctx context.Context, reqBody, respBody io.Reader, contentType string) (*llm.ChatResponse, error)
}
