package embeddings

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/papercomputeco/tapes/pkg/vector"
)

// APIError is a structured error from an embedding provider's HTTP API. It
// lets the embed pass tell three failure modes apart:
//
//   - oversize input (IsOversize) — the model rejected the text for exceeding
//     its context window; the caller should chunk the input and embed the
//     pieces rather than retry the whole thing.
//   - transient fault (Retryable) — a rate limit, server error, or transport
//     failure; the same input may succeed on a later pass.
//   - any other deterministic rejection — neither of the above; retrying the
//     same input is pointless, so the caller records it and stops.
//
// It unwraps to vector.ErrEmbedding so existing errors.Is(err,
// vector.ErrEmbedding) checks keep working.
type APIError struct {
	// Status is the HTTP status code, or 0 for a transport-level failure
	// (connection refused, timeout) where no response was received.
	Status int
	// Code is the provider's machine error code when present (e.g.
	// "context_length_exceeded"); empty when the provider omits it.
	Code string
	// Message is the provider's human-readable error message, or the
	// transport error string when Status is 0.
	Message string
	// RequestedTokens is the token count the provider reported for the
	// rejected input, parsed from Message; 0 when not reported.
	RequestedTokens int
	// Transient marks a transport-level failure (Status 0) as retryable.
	Transient bool
}

func (e *APIError) Error() string {
	if e.Status == 0 {
		return fmt.Sprintf("%v: embedding api request failed: %s", vector.ErrEmbedding, e.Message)
	}
	return fmt.Sprintf("%v: embedding api returned status %d: %s", vector.ErrEmbedding, e.Status, e.Message)
}

// Unwrap reports vector.ErrEmbedding so callers can keep matching on the
// package's sentinel.
func (e *APIError) Unwrap() error { return vector.ErrEmbedding }

var maxContextRe = regexp.MustCompile(`(?i)maximum context length`)

// IsOversize reports whether the input was rejected for exceeding the model's
// context window — the signal to chunk the input and embed the pieces.
func (e *APIError) IsOversize() bool {
	if e.Status != http.StatusBadRequest {
		return false
	}
	return e.Code == "context_length_exceeded" || maxContextRe.MatchString(e.Message)
}

// Retryable reports whether the failure is transient (rate limit, server
// error, or transport failure) and the same input may succeed on a later pass.
func (e *APIError) Retryable() bool {
	return e.Transient || e.Status == http.StatusTooManyRequests || e.Status >= http.StatusInternalServerError
}

// AsAPIError extracts an *APIError from err, if one is present in the chain.
func AsAPIError(err error) (*APIError, bool) {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr, true
	}
	return nil, false
}

var requestedTokensRe = regexp.MustCompile(`requested (\d+) tokens`)

// ParseRequestedTokens extracts the token count a provider reports in an
// oversize message (e.g. "...however you requested 9523 tokens..."), returning
// 0 when no count is present.
func ParseRequestedTokens(message string) int {
	m := requestedTokensRe.FindStringSubmatch(message)
	if len(m) < 2 {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0
	}
	return n
}
