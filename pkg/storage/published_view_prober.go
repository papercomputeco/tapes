package storage

import "context"

// PublishedViewProber is the capability a driver exposes when it can verify
// that a published view is actually readable with the driver's own database
// role — the same role that later renders the claim's EXISTS predicates, so
// a probe that succeeds proves exactly what the request path needs.
//
// One probe validates three things in a single round trip: the view exists,
// the role may SELECT from it, and it carries the contract columns plus the
// claim-declared value column. It proves readability at probe time, not
// forever — callers re-probe on their own cadence and must stay loud when an
// armed filter breaks between probes.
type PublishedViewProber interface {
	// ProbePublishedView reports nil when the view is readable in the shape
	// the claim declared. A non-nil error is either the store's own verdict
	// (the store answered and refused) or a *TransientProbeError when the
	// store could not be asked at all — implementations must keep the two
	// distinguishable, because callers that cache the verdict may only
	// change cached state on an actual answer.
	ProbePublishedView(ctx context.Context, view PublishedViewName, column PublishedColumnName) error
}

// TransientProbeError marks a probe attempt the store never answered — a
// timeout, a refused connection, a closed pool. It is not a verdict about
// the view: the same probe may well succeed a moment later, so callers that
// cache probe outcomes retain their prior state rather than treating the
// blip as a refusal.
type TransientProbeError struct {
	Err error
}

func (failure *TransientProbeError) Error() string {
	return "published view probe did not reach the store: " + failure.Err.Error()
}

// Unwrap exposes the underlying failure for errors.Is/As.
func (failure *TransientProbeError) Unwrap() error { return failure.Err }
