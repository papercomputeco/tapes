package storage

import (
	"context"
	"encoding/json"
	"time"
)

// Raw-turn source discriminators. The raw layer is source-agnostic so
// every capture origin lands in the same substrate; the deriver treats
// rows uniformly.
const (
	// RawTurnSourceWire marks turns captured on the wire
	// (Envoy → extproc → ingest): request verbatim as sent to the
	// provider, response reduced from the SSE stream by the capture
	// adapter.
	RawTurnSourceWire = "wire"

	// RawTurnSourceTranscript marks turns ingested from a harness
	// on-disk transcript (parentUuid causal records + subagent
	// metadata).
	RawTurnSourceTranscript = "transcript"
)

// RawTurnRecord is one immutable captured turn, stored verbatim before
// any parsing or projection. The JSON payloads are the exact bytes from
// the ingest envelope — never re-marshaled through parsed structs — so
// fields unknown to the current build survive for later derivers.
type RawTurnRecord struct {
	// OrgID is the canonical UUID string from the validated session
	// envelope. Empty means "no org context" and maps to the nil-UUID
	// sentinel, mirroring nodes.org_id.
	OrgID string

	// Source is one of the RawTurnSource* constants.
	Source string

	Provider  string
	AgentName string

	// HarnessID / HarnessSessionID echo the session envelope's natural
	// key so a session's raw turns are queryable without decoding the
	// envelope JSONB. Empty when the turn carried no envelope.
	HarnessID        string
	HarnessSessionID string

	// RequestID is the capture adapter's per-call id (extproc forwards
	// the Envoy request id). It dedupes retried POSTs of the same
	// captured turn; empty disables deduplication for the row.
	RequestID string

	// RawRequest is the provider request body, verbatim.
	RawRequest json.RawMessage

	// Response is the reduced provider response, verbatim from the
	// envelope.
	Response json.RawMessage

	// Meta is the capture adapter's metadata block (model, stream,
	// upstream status, byte counts, elapsed seconds, …), verbatim.
	Meta json.RawMessage

	// SessionEnvelope is the session-tracking block, verbatim.
	SessionEnvelope json.RawMessage

	// ReceivedAt is populated by the store on read; ignored on write
	// (the database stamps insertion time).
	ReceivedAt time.Time

	// ID is the store-assigned monotonic row id, populated on read.
	ID int64
}

// RawTurnStore is an optional capability for a Driver: an append-only,
// immutable store of full capture envelopes. The derived layer (nodes,
// edges, typing) is a pure re-runnable function of these rows — see
// pkg/derive.
//
// Only drivers that can host the raw layer implement this (Postgres
// does; in-memory intentionally does not). Callers MUST type-assert.
type RawTurnStore interface {
	// PutRawTurn appends one captured turn. Returns false when the
	// row was deduplicated (same org + request id already stored).
	PutRawTurn(ctx context.Context, rec RawTurnRecord) (bool, error)

	// ListRawTurns scans the raw layer in insertion order, returning
	// up to pageSize rows with id greater than afterID. Pass 0 to
	// start from the beginning.
	ListRawTurns(ctx context.Context, afterID int64, pageSize int32) ([]RawTurnRecord, error)

	// CountRawTurns reports the total number of raw rows.
	CountRawTurns(ctx context.Context) (int64, error)
}
