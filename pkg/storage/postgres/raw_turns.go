package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// Compile-time guarantee that the Postgres driver hosts the raw-turn
// capability. Same rationale as the SessionIngester assertion: callers
// type-assert at runtime, so a signature drift would silently disable
// raw capture rather than fail the build.
var _ storage.RawTurnStore = (*Driver)(nil)

// PutRawTurn implements storage.RawTurnStore. The row is appended
// verbatim; a retried POST with the same (org_id, request_id) is a
// no-op per the partial unique index.
//
// Session-keyed rows also mark the session dirty in derive_queue, in
// the same transaction, so the derive worker picks the session up.
// Marking happens even when the row deduped: a re-POST of an existing
// turn is the natural "re-derive this session" signal, and a redundant
// mark only costs one idempotent derive.
func (d *Driver) PutRawTurn(ctx context.Context, rec storage.RawTurnRecord) (bool, error) {
	if d == nil || d.conn == nil {
		return false, errors.New("postgres driver not open")
	}

	orgID, err := orgIDFromString(rec.OrgID)
	if err != nil {
		return false, fmt.Errorf("decode org_id: %w", err)
	}

	source := rec.Source
	if source == "" {
		source = storage.RawTurnSourceWire
	}

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // commit shadows on success
	qtx := d.q.WithTx(tx)

	// Scrub the JSONB payloads of escapes/bytes Postgres cannot store
	// (SQLSTATE 22P05 / 22021). A clean payload passes through byte-identical,
	// so the raw layer stays verbatim for everything but the offending
	// sequences — which could not be stored faithfully anyway.
	rows, err := qtx.InsertRawTurn(ctx, gensqlc.InsertRawTurnParams{
		OrgID:            orgID,
		Source:           source,
		Provider:         rec.Provider,
		AgentName:        rec.AgentName,
		HarnessID:        rec.HarnessID,
		HarnessSessionID: rec.HarnessSessionID,
		RequestID:        rec.RequestID,
		RawRequest:       sanitizeJSONB(rec.RawRequest),
		Response:         sanitizeJSONB(rec.Response),
		Meta:             sanitizeJSONB(metaOrEmptyObject(rec.Meta)),
		SessionEnvelope:  sanitizeJSONB(rec.SessionEnvelope),

		// Not scrubbed. sanitizeJSONB exists because Postgres rejects certain
		// escapes and NUL bytes inside JSONB; BYTEA has no such constraint and
		// stores any byte string as-is. Scrubbing here would corrupt the one
		// column whose entire purpose is to be byte-faithful.
		RawResponse:         rec.RawResponse,
		RawResponseEncoding: rec.RawResponseEncoding,
		RawResponseDropped:  rec.RawResponseDropped,
	})
	if err != nil {
		// Classify so a content-level rejection that slips past the scrubber
		// surfaces as storage.ErrInvalidContent (→ 4xx) rather than a generic
		// downstream fault (→ 502).
		return false, fmt.Errorf("insert raw turn: %w", asContentError(err))
	}

	if source == storage.RawTurnSourceTranscript && rec.HarnessSessionID != "" {
		if err := upsertTranscriptSession(ctx, qtx, orgID, rec); err != nil {
			return false, err
		}
	}

	if rec.HarnessSessionID != "" {
		if err := qtx.MarkDeriveDirty(ctx, gensqlc.MarkDeriveDirtyParams{
			OrgID:            orgID,
			HarnessID:        rec.HarnessID,
			HarnessSessionID: rec.HarnessSessionID,
		}); err != nil {
			return false, fmt.Errorf("mark derive dirty: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit raw turn: %w", err)
	}
	return rows > 0, nil
}

// ListRawTurns implements storage.RawTurnStore.
func (d *Driver) ListRawTurns(ctx context.Context, afterID int64, pageSize int32) ([]storage.RawTurnRecord, error) {
	if d == nil || d.conn == nil {
		return nil, errors.New("postgres driver not open")
	}
	rows, err := d.q.ListRawTurns(ctx, gensqlc.ListRawTurnsParams{
		AfterID:  afterID,
		PageSize: pageSize,
	})
	if err != nil {
		return nil, fmt.Errorf("list raw turns: %w", err)
	}
	out := make([]storage.RawTurnRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, rawTurnRecordFromRow(row))
	}
	return out, nil
}

// CountRawTurns implements storage.RawTurnStore.
func (d *Driver) CountRawTurns(ctx context.Context) (int64, error) {
	if d == nil || d.conn == nil {
		return 0, errors.New("postgres driver not open")
	}
	return d.q.CountRawTurns(ctx)
}

func rawTurnRecordFromRow(row gensqlc.ListRawTurnsRow) storage.RawTurnRecord {
	return storage.RawTurnRecord{
		ID:               row.ID,
		OrgID:            uuidString(row.OrgID),
		Source:           row.Source,
		Provider:         row.Provider,
		AgentName:        row.AgentName,
		HarnessID:        row.HarnessID,
		HarnessSessionID: row.HarnessSessionID,
		RequestID:        row.RequestID,
		RawRequest:       row.RawRequest,
		Response:         row.Response,
		Meta:             row.Meta,
		SessionEnvelope:  row.SessionEnvelope,
		ReceivedAt:       row.ReceivedAt.Time,

		RawResponse:         row.RawResponse,
		RawResponseEncoding: row.RawResponseEncoding,
		RawResponseDropped:  row.RawResponseDropped,
	}
}

// rawTurnRecordFromEffectiveRow converts the GetRawTurn projection — the raw
// row as seen through the latest attribution correction — to the record shape
// the deriver consumes.
//
// GetRawTurn selects raw_response only for turns whose reduction is missing
// its content blocks (see the query). The deriver reads the reduced `response`
// column and has no use for the verbatim bytes on a healthy turn, and those
// bytes run to the ingest cap — pulling them through every derive read would
// move megabytes per turn to be discarded.
//
// RawResponseDropped is not selected at all: it is a fidelity marker for the
// projection, not an input to derivation, and it is zero here because the
// query didn't ask — which is not the same as the row not having it.
func rawTurnRecordFromEffectiveRow(row gensqlc.GetRawTurnRow) storage.RawTurnRecord {
	return storage.RawTurnRecord{
		ID:               row.ID,
		OrgID:            uuidString(row.OrgID),
		Source:           row.Source,
		Provider:         row.Provider,
		AgentName:        row.AgentName,
		HarnessID:        row.HarnessID,
		HarnessSessionID: row.HarnessSessionID,
		RequestID:        row.RequestID,
		RawRequest:       row.RawRequest,
		Response:         row.Response,
		Meta:             row.Meta,
		SessionEnvelope:  row.SessionEnvelope,
		ReceivedAt:       row.ReceivedAt.Time,

		RawResponse:         row.RawResponse,
		RawResponseEncoding: row.RawResponseEncoding,
	}
}

// metaOrEmptyObject satisfies the NOT NULL meta column for envelopes
// that omitted the block entirely.
func metaOrEmptyObject(meta []byte) []byte {
	if len(meta) == 0 {
		return []byte("{}")
	}
	return meta
}

func upsertTranscriptSession(ctx context.Context, qtx *gensqlc.Queries, orgID pgtype.UUID, rec storage.RawTurnRecord) error {
	var envelope sessions.IngestEnvelope
	if len(rec.SessionEnvelope) == 0 {
		return errors.New("upsert transcript session: missing session envelope")
	}
	if err := json.Unmarshal(rec.SessionEnvelope, &envelope); err != nil {
		return fmt.Errorf("decode transcript session envelope: %w", err)
	}

	now := time.Now().UTC()
	startedAt, lastSeenAt := transcriptTimeRange(rec.RawRequest, now)
	startedTS := pgtype.Timestamptz{Time: startedAt, Valid: true}
	lastSeenTS := pgtype.Timestamptz{Time: lastSeenAt, Valid: true}
	parentID, err := resolveParentSessionID(ctx, qtx, &envelope, orgID, lastSeenTS)
	if err != nil {
		return fmt.Errorf("resolve transcript parent session: %w", err)
	}
	id, err := newAppUUID()
	if err != nil {
		return fmt.Errorf("mint transcript session uuid: %w", err)
	}
	metadata := []byte(envelope.HarnessMetadata)
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}
	_, err = qtx.UpsertSessionFromTranscript(ctx, gensqlc.UpsertSessionFromTranscriptParams{
		ID: id, OrgID: orgID, AuthSubject: envelope.AuthSubject,
		HarnessID: envelope.HarnessIDOrUnknown(), HarnessSessionID: rec.HarnessSessionID,
		Name: nullStringValue(envelope.Name), Cwd: nullStringValue(envelope.Cwd),
		HarnessVersion: nullStringValue(envelope.HarnessVersion), ParentSessionID: parentID,
		StartedAt: startedTS, LastSeenAt: lastSeenTS, HarnessMetadata: metadata,
	})
	if err != nil {
		return fmt.Errorf("upsert transcript session: %w", err)
	}
	return nil
}

// transcriptTimeRange widens over every valid on-disk timestamp. Invalid and
// unsupported records remain raw but cannot distort chronology; when no valid
// timestamp exists, ingest time anchors both ends.
func transcriptTimeRange(records json.RawMessage, fallback time.Time) (time.Time, time.Time) {
	var rows []json.RawMessage
	if json.Unmarshal(records, &rows) != nil {
		return fallback, fallback
	}
	var earliest, latest time.Time
	include := func(at time.Time) {
		if at.IsZero() {
			return
		}
		if earliest.IsZero() || at.Before(earliest) {
			earliest = at
		}
		if latest.IsZero() || at.After(latest) {
			latest = at
		}
	}
	for _, raw := range rows {
		var row struct {
			Timestamp json.RawMessage `json:"timestamp"`
			Payload   json.RawMessage `json:"payload"`
		}
		if json.Unmarshal(raw, &row) != nil {
			continue
		}

		var timestamp string
		if json.Unmarshal(row.Timestamp, &timestamp) == nil {
			at, err := time.Parse(time.RFC3339Nano, timestamp)
			if err == nil {
				include(at)
			}
		}

		var payload struct {
			OccurredAtMS json.RawMessage `json:"occurred_at_ms"`
		}
		if json.Unmarshal(row.Payload, &payload) != nil {
			continue
		}
		var occurredAtMS int64
		if json.Unmarshal(payload.OccurredAtMS, &occurredAtMS) == nil && occurredAtMS > 0 {
			include(time.UnixMilli(occurredAtMS).UTC())
		}
	}
	if earliest.IsZero() {
		return fallback, fallback
	}
	return earliest, latest
}
