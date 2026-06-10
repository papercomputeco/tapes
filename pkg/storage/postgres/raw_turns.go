package postgres

import (
	"context"
	"errors"
	"fmt"

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

	rows, err := d.q.InsertRawTurn(ctx, gensqlc.InsertRawTurnParams{
		OrgID:            orgID,
		Source:           source,
		Provider:         rec.Provider,
		AgentName:        rec.AgentName,
		HarnessID:        rec.HarnessID,
		HarnessSessionID: rec.HarnessSessionID,
		RequestID:        rec.RequestID,
		RawRequest:       rec.RawRequest,
		Response:         rec.Response,
		Meta:             metaOrEmptyObject(rec.Meta),
		SessionEnvelope:  rec.SessionEnvelope,
	})
	if err != nil {
		return false, fmt.Errorf("insert raw turn: %w", err)
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

func rawTurnRecordFromRow(row gensqlc.RawTurn) storage.RawTurnRecord {
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
