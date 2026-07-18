package proxy

import (
	"context"
	"sync"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// captureDriver is an in-memory storage.Driver that ALSO satisfies
// storage.RawTurnStore and storage.SessionIngester, so the proxy's
// capture path — append to the raw-turn layer + upsert the sessions row
// — can be exercised without Postgres. It records the calls it received.
//
// The persisted merkle "node" DAG is retired, so the proxy never writes a
// node store; the embedded in-memory driver is only here to satisfy the
// reduced Driver (Open/Close) interface. Capture assertions read off the
// recorded RawTurnRecords (and IngestTurn requests) instead of the old
// List/Leaves/Ancestry node surface. Mirrors worker.captureDriver.
type captureDriver struct {
	*inmemory.Driver

	mu          sync.Mutex
	rawTurns    []storage.RawTurnRecord
	ingestCalls []storage.IngestTurnRequest
	sessionID   string
}

func newCaptureDriver() *captureDriver {
	return &captureDriver{
		Driver:    inmemory.NewDriver(),
		sessionID: "00000000-0000-0000-0000-000000000001",
	}
}

func (d *captureDriver) PutRawTurn(_ context.Context, rec storage.RawTurnRecord) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rawTurns = append(d.rawTurns, rec)
	return true, nil
}

func (d *captureDriver) ListRawTurns(_ context.Context, _ int64, _ int32) ([]storage.RawTurnRecord, error) {
	return d.RawTurns(), nil
}

func (d *captureDriver) CountRawTurns(_ context.Context) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(len(d.rawTurns)), nil
}

func (d *captureDriver) IngestTurn(_ context.Context, req storage.IngestTurnRequest) (storage.IngestTurnResult, error) {
	d.mu.Lock()
	d.ingestCalls = append(d.ingestCalls, req)
	d.mu.Unlock()
	return storage.IngestTurnResult{SessionID: d.sessionID}, nil
}

// RawTurns returns a copy of every raw turn the proxy captured.
func (d *captureDriver) RawTurns() []storage.RawTurnRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]storage.RawTurnRecord, len(d.rawTurns))
	copy(out, d.rawTurns)
	return out
}

// IngestCalls returns a copy of every session-ingest request the proxy issued.
func (d *captureDriver) IngestCalls() []storage.IngestTurnRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]storage.IngestTurnRequest, len(d.ingestCalls))
	copy(out, d.ingestCalls)
	return out
}
