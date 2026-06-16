package ingest_test

import (
	"context"
	"sync"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// captureDriver is an in-memory storage.Driver that ALSO satisfies
// storage.RawTurnStore (and storage.SessionIngester), so the ingest
// server's capture path — append to the immutable raw-turn layer + upsert
// the sessions row — can be exercised without Postgres. It records the
// rows it received.
//
// The persisted merkle "node" DAG is retired, so ingest no longer writes a
// node store; the embedded in-memory driver is only here to satisfy the
// reduced Driver (Open/Close) interface. Contract specs assert against the
// recorded RawTurnRecords instead of the old node List surface.
type captureDriver struct {
	*inmemory.Driver

	mu          sync.Mutex
	rawTurns    []storage.RawTurnRecord
	ingestCalls []storage.IngestTurnRequest
}

func newCaptureDriver() *captureDriver {
	return &captureDriver{Driver: inmemory.NewDriver()}
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
	return storage.IngestTurnResult{SessionID: "00000000-0000-0000-0000-000000000001"}, nil
}

// RawTurns returns a copy of every raw turn the server captured.
func (d *captureDriver) RawTurns() []storage.RawTurnRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]storage.RawTurnRecord, len(d.rawTurns))
	copy(out, d.rawTurns)
	return out
}

// CountRaw returns the number of captured raw turns.
func (d *captureDriver) CountRaw() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.rawTurns)
}
