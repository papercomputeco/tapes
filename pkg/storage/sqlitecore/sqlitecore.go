// Package sqlitecore is the deliberately small, single-process SQLite core store.
package sqlitecore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/google/uuid"
	// Register the pure-Go SQLite driver used by this storage package.
	_ "modernc.org/sqlite"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
)

type Driver struct {
	db       *sql.DB
	fileLock *flock.Flock
	write    sync.Mutex
	locks    sync.Map
}

var (
	_ storage.Driver          = (*Driver)(nil)
	_ storage.RawTurnStore    = (*Driver)(nil)
	_ storage.SessionIngester = (*Driver)(nil)
	_ storage.DeriveQueue     = (*Driver)(nil)
)

// NewDriver opens a local-only core store. It intentionally has no vector or skills capabilities.
func NewDriver(ctx context.Context, path string) (*Driver, error) {
	if path == "" {
		return nil, errors.New("empty sqlite path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	fileLock := flock.New(path + ".lock")
	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock local SQLite store: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("local SQLite store is already in use: %s", path)
	}

	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if err != nil {
		_ = fileLock.Unlock()
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(4)
	d := &Driver{db: db, fileLock: fileLock}
	if err = d.Open(ctx); err != nil {
		_ = db.Close()
		_ = fileLock.Unlock()
		return nil, err
	}
	return d, nil
}
func (d *Driver) Open(ctx context.Context) error { _, err := d.db.ExecContext(ctx, schema); return err }
func (d *Driver) Close() error {
	if d == nil {
		return nil
	}
	var result error
	if d.db != nil {
		result = d.db.Close()
	}
	if d.fileLock != nil {
		if err := d.fileLock.Unlock(); result == nil {
			result = err
		}
	}
	return result
}

const schema = `
CREATE TABLE IF NOT EXISTS raw_turns (id INTEGER PRIMARY KEY, org_id TEXT NOT NULL, source TEXT NOT NULL, provider TEXT NOT NULL, agent_name TEXT NOT NULL, harness_id TEXT NOT NULL, harness_session_id TEXT NOT NULL, request_id TEXT NOT NULL, raw_request BLOB, response BLOB, raw_response BLOB, raw_response_encoding TEXT NOT NULL, raw_response_dropped INTEGER NOT NULL, meta BLOB NOT NULL, session_envelope BLOB, received_at INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS raw_turn_request ON raw_turns(org_id,request_id) WHERE request_id<>'';
CREATE INDEX IF NOT EXISTS raw_turn_session ON raw_turns(org_id,harness_id,harness_session_id);
CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, org_id TEXT NOT NULL, harness_id TEXT NOT NULL, harness_session_id TEXT NOT NULL, name TEXT NOT NULL DEFAULT '', display_name TEXT NOT NULL DEFAULT '', derived_title TEXT NOT NULL DEFAULT '', cwd TEXT NOT NULL DEFAULT '', harness_version TEXT NOT NULL DEFAULT '', parent_session_id TEXT, started_at INTEGER NOT NULL, last_seen_at INTEGER NOT NULL, auth_subject TEXT NOT NULL DEFAULT '', harness_metadata BLOB NOT NULL DEFAULT '{}', derived_status TEXT NOT NULL DEFAULT 'unknown', derived_model TEXT NOT NULL DEFAULT '', model_usage BLOB, tasks BLOB, kind_counts BLOB, total_input_tokens INTEGER NOT NULL DEFAULT 0,total_output_tokens INTEGER NOT NULL DEFAULT 0,total_cost_micros INTEGER NOT NULL DEFAULT 0,turn_count INTEGER NOT NULL DEFAULT 0, UNIQUE(org_id,harness_id,harness_session_id));
CREATE TABLE IF NOT EXISTS derive_queue (org_id TEXT NOT NULL,harness_id TEXT NOT NULL,harness_session_id TEXT NOT NULL,dirtied_at INTEGER NOT NULL,first_dirtied_at INTEGER NOT NULL,PRIMARY KEY(org_id,harness_id,harness_session_id));
CREATE TABLE IF NOT EXISTS span_turns (org_id TEXT NOT NULL,trace_id TEXT NOT NULL,session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,user_prompt TEXT NOT NULL,response_preview TEXT NOT NULL,synthetic TEXT NOT NULL,status TEXT NOT NULL,source TEXT NOT NULL,started_at INTEGER NOT NULL,ended_at INTEGER,duration_ns INTEGER NOT NULL,total_input_tokens INTEGER NOT NULL,total_output_tokens INTEGER NOT NULL,main_input_tokens INTEGER NOT NULL,main_output_tokens INTEGER NOT NULL,cache_read_tokens INTEGER NOT NULL,cache_creation_tokens INTEGER NOT NULL,total_cost_micros INTEGER NOT NULL,PRIMARY KEY(org_id,trace_id));
CREATE TABLE IF NOT EXISTS spans (org_id TEXT NOT NULL,trace_id TEXT NOT NULL,span_id TEXT NOT NULL,parent_span_id TEXT NOT NULL,session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,kind TEXT NOT NULL,name TEXT NOT NULL,status TEXT NOT NULL,call_kind TEXT NOT NULL,thread_id TEXT NOT NULL,model TEXT NOT NULL,stop_reason TEXT NOT NULL,started_at INTEGER NOT NULL,duration_ns INTEGER NOT NULL,seq INTEGER NOT NULL,input BLOB,output BLOB,usage BLOB,raw_turn_id INTEGER,node_hash TEXT NOT NULL,verdict BLOB,PRIMARY KEY(org_id,trace_id,span_id),FOREIGN KEY(org_id,trace_id) REFERENCES span_turns(org_id,trace_id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS span_links (org_id TEXT NOT NULL,from_trace_id TEXT NOT NULL,from_span_id TEXT NOT NULL,from_io TEXT NOT NULL,to_trace_id TEXT NOT NULL,to_span_id TEXT NOT NULL,to_io TEXT NOT NULL,kind TEXT NOT NULL,session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,PRIMARY KEY(org_id,from_trace_id,from_span_id,to_trace_id,to_span_id,from_io,to_io));`

func org(v string) string {
	if v == "" {
		return "00000000-0000-0000-0000-000000000000"
	}
	return v
}
func ns(t time.Time) int64 { return t.UTC().UnixNano() }
func tm(n int64) time.Time { return time.Unix(0, n).UTC() }
func (d *Driver) PutRawTurn(ctx context.Context, r storage.RawTurnRecord) (bool, error) {
	d.write.Lock()
	defer d.write.Unlock()
	now := ns(time.Now())
	if r.Source == "" {
		r.Source = storage.RawTurnSourceWire
	}
	o := org(r.OrgID)
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `INSERT INTO raw_turns(org_id,source,provider,agent_name,harness_id,harness_session_id,request_id,raw_request,response,raw_response,raw_response_encoding,raw_response_dropped,meta,session_envelope,received_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`, o, r.Source, r.Provider, r.AgentName, r.HarnessID, r.HarnessSessionID, r.RequestID, r.RawRequest, r.Response, r.RawResponse, r.RawResponseEncoding, r.RawResponseDropped, meta(r.Meta), r.SessionEnvelope, now)
	if err != nil {
		return false, err
	}
	if r.HarnessSessionID != "" {
		if _, err = tx.ExecContext(ctx, `INSERT INTO derive_queue VALUES(?,?,?,?,?) ON CONFLICT(org_id,harness_id,harness_session_id) DO UPDATE SET dirtied_at=excluded.dirtied_at`, o, r.HarnessID, r.HarnessSessionID, now, now); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func meta(b []byte) []byte {
	if len(b) == 0 {
		return []byte("{}")
	}
	return b
}

func (d *Driver) ListRawTurns(ctx context.Context, after int64, limit int32) ([]storage.RawTurnRecord, error) {
	rows, e := d.db.QueryContext(ctx, `SELECT id,org_id,source,provider,agent_name,harness_id,harness_session_id,request_id,raw_request,response,raw_response,raw_response_encoding,raw_response_dropped,meta,session_envelope,received_at FROM raw_turns WHERE id>? ORDER BY id LIMIT ?`, after, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []storage.RawTurnRecord
	for rows.Next() {
		var r storage.RawTurnRecord
		var n int64
		var drop bool
		var request, response, metaJSON, envelope []byte
		if e = rows.Scan(&r.ID, &r.OrgID, &r.Source, &r.Provider, &r.AgentName, &r.HarnessID, &r.HarnessSessionID, &r.RequestID, &request, &response, &r.RawResponse, &r.RawResponseEncoding, &drop, &metaJSON, &envelope, &n); e != nil {
			return nil, e
		}
		r.RawRequest, r.Response, r.Meta, r.SessionEnvelope = request, response, metaJSON, envelope
		r.RawResponseDropped = drop
		r.ReceivedAt = tm(n)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (d *Driver) CountRawTurns(ctx context.Context) (int64, error) {
	var n int64
	e := d.db.QueryRowContext(ctx, "SELECT count(*) FROM raw_turns").Scan(&n)
	return n, e
}

func (d *Driver) IngestTurn(ctx context.Context, r storage.IngestTurnRequest) (storage.IngestTurnResult, error) {
	if len(r.Nodes) == 0 {
		return storage.IngestTurnResult{}, errors.New("no nodes")
	}
	e, key, err := sessions.ResolveHarnessSessionID(r.Session, r.Nodes[0].Hash)
	if err != nil {
		return storage.IngestTurnResult{}, err
	}
	o := org(e.OrgID)
	now := ns(time.Now())
	d.write.Lock()
	defer d.write.Unlock()
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return storage.IngestTurnResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var parent any
	if e.ParentHarnessSessionID != nil {
		var parentID string
		err = tx.QueryRowContext(ctx, `SELECT id FROM sessions WHERE org_id=? AND harness_id=? AND harness_session_id=?`, o, e.HarnessIDOrUnknown(), *e.ParentHarnessSessionID).Scan(&parentID)
		if errors.Is(err, sql.ErrNoRows) {
			parentID = uuid.Must(uuid.NewV7()).String()
			_, err = tx.ExecContext(ctx, `INSERT INTO sessions(id,org_id,harness_id,harness_session_id,started_at,last_seen_at,auth_subject,harness_metadata) VALUES(?,?,?,?,?,?,?,?)`, parentID, o, e.HarnessIDOrUnknown(), *e.ParentHarnessSessionID, now, now, e.AuthSubject, []byte("{}"))
		}
		if err != nil {
			return storage.IngestTurnResult{}, err
		}
		parent = parentID
	}

	id := uuid.Must(uuid.NewV7()).String()
	_, err = tx.ExecContext(ctx, `INSERT INTO sessions(id,org_id,harness_id,harness_session_id,name,cwd,harness_version,parent_session_id,started_at,last_seen_at,auth_subject,harness_metadata)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(org_id,harness_id,harness_session_id) DO UPDATE SET
			last_seen_at=excluded.last_seen_at,
			name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE sessions.name END,
			cwd=CASE WHEN excluded.cwd<>'' THEN excluded.cwd ELSE sessions.cwd END,
			harness_version=CASE WHEN excluded.harness_version<>'' THEN excluded.harness_version ELSE sessions.harness_version END,
			parent_session_id=COALESCE(excluded.parent_session_id,sessions.parent_session_id),
			auth_subject=excluded.auth_subject,
			harness_metadata=json_patch(sessions.harness_metadata,excluded.harness_metadata)`,
		id, o, e.HarnessIDOrUnknown(), key, e.Name, e.Cwd, e.HarnessVersion, parent, now, now, e.AuthSubject, meta(e.HarnessMetadata))
	if err != nil {
		return storage.IngestTurnResult{}, err
	}
	var got string
	if err = tx.QueryRowContext(ctx, `SELECT id FROM sessions WHERE org_id=? AND harness_id=? AND harness_session_id=?`, o, e.HarnessIDOrUnknown(), key).Scan(&got); err != nil {
		return storage.IngestTurnResult{}, err
	}
	if r.DerivedTitle != "" {
		if _, err = tx.ExecContext(ctx, `UPDATE sessions SET derived_title=? WHERE id=?`, r.DerivedTitle, got); err != nil {
			return storage.IngestTurnResult{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return storage.IngestTurnResult{}, err
	}
	return storage.IngestTurnResult{SessionID: got}, nil
}

func (d *Driver) MarkDeriveDirty(ctx context.Context, o, h, s string) error {
	d.write.Lock()
	defer d.write.Unlock()
	now := ns(time.Now())
	_, err := d.db.ExecContext(ctx, `INSERT INTO derive_queue VALUES(?,?,?,?,?) ON CONFLICT(org_id,harness_id,harness_session_id) DO UPDATE SET dirtied_at=excluded.dirtied_at`, org(o), h, s, now, now)
	return err
}

func (d *Driver) ListDeriveDirty(ctx context.Context, a, b time.Time, l int32) ([]storage.DeriveQueueEntry, error) {
	rows, e := d.db.QueryContext(ctx, `SELECT org_id,harness_id,harness_session_id,dirtied_at,first_dirtied_at FROM derive_queue WHERE dirtied_at<=? OR first_dirtied_at<=? ORDER BY dirtied_at LIMIT ?`, ns(a), ns(b), l)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []storage.DeriveQueueEntry
	for rows.Next() {
		var x storage.DeriveQueueEntry
		var da, fa int64
		if e = rows.Scan(&x.OrgID, &x.HarnessID, &x.HarnessSessionID, &da, &fa); e != nil {
			return nil, e
		}
		x.DirtiedAt = tm(da)
		x.FirstDirtiedAt = tm(fa)
		out = append(out, x)
	}
	return out, rows.Err()
}

func (d *Driver) GetDeriveDirty(ctx context.Context, o, h, s string) (*storage.DeriveQueueEntry, error) {
	x := &storage.DeriveQueueEntry{OrgID: org(o), HarnessID: h, HarnessSessionID: s}
	var a, b int64
	e := d.db.QueryRowContext(ctx, `SELECT dirtied_at,first_dirtied_at FROM derive_queue WHERE org_id=? AND harness_id=? AND harness_session_id=?`, x.OrgID, h, s).Scan(&a, &b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	x.DirtiedAt = tm(a)
	x.FirstDirtiedAt = tm(b)
	return x, nil
}

func (d *Driver) ClearDeriveDirty(ctx context.Context, e storage.DeriveQueueEntry) (bool, error) {
	d.write.Lock()
	defer d.write.Unlock()
	r, x := d.db.ExecContext(ctx, `DELETE FROM derive_queue WHERE org_id=? AND harness_id=? AND harness_session_id=? AND dirtied_at=?`, org(e.OrgID), e.HarnessID, e.HarnessSessionID, ns(e.DirtiedAt))
	if x != nil {
		return false, x
	}
	n, _ := r.RowsAffected()
	return n > 0, nil
}

func (d *Driver) SweepDeriveDirty(ctx context.Context, activeSince time.Time) (int64, error) {
	d.write.Lock()
	defer d.write.Unlock()
	now := ns(time.Now())
	result, err := d.db.ExecContext(ctx, `INSERT INTO derive_queue(org_id,harness_id,harness_session_id,dirtied_at,first_dirtied_at)
		SELECT org_id,harness_id,harness_session_id,?,? FROM raw_turns WHERE harness_session_id <> '' AND received_at >= ?
		GROUP BY org_id,harness_id,harness_session_id
		ON CONFLICT(org_id,harness_id,harness_session_id) DO NOTHING`, now, now, ns(activeSince))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (d *Driver) DeriveQueueStats(ctx context.Context) (storage.DeriveQueueStats, error) {
	var n int64
	var oldest sql.NullInt64
	e := d.db.QueryRowContext(ctx, "SELECT count(*),min(dirtied_at) FROM derive_queue").Scan(&n, &oldest)
	x := storage.DeriveQueueStats{Depth: n}
	if oldest.Valid {
		x.OldestDirtiedAt = tm(oldest.Int64)
	}
	return x, e
}

func (d *Driver) TryDeriveSessionLock(_ context.Context, o, h, s string) (func(), bool, error) {
	k := org(o) + "/" + h + "/" + s
	v, _ := d.locks.LoadOrStore(k, &sync.Mutex{})
	m := v.(*sync.Mutex)
	if !m.TryLock() {
		return func() {}, false, nil
	}
	return m.Unlock, true, nil
}

func (d *Driver) RederiveSession(ctx context.Context, project, o, h, s string) (*derive.RederiveReport, error) {
	rows, e := d.db.QueryContext(ctx, `SELECT id,org_id,source,provider,agent_name,harness_id,harness_session_id,request_id,raw_request,response,meta,session_envelope,received_at FROM raw_turns WHERE org_id=? AND harness_id=? AND harness_session_id=? ORDER BY id`, org(o), h, s)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	dv, e := derive.NewDeriver(project)
	if e != nil {
		return nil, e
	}
	for rows.Next() {
		var r storage.RawTurnRecord
		var n int64
		var request, response, metaJSON, envelope []byte
		if e = rows.Scan(&r.ID, &r.OrgID, &r.Source, &r.Provider, &r.AgentName, &r.HarnessID, &r.HarnessSessionID, &r.RequestID, &request, &response, &metaJSON, &envelope, &n); e != nil {
			return nil, e
		}
		r.RawRequest, r.Response, r.Meta, r.SessionEnvelope = request, response, metaJSON, envelope
		r.ReceivedAt = tm(n)
		dv.AddTurn(&r)
	}
	set := dv.Finish()
	if e = rows.Err(); e != nil {
		return nil, e
	}
	if e = d.writeSpans(ctx, org(o), h, s, set, derive.EmitSpans(set)); e != nil {
		return nil, e
	}
	return &set.Report, nil
}

func j(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal derived value: %v", err))
	}
	return b
}

func (d *Driver) writeSpans(ctx context.Context, o, h, s string, derived *derive.DerivedSet, set *derive.SpanSet) error {
	d.write.Lock()
	defer d.write.Unlock()
	tx, e := d.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer func() { _ = tx.Rollback() }()
	var sid string
	if e = tx.QueryRowContext(ctx, `SELECT id FROM sessions WHERE org_id=? AND harness_id=? AND harness_session_id=?`, o, h, s).Scan(&sid); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `DELETE FROM span_links WHERE session_id=?`, sid); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, `DELETE FROM span_turns WHERE session_id=?`, sid); e != nil {
		return e
	}
	var in, out int64
	var cost int64
	turnCount := 0
	models := map[string]int{}
	for _, t := range set.Turns {
		if t.Session.HarnessID != h || t.Session.HarnessSessionID != s {
			continue
		}
		c := int64(t.TotalCostUSD * 1e6)
		_, e = tx.ExecContext(ctx, `INSERT INTO span_turns VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, o, t.TraceID, sid, t.UserPrompt, t.ResponsePreview, t.Synthetic, "ok", t.Source, ns(t.StartedAt), ns(t.EndedAt), t.EndedAt.Sub(t.StartedAt).Nanoseconds(), t.TotalInputTokens, t.TotalOutputTokens, t.MainInputTokens, t.MainOutputTokens, t.CacheReadTokens, t.CacheCreationTokens, c)
		if e != nil {
			return e
		}
		turnCount++
		in += t.TotalInputTokens
		out += t.TotalOutputTokens
		cost += c
		for _, sp := range t.Spans {
			if sp.Kind == "llm" && sp.CallKind == derive.KindMain && sp.ThreadID == "" && sp.Model != "" {
				models[sp.Model]++
			}
			_, e = tx.ExecContext(ctx, `INSERT INTO spans VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, o, t.TraceID, sp.SpanID, sp.ParentSpanID, sid, sp.Kind, sp.Name, sp.Status, sp.CallKind, sp.ThreadID, sp.Model, sp.StopReason, ns(sp.StartedAt), sp.DurationNS, sp.Seq, j(sp.Input), j(sp.Output), j(sp.Usage), sp.RawTurnID, sp.NodeHash, j(sp.Verdict))
			if e != nil {
				return e
			}
		}
	}
	writeLink := func(link *derive.SpanLink) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO span_links VALUES(?,?,?,?,?,?,?,?,?)`, o, link.FromTraceID, link.FromSpanID, link.FromIO, link.ToTraceID, link.ToSpanID, link.ToIO, link.Kind, sid)
		return err
	}
	for _, turn := range set.Turns {
		if turn.Session.HarnessID != h || turn.Session.HarnessSessionID != s {
			continue
		}
		for _, link := range turn.Links {
			if e = writeLink(link); e != nil {
				return e
			}
		}
	}
	for _, link := range set.Links {
		if e = writeLink(link); e != nil {
			return e
		}
	}
	key := derive.SessionKey{HarnessID: h, HarnessSessionID: s}
	st := set.Status[key]
	model := ""
	for candidate, calls := range models {
		if model == "" || calls > models[model] || (calls == models[model] && candidate < model) {
			model = candidate
		}
	}
	_, e = tx.ExecContext(ctx, `UPDATE sessions SET total_input_tokens=?,total_output_tokens=?,total_cost_micros=?,turn_count=?,derived_status=?,derived_title=?,derived_model=?,model_usage=?,tasks=?,kind_counts=? WHERE id=?`, in, out, cost, turnCount, st.DerivedStatus, derived.SessionTitles[key], model, j(set.ModelUsage[key]), j(set.Tasks[key]), j(set.KindCounts[key]), sid)
	if e != nil {
		return e
	}
	return tx.Commit()
}

// references keep public types visible while this compact local driver grows with the core schema.
var _ = merkle.Node{}
