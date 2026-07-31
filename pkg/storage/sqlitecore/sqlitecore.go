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
	"sort"
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

const schemaVersion = 1

func (d *Driver) Open(ctx context.Context) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version > schemaVersion {
		return fmt.Errorf("local SQLite schema version %d is newer than this binary supports", version)
	}
	if version < schemaVersion {
		if _, err = tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

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
	if r.Session == nil || r.Session.HarnessSessionID == "" {
		if parent := r.Nodes[0].ParentHash; parent != nil && *parent != "" {
			return storage.IngestTurnResult{}, fmt.Errorf("nodes[0] must be the conversation root when no harness_session_id is supplied, got ParentHash=%q", *parent)
		}
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
	rows, err := d.db.QueryContext(ctx, `SELECT id,org_id,source,provider,agent_name,harness_id,harness_session_id,request_id,raw_request,response,raw_response,raw_response_encoding,meta,session_envelope,received_at FROM raw_turns WHERE org_id=? AND harness_id=? AND harness_session_id=? ORDER BY id`, org(o), h, s)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var wire, transcripts []storage.RawTurnRecord
	for rows.Next() {
		var record storage.RawTurnRecord
		var request, response, rawResponse, metadata, envelope []byte
		var receivedAt int64
		if err = rows.Scan(&record.ID, &record.OrgID, &record.Source, &record.Provider, &record.AgentName, &record.HarnessID, &record.HarnessSessionID, &record.RequestID, &request, &response, &rawResponse, &record.RawResponseEncoding, &metadata, &envelope, &receivedAt); err != nil {
			return nil, err
		}
		record.RawRequest, record.Response, record.RawResponse, record.Meta, record.SessionEnvelope = request, response, rawResponse, metadata, envelope
		record.ReceivedAt = tm(receivedAt)
		if record.Source == storage.RawTurnSourceTranscript {
			transcripts = append(transcripts, record)
			continue
		}
		wire = append(wire, record)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	// The deriver requires capture order. A transcript is a causal sidecar,
	// not a provider request/response, and is reconciled only after wire turns
	// have been derived.
	sort.SliceStable(wire, func(i, j int) bool { return derive.CapturedAt(&wire[i]).Before(derive.CapturedAt(&wire[j])) })
	dv, err := derive.NewDeriver(project)
	if err != nil {
		return nil, err
	}
	for i := range wire {
		recoverReduction(ctx, &wire[i])
		dv.AddTurn(&wire[i])
	}
	set := dv.Finish()
	key := derive.SessionKey{HarnessID: h, HarnessSessionID: s}
	set.Sessions = append(set.Sessions, key)

	// A transcript can be re-uploaded as it grows. Keep the newest version of
	// each agent/lifecycle file before reconciling it with the wire projection.
	// A legacy interacted record has no lifecycle marker in metadata, so inspect
	// its content before letting it shadow that agent's spawn anchor.
	groups := map[transcriptGroup][]storage.RawTurnRecord{}
	for _, record := range transcripts {
		group := transcriptGroupOf(record.Meta)
		groups[group] = append(groups[group], record)
	}
	selected := make([]storage.RawTurnRecord, 0, len(groups))
	for group, records := range groups {
		sort.SliceStable(records, func(i, j int) bool { return records[i].ID > records[j].ID })
		if group.kind != "" {
			selected = append(selected, records[0])
			continue
		}
		keptNonSpawn := false
		for _, record := range records {
			file, parseErr := derive.ParseTranscriptFile(&record)
			if parseErr != nil {
				return nil, fmt.Errorf("parse transcript row %d: %w", record.ID, parseErr)
			}
			if file.SpawnEvidence() {
				selected = append(selected, record)
				break
			}
			if !keptNonSpawn {
				selected = append(selected, record)
				keptNonSpawn = true
			}
		}
	}
	sort.SliceStable(selected, func(i, j int) bool { return selected[i].ID < selected[j].ID })
	files := make([]*derive.TranscriptFile, 0, len(selected))
	for i := range selected {
		file, parseErr := derive.ParseTranscriptFile(&selected[i])
		if parseErr != nil {
			return nil, fmt.Errorf("parse transcript row %d: %w", selected[i].ID, parseErr)
		}
		files = append(files, file)
	}
	set.Report.Reconcile = derive.ReconcileTranscripts(set, files)

	if err = d.writeSpans(ctx, org(o), h, s, set, derive.EmitSpans(set)); err != nil {
		return nil, err
	}
	return &set.Report, nil
}

type transcriptGroup struct{ agent, kind string }

func transcriptGroupOf(meta []byte) transcriptGroup {
	var value struct {
		AgentID string `json:"agent_id"`
		Kind    string `json:"kind"`
	}
	_ = json.Unmarshal(meta, &value)
	if value.AgentID == "" {
		value.AgentID = "main"
	}
	if value.Kind == "started" {
		value.Kind = ""
	}
	return transcriptGroup{agent: value.AgentID, kind: value.Kind}
}

func j(v any) ([]byte, error) { return json.Marshal(v) }

func (d *Driver) writeSpans(ctx context.Context, o, h, s string, derived *derive.DerivedSet, set *derive.SpanSet) error {
	d.write.Lock()
	defer d.write.Unlock()
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var sessionID string
	if err = tx.QueryRowContext(ctx, `SELECT id FROM sessions WHERE org_id=? AND harness_id=? AND harness_session_id=?`, o, h, s).Scan(&sessionID); errors.Is(err, sql.ErrNoRows) {
		// Raw capture can win a crash race with session ingest, and a deleted
		// session intentionally leaves its immutable raw layer behind. Neither
		// has a projection to write, so converge by clearing the queue entry.
		return nil
	} else if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM span_links WHERE session_id=?`, sessionID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM span_turns WHERE session_id=?`, sessionID); err != nil {
		return err
	}

	var inputTokens, outputTokens, cost int64
	turnCount := 0
	models := map[string]int{}
	writtenTraces := map[string]struct{}{}
	for _, turn := range set.Turns {
		if turn.Session.HarnessID != h || turn.Session.HarnessSessionID != s {
			continue
		}
		var endedAt any
		duration := int64(0)
		if !turn.EndedAt.IsZero() {
			endedAt = ns(turn.EndedAt)
			duration = turn.EndedAt.Sub(turn.StartedAt).Nanoseconds()
		}
		turnCost := int64(turn.TotalCostUSD * 1e6)
		_, err = tx.ExecContext(ctx, `INSERT INTO span_turns VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(org_id,trace_id) DO UPDATE SET session_id=excluded.session_id,user_prompt=excluded.user_prompt,response_preview=excluded.response_preview,synthetic=excluded.synthetic,status=excluded.status,source=excluded.source,started_at=excluded.started_at,ended_at=excluded.ended_at,duration_ns=excluded.duration_ns,total_input_tokens=excluded.total_input_tokens,total_output_tokens=excluded.total_output_tokens,main_input_tokens=excluded.main_input_tokens,main_output_tokens=excluded.main_output_tokens,cache_read_tokens=excluded.cache_read_tokens,cache_creation_tokens=excluded.cache_creation_tokens,total_cost_micros=excluded.total_cost_micros`, o, turn.TraceID, sessionID, turn.UserPrompt, turn.ResponsePreview, turn.Synthetic, "ok", turn.Source, ns(turn.StartedAt), endedAt, duration, turn.TotalInputTokens, turn.TotalOutputTokens, turn.MainInputTokens, turn.MainOutputTokens, turn.CacheReadTokens, turn.CacheCreationTokens, turnCost)
		if err != nil {
			return err
		}
		writtenTraces[turn.TraceID] = struct{}{}
		turnCount++
		inputTokens += turn.TotalInputTokens
		outputTokens += turn.TotalOutputTokens
		cost += turnCost
		for _, span := range turn.Spans {
			input, marshalErr := j(span.Input)
			if marshalErr != nil {
				return fmt.Errorf("marshal span %s input: %w", span.SpanID, marshalErr)
			}
			output, marshalErr := j(span.Output)
			if marshalErr != nil {
				return fmt.Errorf("marshal span %s output: %w", span.SpanID, marshalErr)
			}
			usage, marshalErr := j(span.Usage)
			if marshalErr != nil {
				return fmt.Errorf("marshal span %s usage: %w", span.SpanID, marshalErr)
			}
			verdict, marshalErr := j(span.Verdict)
			if marshalErr != nil {
				return fmt.Errorf("marshal span %s verdict: %w", span.SpanID, marshalErr)
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO spans VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(org_id,trace_id,span_id) DO UPDATE SET parent_span_id=excluded.parent_span_id,session_id=excluded.session_id,kind=excluded.kind,name=excluded.name,status=excluded.status,call_kind=excluded.call_kind,thread_id=excluded.thread_id,model=excluded.model,stop_reason=excluded.stop_reason,started_at=excluded.started_at,duration_ns=excluded.duration_ns,seq=excluded.seq,input=excluded.input,output=excluded.output,usage=excluded.usage,raw_turn_id=excluded.raw_turn_id,node_hash=excluded.node_hash,verdict=excluded.verdict`, o, turn.TraceID, span.SpanID, span.ParentSpanID, sessionID, span.Kind, span.Name, span.Status, span.CallKind, span.ThreadID, span.Model, span.StopReason, ns(span.StartedAt), span.DurationNS, span.Seq, input, output, usage, span.RawTurnID, span.NodeHash, verdict)
			if err != nil {
				return err
			}
			if span.Kind == "llm" && span.CallKind == derive.KindMain && span.ThreadID == "" && span.Model != "" {
				models[span.Model]++
			}
		}
	}
	writeLink := func(link *derive.SpanLink) error {
		if _, ok := writtenTraces[link.FromTraceID]; !ok {
			return nil
		}
		_, linkErr := tx.ExecContext(ctx, `INSERT INTO span_links VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(org_id,from_trace_id,from_span_id,to_trace_id,to_span_id,from_io,to_io) DO UPDATE SET kind=excluded.kind,session_id=excluded.session_id`, o, link.FromTraceID, link.FromSpanID, link.FromIO, link.ToTraceID, link.ToSpanID, link.ToIO, link.Kind, sessionID)
		return linkErr
	}
	for _, turn := range set.Turns {
		if _, ok := writtenTraces[turn.TraceID]; ok {
			for _, link := range turn.Links {
				if err = writeLink(link); err != nil {
					return err
				}
			}
		}
	}
	for _, link := range set.Links {
		if err = writeLink(link); err != nil {
			return err
		}
	}

	key := derive.SessionKey{HarnessID: h, HarnessSessionID: s}
	status := "unknown"
	if current, ok := set.Status[key]; ok && current.DerivedStatus != "" {
		status = current.DerivedStatus
	}
	model := ""
	for candidate, calls := range models {
		if model == "" || calls > models[model] || (calls == models[model] && candidate < model) {
			model = candidate
		}
	}
	modelUsage, err := j(set.ModelUsage[key])
	if err != nil {
		return fmt.Errorf("marshal model usage: %w", err)
	}
	tasks, err := j(set.Tasks[key])
	if err != nil {
		return fmt.Errorf("marshal tasks: %w", err)
	}
	kindCounts, err := j(set.KindCounts[key])
	if err != nil {
		return fmt.Errorf("marshal kind counts: %w", err)
	}
	_, err = tx.ExecContext(ctx, `UPDATE sessions SET total_input_tokens=?,total_output_tokens=?,total_cost_micros=?,turn_count=?,derived_status=?,derived_title=?,derived_model=?,model_usage=?,tasks=?,kind_counts=? WHERE id=?`, inputTokens, outputTokens, cost, turnCount, status, derived.SessionTitles[key], model, modelUsage, tasks, kindCounts, sessionID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// references keep public types visible while this compact local driver grows with the core schema.
var _ = merkle.Node{}
