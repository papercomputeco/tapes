package sqlitecore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/storage"
)

func scanSession(s interface{ Scan(...any) error }) (*storage.SessionRecord, error) {
	var r storage.SessionRecord
	var start, last int64
	var parent sql.NullString
	var metadata, model, tasks, kinds []byte
	var cost int64
	e := s.Scan(&r.ID, &r.HarnessID, &r.HarnessSessionID, &r.Name, &r.DisplayName, &r.DerivedTitle, &r.Cwd, &r.HarnessVersion, &parent, &start, &last, &r.AuthSubject, &metadata, &r.DerivedStatus, &r.Model, &model, &tasks, &kinds, &r.TotalInputTokens, &r.TotalOutputTokens, &cost, &r.TurnCount)
	if e != nil {
		return nil, e
	}
	r.StartedAt = tm(start)
	r.LastSeenAt = tm(last)
	r.TotalCostUsd = float64(cost) / 1e6
	if parent.Valid {
		r.ParentSessionID = parent.String
	}
	_ = json.Unmarshal(metadata, &r.HarnessMetadata)
	r.ModelUsage = decodeModels(model)
	r.Tasks = tasks
	r.KindCounts = kinds
	return &r, nil
}

const sessionCols = `id,harness_id,harness_session_id,name,display_name,derived_title,cwd,harness_version,parent_session_id,started_at,last_seen_at,auth_subject,harness_metadata,derived_status,derived_model,model_usage,tasks,kind_counts,total_input_tokens,total_output_tokens,total_cost_micros,turn_count`

func decodeModels(b []byte) []storage.ModelUsage {
	var v []storage.ModelUsage
	_ = json.Unmarshal(b, &v)
	return v
}

// attachPreview mirrors the Postgres read path's derive-owned first prompt
// decoration. It is deliberately best-effort: an absent projection is not a
// failed session read.
func attachPreview(ctx context.Context, db *sql.DB, r *storage.SessionRecord) {
	var preview string
	err := db.QueryRowContext(ctx, `SELECT user_prompt FROM span_turns
		WHERE session_id=? ORDER BY (synthetic='' AND trim(user_prompt)<>'') DESC, started_at ASC LIMIT 1`, r.ID).Scan(&preview)
	if err == nil {
		r.Preview = preview
	}
}

func (d *Driver) GetSessionRecord(ctx context.Context, o, id string) (*storage.SessionRecord, error) {
	r, e := scanSession(d.db.QueryRowContext(ctx, `SELECT `+sessionCols+` FROM sessions WHERE org_id=? AND id=?`, org(o), id))
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e == nil {
		attachPreview(ctx, d.db, r)
	}
	return r, e
}

func (d *Driver) GetSessionRecordByHarness(ctx context.Context, o, h, s string) (*storage.SessionRecord, error) {
	r, e := scanSession(d.db.QueryRowContext(ctx, `SELECT `+sessionCols+` FROM sessions WHERE org_id=? AND harness_id=? AND harness_session_id=?`, org(o), h, s))
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	if e == nil {
		attachPreview(ctx, d.db, r)
	}
	return r, e
}

func (d *Driver) ListSessionRecords(ctx context.Context, o string, opts storage.SessionListOpts) ([]storage.SessionRecord, error) {
	q := `SELECT ` + sessionCols + ` FROM sessions WHERE org_id=?`
	args := []any{org(o)}
	if opts.Since != nil {
		q += ` AND last_seen_at>=?`
		args = append(args, ns(*opts.Since))
	}
	if opts.Until != nil {
		q += ` AND last_seen_at<?`
		args = append(args, ns(*opts.Until))
	}
	if opts.AuthSubject != "" {
		q += ` AND auth_subject=?`
		args = append(args, opts.AuthSubject)
	}
	// Local data sets are intentionally small. Sort in Go rather than duplicating the Postgres keyset SQL matrix.
	rows, e := d.db.QueryContext(ctx, q, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []storage.SessionRecord
	for rows.Next() {
		r, e := scanSession(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, *r)
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	field := opts.Sort
	if field == "" {
		field = storage.SortLastActive
	}
	desc := opts.Dir != storage.SortAsc
	key := func(r storage.SessionRecord) string {
		switch field {
		case storage.SortStartedAt:
			return fmt.Sprintf("%020d", ns(r.StartedAt))
		case storage.SortTurnCount:
			return fmt.Sprintf("%020d", r.TurnCount)
		case storage.SortTotalCost:
			return fmt.Sprintf("%020d", int64(r.TotalCostUsd*1e6))
		case storage.SortTotalTokens:
			return fmt.Sprintf("%020d", r.TotalInputTokens+r.TotalOutputTokens)
		case storage.SortDerivedStatus:
			return r.DerivedStatus
		case storage.SortAuthSubject:
			return r.AuthSubject
		case storage.SortDurationNs:
			return fmt.Sprintf("%020d", r.LastSeenAt.Sub(r.StartedAt).Nanoseconds())
		case storage.SortLastActive:
			return fmt.Sprintf("%020d", ns(r.LastSeenAt))
		default:
			return fmt.Sprintf("%020d", ns(r.LastSeenAt))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := key(out[i]), key(out[j])
		if a == b {
			return out[i].ID > out[j].ID == desc
		}
		if desc {
			return a > b
		}
		return a < b
	})
	filtered := out[:0]
	for _, r := range out {
		v := key(r)
		if opts.CursorVal != nil {
			var after bool
			if desc {
				after = v < *opts.CursorVal || (v == *opts.CursorVal && r.ID < *opts.CursorID)
			} else {
				after = v > *opts.CursorVal || (v == *opts.CursorVal && r.ID > *opts.CursorID)
			}
			if !after {
				continue
			}
		}
		r.SortVal = v
		attachPreview(ctx, d.db, &r)
		filtered = append(filtered, r)
		if opts.Limit > 0 && len(filtered) >= opts.Limit {
			break
		}
	}
	return filtered, nil
}

func (d *Driver) UpdateSessionDisplayName(ctx context.Context, o, id string, name *string) (int64, error) {
	d.write.Lock()
	defer d.write.Unlock()
	v := ""
	if name != nil {
		v = strings.TrimSpace(*name)
	}
	r, e := d.db.ExecContext(ctx, `UPDATE sessions SET display_name=? WHERE org_id=? AND id=?`, v, org(o), id)
	if e != nil {
		return 0, e
	}
	return r.RowsAffected()
}

func (d *Driver) DeleteSession(ctx context.Context, o, id string) (bool, error) {
	d.write.Lock()
	defer d.write.Unlock()
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `WITH RECURSIVE tree(id) AS (
		SELECT id FROM sessions WHERE org_id=? AND id=?
		UNION SELECT s.id FROM sessions s JOIN tree t ON s.parent_session_id=t.id WHERE s.org_id=?
	) SELECT harness_id,harness_session_id FROM sessions WHERE org_id=? AND id IN (SELECT id FROM tree)`, org(o), id, org(o), org(o))
	if err != nil {
		return false, err
	}
	var keys [][2]string
	for rows.Next() {
		var key [2]string
		if err = rows.Scan(&key[0], &key[1]); err != nil {
			_ = rows.Close()
			return false, err
		}
		keys = append(keys, key)
	}
	if err = rows.Close(); err != nil {
		return false, err
	}
	if len(keys) == 0 {
		return false, nil
	}
	if _, err = tx.ExecContext(ctx, `WITH RECURSIVE tree(id) AS (
		SELECT id FROM sessions WHERE org_id=? AND id=?
		UNION SELECT s.id FROM sessions s JOIN tree t ON s.parent_session_id=t.id WHERE s.org_id=?
	) DELETE FROM sessions WHERE org_id=? AND id IN (SELECT id FROM tree)`, org(o), id, org(o), org(o)); err != nil {
		return false, err
	}
	now := ns(time.Now())
	for _, key := range keys {
		if _, err = tx.ExecContext(ctx, `INSERT INTO deleted_sessions VALUES(?,?,?,?) ON CONFLICT(org_id,harness_id,harness_session_id) DO UPDATE SET deleted_at=excluded.deleted_at`, org(o), key[0], key[1], now); err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM derive_queue WHERE org_id=? AND harness_id=? AND harness_session_id=?`, org(o), key[0], key[1]); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}

func scanTurn(s interface{ Scan(...any) error }) (storage.SpanTurnRecord, error) {
	var r storage.SpanTurnRecord
	var start int64
	var end sql.NullInt64
	var cost int64
	e := s.Scan(&r.TraceID, &r.SessionID, &r.UserPrompt, &r.ResponsePreview, &r.Synthetic, &r.Status, &r.Source, &start, &end, &r.DurationNS, &r.TotalInputTokens, &r.TotalOutputTokens, &r.MainInputTokens, &r.MainOutputTokens, &r.CacheReadTokens, &r.CacheCreationTokens, &cost)
	r.StartedAt = tm(start)
	if end.Valid {
		x := tm(end.Int64)
		r.EndedAt = &x
	}
	r.TotalCostUSD = float64(cost) / 1e6
	return r, e
}

const turnCols = `trace_id,session_id,user_prompt,response_preview,synthetic,status,source,started_at,ended_at,duration_ns,total_input_tokens,total_output_tokens,main_input_tokens,main_output_tokens,cache_read_tokens,cache_creation_tokens,total_cost_micros`

func scanSpan(s interface{ Scan(...any) error }) (storage.SpanRecord, error) {
	var r storage.SpanRecord
	var start int64
	e := s.Scan(&r.TraceID, &r.SpanID, &r.ParentSpanID, &r.Kind, &r.Name, &r.Status, &r.CallKind, &r.ThreadID, &r.Model, &r.StopReason, &start, &r.DurationNS, &r.Seq, &r.Input, &r.Output, &r.Usage, &r.RawTurnID, &r.NodeHash, &r.Verdict)
	r.StartedAt = tm(start)
	return r, e
}

const spanCols = `trace_id,span_id,parent_span_id,kind,name,status,call_kind,thread_id,model,stop_reason,started_at,duration_ns,seq,input,output,usage,raw_turn_id,node_hash,verdict`

func (d *Driver) ListSessionSpanModel(ctx context.Context, id string) ([]storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	ts, e := d.ListTraceSummaries(ctx, id)
	if e != nil {
		return nil, nil, nil, e
	}
	turns := make([]storage.SpanTurnRecord, len(ts))
	for i, t := range ts {
		turns[i] = t.SpanTurnRecord
	}
	rows, e := d.db.QueryContext(ctx, `SELECT `+spanCols+` FROM spans WHERE session_id=? ORDER BY trace_id,seq`, id)
	if e != nil {
		return nil, nil, nil, e
	}
	defer rows.Close()
	var spans []storage.SpanRecord
	for rows.Next() {
		r, err := scanSpan(rows)
		if err != nil {
			return nil, nil, nil, err
		}
		spans = append(spans, r)
	}
	if e = rows.Err(); e != nil {
		return nil, nil, nil, e
	}
	links, e := d.ListSessionLinks(ctx, id)
	return turns, spans, links, e
}

func (d *Driver) ListTraceSummaries(ctx context.Context, id string) ([]storage.TraceSummaryRecord, error) {
	rows, e := d.db.QueryContext(ctx, `SELECT `+turnCols+`,(SELECT count(*) FROM spans p WHERE p.org_id=t.org_id AND p.trace_id=t.trace_id) FROM span_turns t WHERE session_id=? ORDER BY started_at,trace_id`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []storage.TraceSummaryRecord
	for rows.Next() {
		var r storage.SpanTurnRecord
		var start int64
		var end sql.NullInt64
		var cost int64
		var n int
		e = rows.Scan(&r.TraceID, &r.SessionID, &r.UserPrompt, &r.ResponsePreview, &r.Synthetic, &r.Status, &r.Source, &start, &end, &r.DurationNS, &r.TotalInputTokens, &r.TotalOutputTokens, &r.MainInputTokens, &r.MainOutputTokens, &r.CacheReadTokens, &r.CacheCreationTokens, &cost, &n)
		if e != nil {
			return nil, e
		}
		r.StartedAt = tm(start)
		if end.Valid {
			x := tm(end.Int64)
			r.EndedAt = &x
		}
		r.TotalCostUSD = float64(cost) / 1e6
		out = append(out, storage.TraceSummaryRecord{SpanTurnRecord: r, SpanCount: n})
	}
	return out, rows.Err()
}

func (d *Driver) ListTraceSpans(ctx context.Context, o, trace string) ([]storage.SpanRecord, error) {
	rows, e := d.db.QueryContext(ctx, `SELECT `+spanCols+` FROM spans WHERE org_id=? AND trace_id=? ORDER BY seq`, org(o), trace)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []storage.SpanRecord
	for rows.Next() {
		r, e := scanSpan(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (d *Driver) ListSessionLinks(ctx context.Context, id string) ([]storage.SpanLinkRecord, error) {
	rows, e := d.db.QueryContext(ctx, `SELECT from_trace_id,from_span_id,from_io,to_trace_id,to_span_id,to_io,kind FROM span_links WHERE session_id=? ORDER BY from_trace_id,from_span_id`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []storage.SpanLinkRecord
	for rows.Next() {
		var x storage.SpanLinkRecord
		if e = rows.Scan(&x.FromTraceID, &x.FromSpanID, &x.FromIO, &x.ToTraceID, &x.ToSpanID, &x.ToIO, &x.Kind); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (d *Driver) GetTraceDetail(ctx context.Context, o, trace string) (*storage.SpanTurnRecord, []storage.SpanRecord, []storage.SpanLinkRecord, error) {
	r, e := scanTurn(d.db.QueryRowContext(ctx, `SELECT `+turnCols+` FROM span_turns WHERE org_id=? AND trace_id=?`, org(o), trace))
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil, nil, nil
	}
	if e != nil {
		return nil, nil, nil, e
	}
	sp, e := d.ListTraceSpans(ctx, o, trace)
	if e != nil {
		return nil, nil, nil, e
	}
	rows, e := d.db.QueryContext(ctx, `SELECT from_trace_id,from_span_id,from_io,to_trace_id,to_span_id,to_io,kind FROM span_links WHERE org_id=? AND from_trace_id=?`, org(o), trace)
	if e != nil {
		return nil, nil, nil, e
	}
	defer rows.Close()
	var ls []storage.SpanLinkRecord
	for rows.Next() {
		var x storage.SpanLinkRecord
		if e = rows.Scan(&x.FromTraceID, &x.FromSpanID, &x.FromIO, &x.ToTraceID, &x.ToSpanID, &x.ToIO, &x.Kind); e != nil {
			return nil, nil, nil, e
		}
		ls = append(ls, x)
	}
	return &r, sp, ls, rows.Err()
}

func (d *Driver) GetSpanRecord(ctx context.Context, o, trace, span string) (*storage.SpanRecord, error) {
	r, e := scanSpan(d.db.QueryRowContext(ctx, `SELECT `+spanCols+` FROM spans WHERE org_id=? AND trace_id=? AND span_id=?`, org(o), trace, span))
	if errors.Is(e, sql.ErrNoRows) {
		return nil, nil
	}
	return &r, e
}

func (d *Driver) ListRawTurnHeaders(ctx context.Context, o, h, s string) ([]storage.RawTurnHeader, error) {
	rows, e := d.db.QueryContext(ctx, `SELECT id,source,provider,agent_name,request_id,received_at,meta,length(raw_request),length(response) FROM raw_turns WHERE org_id=? AND harness_id=? AND harness_session_id=? ORDER BY id`, org(o), h, s)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []storage.RawTurnHeader
	for rows.Next() {
		var r storage.RawTurnHeader
		var n int64
		if e = rows.Scan(&r.ID, &r.Source, &r.Provider, &r.AgentName, &r.RequestID, &n, &r.Meta, &r.RequestBytes, &r.ResponseBytes); e != nil {
			return nil, e
		}
		r.ReceivedAt = tm(n)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (d *Driver) AggregateSpanStats(ctx context.Context, o string, since, until *time.Time) (storage.SpanStats, error) {
	where := `t.org_id=?`
	args := []any{org(o)}
	if since != nil {
		where += ` AND t.started_at>=?`
		args = append(args, ns(*since))
	}
	if until != nil {
		where += ` AND t.started_at<?`
		args = append(args, ns(*until))
	}
	q := `SELECT count(*), count(DISTINCT t.session_id),
		count(DISTINCT CASE WHEN s.derived_status='completed' THEN t.session_id END),
		coalesce(sum(t.total_input_tokens),0), coalesce(sum(t.total_output_tokens),0),
		coalesce(sum(t.cache_creation_tokens),0), coalesce(sum(t.cache_read_tokens),0),
		coalesce(sum(t.duration_ns),0), coalesce(sum(t.total_cost_micros),0)
		FROM span_turns t LEFT JOIN sessions s ON s.id=t.session_id WHERE ` + where
	var x storage.SpanStats
	var cost int64
	if err := d.db.QueryRowContext(ctx, q, args...).Scan(
		&x.TurnCount, &x.SessionCount, &x.CompletedCount, &x.InputTokens,
		&x.OutputTokens, &x.CacheCreationTokens, &x.CacheReadTokens,
		&x.TotalDurationNS, &cost,
	); err != nil {
		return storage.SpanStats{}, err
	}

	toolWhere := `org_id=? AND kind='tool'`
	toolArgs := []any{org(o)}
	if since != nil {
		toolWhere += ` AND started_at>=?`
		toolArgs = append(toolArgs, ns(*since))
	}
	if until != nil {
		toolWhere += ` AND started_at<?`
		toolArgs = append(toolArgs, ns(*until))
	}
	if err := d.db.QueryRowContext(ctx, `SELECT count(*) FROM spans WHERE `+toolWhere, toolArgs...).Scan(&x.ToolCalls); err != nil {
		return storage.SpanStats{}, err
	}
	x.TotalCostUSD = float64(cost) / 1e6
	return x, nil
}
