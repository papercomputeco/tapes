package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres/gensqlc"
)

// ListSessionRecords returns a page of sessions for an org ordered by the
// requested sort column (default last_seen_at DESC), optionally windowed by
// activity (last_seen_at) and narrowed to one gateway-stamped JWT subject
// (exact match on the indexed column; empty lists every user's sessions).
// Pass zero-value opts to start from the beginning, unwindowed and unfiltered.
func (d *Driver) ListSessionRecords(
	ctx context.Context,
	orgID string,
	opts storage.SessionListOpts,
) ([]storage.SessionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list session records: %w", err)
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = storage.DefaultListLimit
	}
	sort := opts.Sort
	if sort == "" {
		sort = storage.SortLastActive
	}
	dir := opts.Dir
	if dir == "" {
		dir = storage.SortDesc
	}
	col, ok := storage.SessionSortColumn(sort) // exported accessor over the allowlist map
	if !ok {
		return nil, fmt.Errorf("list session records: invalid sort field %q", sort)
	}

	order := "DESC"
	cmp := "<"
	if dir == storage.SortAsc {
		order = "ASC"
		cmp = ">"
	}

	// Explicit column list = the 24 columns the previous generated query
	// selected, plus the sort column rendered to canonical text so the cursor
	// round-trips exactly. col.Col()/col.Cast() and order/cmp come from the
	// allowlist only.
	//
	// MAINTENANCE: this list and the rows.Scan below are a matched pair and must
	// stay in lockstep with gensqlc.Session (pkg/storage/postgres/gensqlc). A
	// column added to the sessions table + regenerated struct must be appended
	// here AND scanned in the same position, or the scan mispairs. This is the
	// one read path that spells the columns out instead of `SELECT *`, because
	// it also needs `col::text AS sort_val` appended for the cursor.
	const baseCols = `id, org_id, auth_subject, harness_id, harness_session_id, name, cwd, ` +
		`harness_version, parent_session_id, started_at, last_seen_at, ended_at, harness_metadata, ` +
		`total_input_tokens, total_output_tokens, total_cost_usd, turn_count, derived_status, ` +
		`has_git_activity, tool_result_count, tool_error_count, derived_title, derived_model, model_usage`

	args := []any{oid} // $1
	where := []string{"org_id = $1"}
	add := func(v any) string { args = append(args, v); return "$" + strconv.Itoa(len(args)) }

	if opts.Since != nil {
		where = append(where, "last_seen_at >= "+add(*opts.Since)+"::timestamptz")
	}
	if opts.Until != nil {
		where = append(where, "last_seen_at < "+add(*opts.Until)+"::timestamptz")
	}
	if opts.AuthSubject != "" {
		where = append(where, "auth_subject = "+add(opts.AuthSubject)+"::text")
	}
	if opts.CursorVal != nil && opts.CursorID != nil {
		valP := add(*opts.CursorVal) + "::" + col.Cast()
		idP := add(*opts.CursorID) + "::uuid"
		where = append(where, fmt.Sprintf("(%s %s %s OR (%s = %s AND id %s %s))",
			col.Col(), cmp, valP, col.Col(), valP, cmp, idP))
	}
	limP := add(int32(limit)) //nolint:gosec // bounded by the API handler

	// Not an injection surface: col.Col()/col.Cast() are an opaque SortColumn
	// that can only come from the allowlist (SessionSortColumn), order/cmp come
	// from the validated direction, and every caller value is a bound $N param.
	// No raw string reaches an identifier position — the type system, not just
	// convention, guarantees it. gosec does not flag this Sprintf.
	q := fmt.Sprintf(
		"SELECT %s, %s::text AS sort_val FROM sessions WHERE %s ORDER BY %s %s, id %s LIMIT %s",
		baseCols, col.Col(), strings.Join(where, " AND "), col.Col(), order, order, limP)

	rows, err := d.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list session records: %w", err)
	}
	defer rows.Close()

	var out []storage.SessionRecord
	for rows.Next() {
		var g gensqlc.Session
		// pgtype.Text (not a bare string) so a NULL sort_val degrades to an
		// empty SortVal instead of failing the whole scan. Every column in the
		// allowlist is NOT NULL today (see the invariant on sessionSortColumn),
		// so this never actually fires — it is a guard against a future nullable
		// sortable column silently 500ing the entire list mid-page. Note this
		// alone does not make nullable sort columns *work*: the keyset cursor
		// still can't encode a NULL boundary, so adding one needs more than this.
		var sortVal pgtype.Text
		if err := rows.Scan(
			&g.ID, &g.OrgID, &g.AuthSubject, &g.HarnessID, &g.HarnessSessionID, &g.Name, &g.Cwd,
			&g.HarnessVersion, &g.ParentSessionID, &g.StartedAt, &g.LastSeenAt, &g.EndedAt, &g.HarnessMetadata,
			&g.TotalInputTokens, &g.TotalOutputTokens, &g.TotalCostUsd, &g.TurnCount, &g.DerivedStatus,
			&g.HasGitActivity, &g.ToolResultCount, &g.ToolErrorCount, &g.DerivedTitle, &g.DerivedModel, &g.ModelUsage,
			&sortVal,
		); err != nil {
			return nil, fmt.Errorf("list session records: scan: %w", err)
		}
		rec := sessionRecordFromRow(g)
		rec.SortVal = sortVal.String
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list session records: %w", err)
	}
	d.attachPreviews(ctx, out)
	return out, nil
}

const sessionPreviewMaxRunes = 120

// attachPreviews populates Preview on each record in place from a single
// batched preview query. It owns the best-effort policy for session reads:
// previews are decoration, so a fetch failure is logged and the records are
// returned without previews rather than failing the read.
func (d *Driver) attachPreviews(ctx context.Context, records []storage.SessionRecord) {
	previews, err := d.getSessionPreviews(ctx, records)
	if err != nil {
		slog.WarnContext(ctx, "attach session previews", "error", err)
		return
	}
	for i := range records {
		records[i].Preview = previews[records[i].ID]
	}
}

// getSessionPreviews fetches the first user-role node text for each session in
// the supplied list, in a single query. Returns a map of session UUID string →
// truncated plain text preview (harness tags still present; stripping is the
// caller's responsibility).
func (d *Driver) getSessionPreviews(ctx context.Context, sessions []storage.SessionRecord) (map[string]string, error) {
	if len(sessions) == 0 {
		return nil, nil
	}
	ids := make([]string, len(sessions))
	for i, s := range sessions {
		ids[i] = s.ID
	}

	rows, err := d.conn.Query(ctx, `
SELECT DISTINCT ON (session_id) session_id::text, bucket
FROM nodes
WHERE session_id = ANY($1::uuid[])
  AND role = 'user'
ORDER BY session_id, created_at ASC
`, ids)
	if err != nil {
		return nil, fmt.Errorf("get session previews: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string, len(sessions))
	for rows.Next() {
		var sessionID string
		var bucketBytes []byte
		if err := rows.Scan(&sessionID, &bucketBytes); err != nil {
			continue
		}
		var bucket merkle.Bucket
		if err := json.Unmarshal(bucketBytes, &bucket); err != nil {
			continue
		}
		text := strings.TrimSpace(previewText(bucket))
		if utf8.RuneCountInString(text) > sessionPreviewMaxRunes {
			runes := []rune(text)
			text = string(runes[:sessionPreviewMaxRunes])
		}
		out[sessionID] = text
	}
	return out, rows.Err()
}

// previewText renders one user node as a session preview line. Harnesses
// prepend injected context (Claude Code's claudeMd) as <system-reminder>
// text blocks ahead of the human prompt; sessions with no derived title
// fall back to this preview, so those blocks would otherwise become the
// session's display name. Mirrors pkg/derive's promptText preference;
// injected-only nodes keep the full extraction.
func previewText(bucket merkle.Bucket) string {
	var texts []string
	for _, block := range bucket.Content {
		if block.Text == "" || strings.HasPrefix(strings.TrimSpace(block.Text), "<system-reminder>") {
			continue
		}
		texts = append(texts, block.Text)
	}
	if len(texts) == 0 {
		return bucket.ExtractText()
	}
	return strings.Join(texts, "\n")
}

// GetSessionRecord returns a single session by its UUID, or nil if not found.
func (d *Driver) GetSessionRecord(ctx context.Context, orgID, id string) (*storage.SessionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("get session record: %w", err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("get session record: invalid id %q: %w", id, err)
	}
	row, err := d.q.GetSessionRecord(ctx, gensqlc.GetSessionRecordParams{
		OrgID: oid,
		ID:    pgtype.UUID{Bytes: parsed, Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get session record: %w", err)
	}
	s := sessionRecordFromRow(row)
	return &s, nil
}

// DeleteSession removes a session by its org-scoped id and returns whether a
// row was actually deleted (false when the id was absent). The session_id
// ON DELETE CASCADE foreign keys tear down the rest of the subtree in the same
// statement: subagent child sessions (parent_session_id), the session's derived
// nodes, and its spans/span_turns/span_links. A malformed id is treated as a
// no-op delete, matching DeleteSkill.
func (d *Driver) DeleteSession(ctx context.Context, orgID, id string) (bool, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return false, fmt.Errorf("delete session: %w", err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return false, nil //nolint:nilerr // invalid id == nothing to delete
	}
	n, err := d.q.DeleteSession(ctx, gensqlc.DeleteSessionParams{
		OrgID: oid,
		ID:    pgtype.UUID{Bytes: parsed, Valid: true},
	})
	if err != nil {
		return false, fmt.Errorf("delete session: %w", err)
	}
	return n > 0, nil
}

// GetSessionRecordByHarness returns the single session matching the
// org-scoped natural key (org_id, harness_id, harness_session_id), or nil
// if no row matches. The lookup is an exact-match point read on the
// sessions_harness_uq unique index, mirroring the GetSessionRecord
// nil-on-no-rows contract.
func (d *Driver) GetSessionRecordByHarness(
	ctx context.Context,
	orgID string,
	harnessID string,
	harnessSessionID string,
) (*storage.SessionRecord, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("get session record by harness: %w", err)
	}
	row, err := d.q.GetSessionByNaturalKey(ctx, gensqlc.GetSessionByNaturalKeyParams{
		OrgID:            oid,
		HarnessID:        harnessID,
		HarnessSessionID: harnessSessionID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get session record by harness: %w", err)
	}
	recs := []storage.SessionRecord{sessionRecordFromRow(row)}
	d.attachPreviews(ctx, recs)
	return &recs[0], nil
}

// sessionRecordFromRow converts a sqlc-generated Session row to
// the storage-level SessionRecord type.
func sessionRecordFromRow(row gensqlc.Session) storage.SessionRecord {
	s := storage.SessionRecord{
		ID:                uuidToString(row.ID),
		HarnessID:         row.HarnessID,
		HarnessSessionID:  row.HarnessSessionID,
		TotalInputTokens:  row.TotalInputTokens,
		TotalOutputTokens: row.TotalOutputTokens,
		TurnCount:         int(row.TurnCount),
		DerivedStatus:     row.DerivedStatus,
		Model:             row.DerivedModel,
		AuthSubject:       row.AuthSubject,
	}
	// The folded title-gen output is the session's display title; the
	// envelope's internal name (a plan slug for Claude Code) is the
	// fallback. See the derived_title migration.
	if row.DerivedTitle.Valid && row.DerivedTitle.String != "" {
		s.Name = row.DerivedTitle.String
	} else if row.Name.Valid {
		s.Name = row.Name.String
	}
	if row.Cwd.Valid {
		s.Cwd = row.Cwd.String
	}
	if row.HarnessVersion.Valid {
		s.HarnessVersion = row.HarnessVersion.String
	}
	if row.ParentSessionID.Valid {
		s.ParentSessionID = uuidToString(row.ParentSessionID)
	}
	if row.StartedAt.Valid {
		s.StartedAt = row.StartedAt.Time
	}
	if row.LastSeenAt.Valid {
		s.LastSeenAt = row.LastSeenAt.Time
	}
	if row.EndedAt.Valid {
		t := row.EndedAt.Time
		s.EndedAt = &t
	}
	if len(row.HarnessMetadata) > 0 {
		var m map[string]any
		if err := json.Unmarshal(row.HarnessMetadata, &m); err == nil {
			s.HarnessMetadata = m
		}
	}
	if len(row.ModelUsage) > 0 {
		var mu []storage.ModelUsage
		if err := json.Unmarshal(row.ModelUsage, &mu); err == nil {
			s.ModelUsage = mu
		}
	}
	if row.TotalCostUsd.Valid {
		if f, err := row.TotalCostUsd.Float64Value(); err == nil && f.Valid {
			s.TotalCostUsd = f.Float64
		}
	}
	return s
}

// uuidToString converts a pgtype.UUID to its canonical string form.
// Returns empty string for invalid (null) UUIDs.
func uuidToString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	return uuid.UUID(id.Bytes).String()
}
