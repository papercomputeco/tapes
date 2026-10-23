package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// ListSubjects returns one row per distinct auth_subject on the org's
// sessions, most recently active first, with how many sessions carry it.
// q, when non-empty, keeps subjects whose id contains it
// (case-insensitive) as literal text: strpos, not a LIKE pattern, so `%`,
// `_` and `\` in q match only themselves.
func (d *Driver) ListSubjects(ctx context.Context, orgID, q string, limit int) ([]storage.Subject, error) {
	oid, err := orgIDFromString(orgID)
	if err != nil {
		return nil, fmt.Errorf("list subjects: %w", err)
	}
	if limit <= 0 {
		limit = storage.DefaultListLimit
	}
	rows, err := d.conn.Query(ctx, `
SELECT auth_subject, count(*) AS session_count, max(last_seen_at) AS last_seen_at
FROM sessions
WHERE org_id = $1
  AND auth_subject <> ''
  AND ($2 = '' OR strpos(lower(auth_subject), lower($2)) > 0)
GROUP BY auth_subject
ORDER BY max(last_seen_at) DESC, auth_subject
LIMIT $3`, oid, strings.TrimSpace(q), int32(limit))
	if err != nil {
		return nil, fmt.Errorf("list subjects: %w", err)
	}
	defer rows.Close()

	var out []storage.Subject
	for rows.Next() {
		var s storage.Subject
		if err := rows.Scan(&s.Subject, &s.SessionCount, &s.LastSeenAt); err != nil {
			return nil, fmt.Errorf("list subjects: scan: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list subjects: %w", err)
	}
	return out, nil
}
