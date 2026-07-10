package storage

import (
	"regexp"
	"testing"
)

// The sort allowlist is interpolated into an ORDER BY clause and a ::cast, so
// its contents — not just the SortColumn type — must be auditable. This ranges
// every entry (present and future) and fails if any column is not a bare
// identifier or any cast type is outside the known-safe set, so a stray space,
// quote, semicolon, or paren trips here rather than reaching SQL.
func TestSessionSortColumnsAreSQLSafe(t *testing.T) {
	ident := regexp.MustCompile(`^[a-z_]+$`)
	safeCasts := map[string]bool{
		"bigint": true, "numeric": true, "timestamptz": true, "text": true,
	}

	for field, col := range sessionSortColumn {
		if !ident.MatchString(col.col) {
			t.Errorf("sort field %q: column %q is not a bare identifier", field, col.col)
		}
		if !safeCasts[col.cast] {
			t.Errorf("sort field %q: cast %q is not in the safe set", field, col.cast)
		}
	}
}

// A sort key outside the allowlist must never resolve to a SortColumn — the
// injection guard the handler relies on to reject unknown ?sort= values.
func TestSessionSortColumnRejectsNonAllowlisted(t *testing.T) {
	if _, ok := SessionSortColumn(SessionSortField("total_cost_usd; DROP TABLE sessions")); ok {
		t.Fatal("a non-allowlisted sort field resolved to a column")
	}
}
