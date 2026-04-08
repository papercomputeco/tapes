package deck

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEffectiveSinceCutoff(t *testing.T) {
	now := time.Now()
	from := now.Add(-3 * time.Hour)
	older := now.Add(-12 * time.Hour)

	tests := []struct {
		name string
		in   Filters
		// want is checked via a tolerance because Filters.Since is computed
		// against time.Now() inside the helper.
		wantZero bool
		wantNear time.Time
	}{
		{name: "no bounds", in: Filters{}, wantZero: true},
		{name: "since only", in: Filters{Since: 1 * time.Hour}, wantNear: now.Add(-time.Hour)},
		{name: "from only", in: Filters{From: &from}, wantNear: from},
		{name: "since wins when later", in: Filters{Since: 1 * time.Hour, From: &older}, wantNear: now.Add(-time.Hour)},
		{name: "from wins when later", in: Filters{Since: 24 * time.Hour, From: &from}, wantNear: from},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := effectiveSinceCutoff(tc.in)
			if tc.wantZero {
				if !got.IsZero() {
					t.Errorf("got %v, want zero", got)
				}
				return
			}
			delta := got.Sub(tc.wantNear)
			if delta < -time.Second || delta > time.Second {
				t.Errorf("got %v, want within 1s of %v (delta=%v)", got, tc.wantNear, delta)
			}
		})
	}
}

// TestHTTPQueryPushesFiltersDown is the regression test for issue #160. It
// uses an httptest server to capture the query params HTTPQuery sends on
// /v1/sessions/summary, then asserts that since/project/model are present
// when the deck Filters carry them. Without this pushdown the deck would
// page through every session in the store before applying client-side
// filters, which OOMs on large databases.
func TestHTTPQueryPushesFiltersDown(t *testing.T) {
	type captured struct {
		path  string
		since string
		proj  string
		model string
	}
	var seen []captured

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/sessions/summary") {
			http.NotFound(w, r)
			return
		}
		qp := r.URL.Query()
		seen = append(seen, captured{
			path:  r.URL.Path,
			since: qp.Get("since"),
			proj:  qp.Get("project"),
			model: qp.Get("model"),
		})
		// Return an empty page so HTTPQuery stops paginating.
		_ = json.NewEncoder(w).Encode(httpSummaryResponse{})
	}))
	defer srv.Close()

	q := NewHTTPQuery(srv.URL, nil)
	from := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	filters := Filters{
		From:    &from,
		Project: "tapes",
		Model:   "claude-opus-4.6",
	}

	if _, err := q.Overview(context.Background(), filters); err != nil {
		t.Fatalf("Overview returned error: %v", err)
	}

	if len(seen) == 0 {
		t.Fatal("expected at least one request to /v1/sessions/summary")
	}
	got := seen[0]
	if got.proj != "tapes" {
		t.Errorf("project filter not pushed down: got %q want %q", got.proj, "tapes")
	}
	if got.model != "claude-opus-4.6" {
		t.Errorf("model filter not pushed down: got %q want %q", got.model, "claude-opus-4.6")
	}
	if got.since == "" {
		t.Errorf("since filter not pushed down: got empty want RFC3339 timestamp")
	} else if _, err := time.Parse(time.RFC3339, got.since); err != nil {
		t.Errorf("since filter not RFC3339 parseable: %q (%v)", got.since, err)
	}
}

// TestHTTPQueryNoFiltersOmitted verifies that when the deck has no
// pushable filters set, the HTTP request does not include since/project/
// model query params at all (so the API returns the full set per page).
func TestHTTPQueryNoFiltersOmitted(t *testing.T) {
	var seen *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r
		_ = json.NewEncoder(w).Encode(httpSummaryResponse{})
	}))
	defer srv.Close()

	q := NewHTTPQuery(srv.URL, nil)
	if _, err := q.Overview(context.Background(), Filters{}); err != nil {
		t.Fatalf("Overview returned error: %v", err)
	}

	if seen == nil {
		t.Fatal("expected a request to be captured")
	}
	qp := seen.URL.Query()
	for _, key := range []string{"since", "project", "model"} {
		if qp.Has(key) {
			t.Errorf("unexpected %q query param: %q", key, qp.Get(key))
		}
	}
}
