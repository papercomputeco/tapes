package derive_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// The deriver and ingest's raw-only CreatedAt stamping must resolve
// capture time with the same precedence, or a row's CreatedAt and its
// derived span's StartedAt drift onto different clocks.
func TestCapturedAtPrecedence(t *testing.T) {
	received := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	captured := "2026-07-31T11:00:00.5Z"
	requested := "2026-07-31T10:59:58Z"

	cases := []struct {
		name string
		meta map[string]string
		want time.Time
	}{
		{
			name: "captured_at wins over ts_request",
			meta: map[string]string{"captured_at": captured, "ts_request": requested},
			want: time.Date(2026, 7, 31, 11, 0, 0, 500_000_000, time.UTC),
		},
		{
			name: "ts_request stands alone",
			meta: map[string]string{"ts_request": requested},
			want: time.Date(2026, 7, 31, 10, 59, 58, 0, time.UTC),
		},
		{
			name: "malformed captured_at falls through to ts_request",
			meta: map[string]string{"captured_at": "yesterday-ish", "ts_request": requested},
			want: time.Date(2026, 7, 31, 10, 59, 58, 0, time.UTC),
		},
		{
			name: "no capture-side stamp falls back to received_at",
			meta: nil,
			want: received,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &storage.RawTurnRecord{ReceivedAt: received}
			if tc.meta != nil {
				raw, err := json.Marshal(tc.meta)
				if err != nil {
					t.Fatalf("marshal meta: %v", err)
				}
				rec.Meta = raw
			}
			if got := derive.CapturedAt(rec); !got.Equal(tc.want) {
				t.Fatalf("CapturedAt = %v, want %v", got, tc.want)
			}
		})
	}
}
