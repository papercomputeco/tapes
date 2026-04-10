package storage_test

import (
	"strings"
	"testing"
	"time"

	"github.com/papercomputeco/tapes/pkg/storage"
)

func TestCursorRoundTrip(t *testing.T) {
	original := storage.Cursor{
		CreatedAt: time.Date(2026, 4, 1, 12, 34, 56, 123456000, time.UTC),
		Hash:      "abc123",
	}

	encoded := original.Encode()
	if encoded == "" {
		t.Fatal("encoded cursor is empty")
	}

	decoded, err := storage.DecodeCursor(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !decoded.CreatedAt.Equal(original.CreatedAt) {
		t.Errorf("created_at mismatch: got %v want %v", decoded.CreatedAt, original.CreatedAt)
	}
	if decoded.Hash != original.Hash {
		t.Errorf("hash mismatch: got %q want %q", decoded.Hash, original.Hash)
	}
}

func TestDecodeCursorEmpty(t *testing.T) {
	c, err := storage.DecodeCursor("")
	if err != nil {
		t.Fatalf("empty cursor should decode without error, got %v", err)
	}
	if !c.CreatedAt.IsZero() || c.Hash != "" {
		t.Errorf("empty cursor should be zero value, got %+v", c)
	}
}

func TestDecodeCursorInvalid(t *testing.T) {
	cases := []string{
		"not-base64!!!",
		"YWJjZA",          // valid base64 but not JSON
		"e30",             // valid base64 of "{}" — missing hash
		"eyJ0IjoiYWJjIn0", // valid base64 of {"t":"abc"} — bad time
	}
	for _, in := range cases {
		if _, err := storage.DecodeCursor(in); err == nil {
			t.Errorf("expected error for input %q", in)
		}
	}
}

func TestNormalizeLimit(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int
	}{
		{"zero defaults", 0, storage.DefaultListLimit},
		{"negative defaults", -5, storage.DefaultListLimit},
		{"small passes", 10, 10},
		{"max passes", storage.MaxListLimit, storage.MaxListLimit},
		{"over clamps", storage.MaxListLimit + 50, storage.MaxListLimit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := storage.ListOpts{Limit: tc.in}.Normalize().Limit
			if got != tc.want {
				t.Errorf("Normalize(Limit=%d).Limit = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestDecodeCursorInvalidErrorMessage(t *testing.T) {
	_, err := storage.DecodeCursor("!!!")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "invalid cursor") {
		t.Errorf("error message should mention 'invalid cursor', got %q", err.Error())
	}
}
