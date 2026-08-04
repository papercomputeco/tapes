package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeBundle stages one capture directory under root so the test can
// exercise the full loadBundles → Server.replay path. Mirrors what
// paperd writes at runtime; if either side's format drifts we'll see
// it in test failures.
func writeBundle(t *testing.T, root, name string, meta map[string]any, body string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	mb, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), mb, 0o644); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "response.sse"), []byte(body), 0o644); err != nil {
		t.Fatalf("write response.sse: %v", err)
	}
}

func TestLoadBundlesAndServe(t *testing.T) {
	root := t.TempDir()
	writeBundle(t, root, "turn-001", map[string]any{
		"status":           200,
		"content_type":     "text/event-stream",
		"response_bytes":   42,
		"duration_ms":      uint64(150),
		"finalized_by":     "stream_complete",
		"response_headers": [][]string{},
	}, "event: message_start\ndata: {\"id\":\"msg_X\"}\n\n")

	writeBundle(t, root, "turn-002", map[string]any{
		"status":           500,
		"content_type":     "application/json",
		"response_bytes":   58,
		"duration_ms":      uint64(20),
		"finalized_by":     "error_response",
		"response_headers": [][]string{},
	}, `{"type":"error","error":{"type":"overloaded_error"}}`)

	bundles, err := loadBundles(root)
	if err != nil {
		t.Fatalf("loadBundles: %v", err)
	}
	if got, want := len(bundles), 2; got != want {
		t.Fatalf("bundle count: got %d, want %d", got, want)
	}
	if bundles[0].Name != "turn-001" || bundles[1].Name != "turn-002" {
		t.Fatalf("bundle order: got %q,%q (loadBundles must sort by name for deterministic round-robin)",
			bundles[0].Name, bundles[1].Name)
	}

	srv := &Server{bundles: bundles}

	// Round-robin: first call → turn-001, second → turn-002, third wraps.
	for i, want := range []string{"turn-001", "turn-002", "turn-001"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		rec := httptest.NewRecorder()
		srv.replay(rec, req)
		if got := rec.Header().Get("X-Replay-Turn"); got != want {
			t.Errorf("round-robin call %d: got turn %q, want %q", i, got, want)
		}
	}
}

func TestPickByHeader(t *testing.T) {
	root := t.TempDir()
	writeBundle(t, root, "turn-aaa", map[string]any{"status": 200}, "first")
	writeBundle(t, root, "turn-bbb", map[string]any{"status": 200}, "second")
	bundles, err := loadBundles(root)
	if err != nil {
		t.Fatalf("loadBundles: %v", err)
	}
	srv := &Server{bundles: bundles}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("X-Replay-Turn", "turn-bbb")
	rec := httptest.NewRecorder()
	srv.replay(rec, req)
	body, _ := io.ReadAll(rec.Body)
	if string(body) != "second" {
		t.Errorf("X-Replay-Turn selection: got body %q, want %q", body, "second")
	}
	if rec.Header().Get("X-Replay-Turn") != "turn-bbb" {
		t.Errorf("response should echo turn name in X-Replay-Turn")
	}
}

func TestPickByQuery(t *testing.T) {
	root := t.TempDir()
	writeBundle(t, root, "turn-aaa", map[string]any{"status": 200}, "first")
	writeBundle(t, root, "turn-bbb", map[string]any{"status": 200}, "second")
	bundles, err := loadBundles(root)
	if err != nil {
		t.Fatalf("loadBundles: %v", err)
	}
	srv := &Server{bundles: bundles}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages?turn=turn-aaa", nil)
	rec := httptest.NewRecorder()
	srv.replay(rec, req)
	body, _ := io.ReadAll(rec.Body)
	if string(body) != "first" {
		t.Errorf("?turn= selection: got body %q, want %q", body, "first")
	}
}

func TestUnknownTurnReturns503(t *testing.T) {
	srv := &Server{bundles: []*Bundle{
		{Name: "turn-only", Meta: MetaRecord{Status: 200}, Response: []byte("only")},
	}}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("X-Replay-Turn", "missing")
	rec := httptest.NewRecorder()
	srv.replay(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("unknown turn should 503, got %d", rec.Code)
	}
}

func TestEmptyBundleSetReturns503(t *testing.T) {
	srv := &Server{bundles: nil}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	rec := httptest.NewRecorder()
	srv.replay(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("empty bundle set should 503, got %d", rec.Code)
	}
}

func TestReplayServesCapturedStatusAndContentType(t *testing.T) {
	root := t.TempDir()
	writeBundle(t, root, "turn-overload", map[string]any{
		"status":       500,
		"content_type": "application/json",
		"finalized_by": "error_response",
	}, `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`)
	bundles, err := loadBundles(root)
	if err != nil {
		t.Fatalf("loadBundles: %v", err)
	}
	srv := &Server{bundles: bundles}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	rec := httptest.NewRecorder()
	srv.replay(rec, req)

	if rec.Code != 500 {
		t.Errorf("status: got %d, want 500 (replay must preserve upstream status)", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("content-type: got %q, want application/json", got)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), `"overloaded_error"`) {
		t.Errorf("body does not contain the captured error payload: %q", body)
	}
}

func TestSkipsMalformedBundles(t *testing.T) {
	root := t.TempDir()
	// Good bundle.
	writeBundle(t, root, "turn-good", map[string]any{"status": 200}, "ok")
	// Bundle missing response.sse.
	if err := os.MkdirAll(filepath.Join(root, "turn-no-response"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "turn-no-response", "meta.json"),
		[]byte(`{"status":200}`), 0o644); err != nil {
		t.Fatal(err)
	}
	// Bundle with status=0 (treated as malformed because we won't serve
	// a "no status" response that has no real-world meaning).
	writeBundle(t, root, "turn-zero-status", map[string]any{"status": 0}, "x")
	// Non-directory entry — should be ignored.
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("docs"), 0o644); err != nil {
		t.Fatal(err)
	}

	bundles, err := loadBundles(root)
	if err != nil {
		t.Fatalf("loadBundles: %v", err)
	}
	if len(bundles) != 1 || bundles[0].Name != "turn-good" {
		names := make([]string, 0, len(bundles))
		for _, b := range bundles {
			names = append(names, b.Name)
		}
		t.Errorf("expected only turn-good to load, got %v", names)
	}
}

func TestPreview(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		max  int
		want string
	}{
		{"empty", nil, 100, "<empty>"},
		{"ascii", []byte("event: message_start"), 100, "event: message_start"},
		{"gzip-magic", []byte{0x1f, 0x8b, 0x08, 0x00}, 100, `\x1f\x8b\x08\x00`},
		{"newline", []byte("a\nb"), 100, `a\nb`},
		{"truncate", []byte(strings.Repeat("x", 50)), 10, "xxxxxxxxxx...(50B total)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := preview(tc.in, tc.max); got != tc.want {
				t.Errorf("preview(%q, %d): got %q, want %q", tc.in, tc.max, got, tc.want)
			}
		})
	}
}

func TestListEndpoint(t *testing.T) {
	root := t.TempDir()
	writeBundle(t, root, "turn-001", map[string]any{
		"status":       200,
		"content_type": "text/event-stream",
		"finalized_by": "stream_complete",
	}, "event: message_start\n")
	bundles, _ := loadBundles(root)
	srv := &Server{bundles: bundles}

	req := httptest.NewRequest(http.MethodGet, "/_replay/list", nil)
	rec := httptest.NewRecorder()
	srv.list(rec, req)

	if rec.Code != 200 {
		t.Fatalf("list: got %d, want 200", rec.Code)
	}
	var entries []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &entries); err != nil {
		t.Fatalf("parse list response: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries: got %d, want 1", len(entries))
	}
	if entries[0]["name"] != "turn-001" {
		t.Errorf("name: got %v, want turn-001", entries[0]["name"])
	}
	if preview, _ := entries[0]["preview"].(string); !strings.Contains(preview, "message_start") {
		t.Errorf("preview should include message_start, got %q", preview)
	}
}
