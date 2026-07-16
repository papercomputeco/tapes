package exportcmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/papercomputeco/tapes/pkg/skill"
)

const testSessionID = "0196fdb1-93f4-7c41-a53d-0fbe2c5e1f23"

// exportBody is a stand-in for the API's session→traces→spans JSONL line.
const exportBody = `{"schema":"2026-06-15","session":{"id":"0196fdb1-93f4-7c41-a53d-0fbe2c5e1f23"},"traces":[],"links":[]}` + "\n"

// exportServer serves the two endpoints the command touches: the export
// itself, and the session list (for prefix resolution).
func exportServer(t *testing.T, wantDetail string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sessions/" + testSessionID + "/export":
			if got := r.URL.Query().Get("detail"); got != wantDetail {
				t.Errorf("detail = %q, want %q", got, wantDetail)
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = w.Write([]byte(exportBody))
		case "/v1/sessions":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"items":[{"id":"` + testSessionID + `"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestExportWritesTheAPIBodyVerbatim(t *testing.T) {
	srv := exportServer(t, "spans")
	defer srv.Close()

	out := filepath.Join(t.TempDir(), "session.jsonl")
	cmd := NewExportCmd()
	cmd.SetArgs([]string{testSessionID, "--api-target", srv.URL, "-o", out})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != exportBody {
		t.Errorf("export body = %q, want the API body %q", got, exportBody)
	}
}

func TestExportRejectsBadDetail(t *testing.T) {
	cmd := NewExportCmd()
	cmd.SetArgs([]string{testSessionID, "--api-target", "http://x", "--detail", "nodes"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected an error for --detail nodes")
	}
}

func TestResolveSessionID(t *testing.T) {
	srv := exportServer(t, "spans")
	defer srv.Close()
	client := skill.NewAPIClient(srv.URL)

	// A full UUID passes through without a list call.
	got, err := resolveSessionID(context.Background(), client, testSessionID)
	if err != nil || got != testSessionID {
		t.Fatalf("full uuid: got %q, err %v", got, err)
	}

	// A unique prefix resolves against the session list.
	got, err = resolveSessionID(context.Background(), client, "0196fdb1")
	if err != nil || got != testSessionID {
		t.Fatalf("prefix: got %q, err %v", got, err)
	}

	// An unknown prefix is a clean error, not a silent empty.
	if _, err := resolveSessionID(context.Background(), client, "deadbeef"); err == nil {
		t.Fatal("expected an error for an unmatched prefix")
	}
}
