package headers

import (
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

func mkHeaders(pairs ...[2]string) *extprocv3.HttpHeaders {
	hm := &corev3.HeaderMap{}
	for _, p := range pairs {
		hm.Headers = append(hm.Headers, &corev3.HeaderValue{
			Key:      p[0],
			RawValue: []byte(p[1]),
		})
	}
	return &extprocv3.HttpHeaders{Headers: hm}
}

func TestGet_PresentAndCaseInsensitive(t *testing.T) {
	hdrs := mkHeaders(
		[2]string{":status", "200"},
		[2]string{"Content-Type", "text/event-stream"},
		[2]string{"x-request-id", "abc-123"},
	)
	if got := Get(hdrs, Status); got != "200" {
		t.Errorf("Status: got %q want %q", got, "200")
	}
	if got := Get(hdrs, ContentType); got != "text/event-stream" {
		t.Errorf("ContentType: got %q want %q", got, "text/event-stream")
	}
	if got := Get(hdrs, "CONTENT-TYPE"); got != "text/event-stream" {
		t.Errorf("uppercase lookup should case-insensitively match: got %q", got)
	}
	if got := Get(hdrs, RequestID); got != "abc-123" {
		t.Errorf("RequestID: got %q want %q", got, "abc-123")
	}
}

func TestGet_Missing(t *testing.T) {
	hdrs := mkHeaders([2]string{":status", "200"})
	if got := Get(hdrs, ContentEncoding); got != "" {
		t.Errorf("missing header should return \"\", got %q", got)
	}
}

func TestGet_PrefersRawValueOverValue(t *testing.T) {
	hm := &corev3.HeaderMap{
		Headers: []*corev3.HeaderValue{
			{Key: "content-type", Value: "value-field", RawValue: []byte("rawvalue-field")},
		},
	}
	hdrs := &extprocv3.HttpHeaders{Headers: hm}
	if got := Get(hdrs, ContentType); got != "rawvalue-field" {
		t.Errorf("RawValue should win when both are set: got %q", got)
	}
}

func TestGet_FallsBackToValueWhenRawValueEmpty(t *testing.T) {
	// Older Envoy builds populate Value but leave RawValue empty.
	// extproc must still find the header.
	hm := &corev3.HeaderMap{
		Headers: []*corev3.HeaderValue{
			{Key: "content-type", Value: "application/json"},
		},
	}
	hdrs := &extprocv3.HttpHeaders{Headers: hm}
	if got := Get(hdrs, ContentType); got != "application/json" {
		t.Errorf("Value fallback failed: got %q", got)
	}
}

func TestThreadID(t *testing.T) {
	// IDs mirror captured wire evidence: Codex stamps session-id and
	// thread-id on every call; root turns carry thread-id == session-id.
	const (
		codexRoot   = "019f863d-0cd6-7ce2-b481-20abd683a14e"
		codexChild  = "019f8713-2213-75e3-be33-36fd2f8dd384"
		claudeAgent = "agent-0a1b2c3d"
	)

	tests := []struct {
		name string
		hdrs *extprocv3.HttpHeaders
		want string
	}{
		{
			name: "no thread headers resolves to main thread",
			hdrs: mkHeaders([2]string{"content-type", "application/json"}),
			want: "",
		},
		{
			name: "claude subagent header maps verbatim",
			hdrs: mkHeaders([2]string{"x-claude-code-agent-id", claudeAgent}),
			want: claudeAgent,
		},
		{
			name: "claude header wins over a codex-shaped pair",
			hdrs: mkHeaders(
				[2]string{"x-claude-code-agent-id", claudeAgent},
				[2]string{"session-id", codexRoot},
				[2]string{"thread-id", codexChild},
			),
			want: claudeAgent,
		},
		{
			name: "codex child turn maps thread-id",
			hdrs: mkHeaders(
				[2]string{"session-id", codexRoot},
				[2]string{"thread-id", codexChild},
			),
			want: codexChild,
		},
		{
			name: "codex root guard: thread-id equal to session-id is main thread",
			hdrs: mkHeaders(
				[2]string{"session-id", codexRoot},
				[2]string{"thread-id", codexRoot},
			),
			want: "",
		},
		{
			name: "codex session-id without thread-id is main thread",
			hdrs: mkHeaders([2]string{"session-id", codexRoot}),
			want: "",
		},
		{
			name: "lone thread-id without session-id is not a codex shape",
			hdrs: mkHeaders([2]string{"thread-id", codexChild}),
			want: "",
		},
		{
			name: "codex pair matches case-insensitively",
			hdrs: mkHeaders(
				[2]string{"Session-Id", codexRoot},
				[2]string{"Thread-Id", codexChild},
			),
			want: codexChild,
		},
		{
			// Get resolves duplicate headers first-occurrence-wins;
			// pin that ThreadID inherits that determinism.
			name: "duplicate thread-id headers resolve first-occurrence-wins",
			hdrs: mkHeaders(
				[2]string{"session-id", codexRoot},
				[2]string{"thread-id", codexChild},
				[2]string{"thread-id", codexRoot},
			),
			want: codexChild,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ThreadID(tc.hdrs); got != tc.want {
				t.Errorf("ThreadID: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestStatusCode_Parses(t *testing.T) {
	hdrs := mkHeaders([2]string{":status", "200"})
	if got := StatusCode(hdrs); got != 200 {
		t.Errorf("StatusCode: got %d want 200", got)
	}
}

func TestStatusCode_MissingReturnsZero(t *testing.T) {
	hdrs := mkHeaders([2]string{"content-type", "application/json"})
	if got := StatusCode(hdrs); got != 0 {
		t.Errorf("missing :status should return 0, got %d", got)
	}
}

func TestStatusCode_UnparseableReturnsZero(t *testing.T) {
	hdrs := mkHeaders([2]string{":status", "not-a-number"})
	if got := StatusCode(hdrs); got != 0 {
		t.Errorf("unparseable :status should return 0, got %d", got)
	}
}
