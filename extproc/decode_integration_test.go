package extproc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

// respHeaderReqWithEncoding mirrors respHeaderReq from
// processor_test.go but also sets content-encoding so we can drive
// the decode path. The helper lives in this file rather than the
// shared processor_test fixtures so the decode-path coverage stays
// self-contained next to the integration that uses it.
func respHeaderReqWithEncoding(status, ct, ce string) *extprocv3.ProcessingRequest {
	h := &corev3.HeaderMap{
		Headers: []*corev3.HeaderValue{
			{Key: ":status", RawValue: []byte(status)},
			{Key: "content-type", RawValue: []byte(ct)},
		},
	}
	if ce != "" {
		h.Headers = append(h.Headers, &corev3.HeaderValue{Key: "content-encoding", RawValue: []byte(ce)})
	}
	return &extprocv3.ProcessingRequest{
		Request: &extprocv3.ProcessingRequest_ResponseHeaders{
			ResponseHeaders: &extprocv3.HttpHeaders{Headers: h},
		},
	}
}

// TestGzipResponseRoundTripsThroughDispatchTurn drives a full ext_proc
// stream where the upstream response is gzip-compressed. It asserts:
//   - the test-side fake ingest receives a POST (no DropResponseDecode)
//   - the dispatched envelope's response carries non-empty Content
//
// This is the integration counterpart to TestDecodeResponseBody and
// the WireCapture fidelity tests — proves the decode helper is
// actually wired into dispatchTurn, not just unit-tested in isolation.
func TestGzipResponseRoundTripsThroughDispatchTurn(t *testing.T) {
	// A minimal but realistic Anthropic SSE turn. Mirrors goodAnthropicSSE
	// in reducer_adversarial_test.go; kept inline so this file is
	// self-contained.
	const sse = `event: message_start
data: {"type":"message_start","message":{"id":"msg_X","type":"message","role":"assistant","content":[],"model":"claude-3-5-sonnet-20241022","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}

event: message_stop
data: {"type":"message_stop"}

`

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write([]byte(sse)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}

	// Capture the dispatched envelope so we can assert what extproc
	// would have POSTed to tapes-ingest.
	var receivedEnvelope atomic.Pointer[TurnEnvelope]
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ingest read: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		var env TurnEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			t.Errorf("ingest unmarshal: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		receivedEnvelope.Store(&env)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ingest.Close()

	proc, err := NewProcessor(Config{IngestURL: ingest.URL, MaxInflight: 4})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}

	stream := &fakeStream{
		ctx: context.Background(),
		toSend: []*extprocv3.ProcessingRequest{
			headerReq(map[string]string{":path": "/v1/messages"}),
			reqBodyReq([]byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"hi"}]}`), true),
			respHeaderReqWithEncoding("200", "text/event-stream", "gzip"),
			respBodyReq(compressed.Bytes(), true),
		},
	}

	if err := proc.Process(stream); err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Dispatch is async — wait briefly for the POST to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if receivedEnvelope.Load() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	env := receivedEnvelope.Load()
	if env == nil {
		t.Fatal("ingest never received the dispatched turn — decode path likely dropped it")
	}

	if env.Provider != "anthropic" {
		t.Errorf("provider: got %q, want anthropic", env.Provider)
	}
	if env.Response == nil {
		t.Fatalf("response is nil — decode path didn't produce a ChatResponse")
	}
	if env.Response.Message.Role != "assistant" {
		t.Errorf("role: got %q, want assistant", env.Response.Message.Role)
	}
	if len(env.Response.Message.Content) != 1 {
		t.Fatalf("content blocks: got %d, want 1", len(env.Response.Message.Content))
	}
	if env.Response.Message.Content[0].Type != "text" {
		t.Errorf("block type: got %q, want text", env.Response.Message.Content[0].Type)
	}
	if env.Response.Message.Content[0].Text != "Hello" {
		t.Errorf("block text: got %q, want %q", env.Response.Message.Content[0].Text, "Hello")
	}
	if env.Response.StopReason != "end_turn" {
		t.Errorf("stop_reason: got %q, want end_turn", env.Response.StopReason)
	}
	// PCC-570: extproc must stamp the proxy-measured wall-clock onto Usage.
	// The legacy tapes/proxy stampDuration never runs on cluster traffic, so
	// without the dispatchTurn stamp nodes.total_duration_ns stays NULL.
	if env.Response.Usage == nil {
		t.Fatal("response usage is nil — duration stamp would have nothing to write")
	}
	if env.Response.Usage.TotalDurationNs <= 0 {
		t.Errorf("total_duration_ns: got %d, want > 0 (wall-clock not stamped)", env.Response.Usage.TotalDurationNs)
	}
}

func TestSalvageTruncatedGzipDispatches(t *testing.T) {
	const sse = `event: message_start
data: {"type":"message_start","message":{"id":"msg_X","type":"message","role":"assistant","content":[],"model":"claude-3-5-sonnet-20241022","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}

event: message_stop
data: {"type":"message_stop"}

`

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write([]byte(sse)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	truncated := compressed.Bytes()[:compressed.Len()-8]

	var receivedEnvelope atomic.Pointer[TurnEnvelope]
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ingest read: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		var env TurnEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			t.Errorf("ingest unmarshal: %v", err)
			http.Error(w, err.Error(), 500)
			return
		}
		receivedEnvelope.Store(&env)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ingest.Close()

	proc, err := NewProcessor(Config{IngestURL: ingest.URL, MaxInflight: 4})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}

	stream := &fakeStream{
		ctx: context.Background(),
		toSend: []*extprocv3.ProcessingRequest{
			headerReq(map[string]string{":method": "POST", ":path": "/v1/messages"}),
			reqBodyReq([]byte(`{"model":"claude-3-5-sonnet-20241022","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`), true),
			respHeaderReqWithEncoding("200", "text/event-stream", "gzip"),
			respBodyReq(truncated, false),
		},
	}
	if err := proc.Process(stream); err != nil {
		t.Fatalf("Process: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if receivedEnvelope.Load() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	env := receivedEnvelope.Load()
	if env == nil {
		t.Fatal("ingest never received the dispatched turn")
	}
	if env.Response == nil {
		t.Fatal("response is nil")
	}
	if env.Response.Message.Role != "assistant" {
		t.Errorf("role: got %q, want assistant", env.Response.Message.Role)
	}
	if len(env.Response.Message.Content) == 0 {
		t.Fatal("no content blocks decoded from the truncated gzip stream")
	}
	if env.Response.Message.Content[0].Text != "Hello" {
		t.Errorf("block text: got %q, want %q", env.Response.Message.Content[0].Text, "Hello")
	}

	metrics := scrapeDecodeIntegrationMetrics(t, proc)
	if !strings.Contains(metrics, `tapes_extproc_response_decode_salvaged_total{encoding="gzip",message_stop_seen="true",provider="anthropic"} 1`) {
		t.Fatalf("missing salvaged truncated gzip metric:\n%s", metrics)
	}
}

func scrapeDecodeIntegrationMetrics(t *testing.T, proc *Processor) string {
	t.Helper()
	srv := httptest.NewServer(proc.Metrics().Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	return string(body)
}
