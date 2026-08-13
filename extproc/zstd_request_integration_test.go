package extproc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	processingmodev3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/klauspost/compress/zstd"
)

// TestMalformedZstdRequestRecordsRequestDecodeDrop pins the observable failure
// mode for unsupported or corrupt request encodings.
func TestMalformedZstdRequestRecordsRequestDecodeDrop(t *testing.T) {
	proc, err := NewProcessor(Config{IngestURL: "http://127.0.0.1:1", MaxInflight: 1})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	obs := newObserver()
	proc.Dispatcher().SetObserver(obs)

	st := &streamState{
		provider:               "openai",
		method:                 http.MethodPost,
		path:                   "/local-gw/codex/responses",
		endpoint:               endpointResponses,
		requestID:              "malformed-zstd",
		requestContentEncoding: "zstd",
		statusCode:             http.StatusOK,
		startedAt:              time.Now(),
	}
	proc.onRequestBody(st, &extprocv3.HttpBody{
		Body:        []byte{0x28, 0xb5, 0x2f, 0xfd, 0xff},
		EndOfStream: true,
	})
	st.respBuf.WriteString("event: response.completed\ndata: {}\n\n")

	proc.dispatchTurn(context.Background(), st)

	if got := obs.DropCount(DropRequestDecode); got != 1 {
		t.Fatalf("request decode drops: got %d, want 1", got)
	}
}

// TestZstdCodexRequestRoundTripsThroughDispatchTurn covers the Pi 0.80.4+
// wire shape: Codex request JSON compressed with Content-Encoding: zstd and
// an uncompressed Responses SSE stream. The request must be decoded before
// stream/model detection, reduction, and json.RawMessage envelope marshaling.
func TestZstdCodexRequestRoundTripsThroughDispatchTurn(t *testing.T) {
	const requestJSON = `{"model":"gpt-5.4","stream":true,"instructions":"Reply briefly.","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`
	const responseSSE = `event: response.created
` +
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","status":"in_progress","model":"gpt-5.4","output":[]}}

` +
		`event: response.output_item.done
` +
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hello"}]}}

` +
		`event: response.completed
` +
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}

`

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	compressedRequest := encoder.EncodeAll([]byte(requestJSON), nil)
	encoder.Close()

	var received atomic.Pointer[TurnEnvelope]
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ingest read: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var env TurnEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			t.Errorf("ingest unmarshal: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		received.Store(&env)
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
			headerReq(map[string]string{
				":method":                    http.MethodPost,
				":path":                      "/local-gw/codex/responses",
				"content-encoding":           "zstd",
				"x-tapes-harness-id":         "pi",
				"x-tapes-harness-session-id": "pi-zstd-session",
			}),
			reqBodyReq(compressedRequest, true),
			respHeaderReq("200", ""),
			respBodyReq([]byte(responseSSE), true),
		},
	}

	if err := proc.Process(stream); err != nil {
		t.Fatalf("Process: %v", err)
	}

	var sawStreamingOverride bool
	for _, response := range stream.Responses() {
		if response.ModeOverride != nil &&
			response.ModeOverride.ResponseBodyMode == processingmodev3.ProcessingMode_FULL_DUPLEX_STREAMED {
			sawStreamingOverride = true
		}
	}
	if !sawStreamingOverride {
		t.Fatal("zstd request was not decoded in time to enable streamed response capture")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && received.Load() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	env := received.Load()
	if env == nil {
		t.Fatal("ingest never received the zstd Codex turn")
		return
	}
	if env.Provider != "openai" {
		t.Errorf("provider: got %q, want openai", env.Provider)
	}
	if string(env.Request) != requestJSON {
		t.Errorf("request was not restored to canonical JSON:\n got: %q\nwant: %q", env.Request, requestJSON)
	}
	if env.Meta.Model != "gpt-5.4" || env.Meta.Stream != "true" {
		t.Errorf("request metadata was not decoded: model=%q stream=%q", env.Meta.Model, env.Meta.Stream)
	}
	if env.Response == nil || len(env.Response.Message.Content) == 0 || env.Response.Message.Content[0].Text != "hello" {
		t.Fatalf("response was not reduced: %#v", env.Response)
	}
	if env.Session == nil || env.Session.HarnessID != "pi" || env.Session.HarnessSessionID != "pi-zstd-session" {
		t.Fatalf("Pi session envelope was not preserved: %#v", env.Session)
	}
}
