package extproc

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	processingmodev3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/klauspost/compress/zstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// fakeStream is a hand-rolled ExternalProcessor_ProcessServer that feeds
// pre-scripted ProcessingRequest messages to the processor and captures every
// ProcessingResponse it sends. <50 LOC, no test dependency beyond the stdlib
// and the grpc metadata type the interface requires.
type fakeStream struct {
	ctx context.Context

	toSend    []*extprocv3.ProcessingRequest
	sendIndex int

	recvMu sync.Mutex
	recv   []*extprocv3.ProcessingResponse
}

func (s *fakeStream) Recv() (*extprocv3.ProcessingRequest, error) {
	if s.sendIndex >= len(s.toSend) {
		return nil, io.EOF
	}
	msg := s.toSend[s.sendIndex]
	s.sendIndex++
	return msg, nil
}

func (s *fakeStream) Send(resp *extprocv3.ProcessingResponse) error {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	s.recv = append(s.recv, resp)
	return nil
}

func (s *fakeStream) Responses() []*extprocv3.ProcessingResponse {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	out := make([]*extprocv3.ProcessingResponse, len(s.recv))
	copy(out, s.recv)
	return out
}

// grpc stream interface stubs — we implement only what Process actually calls.
func (s *fakeStream) Context() context.Context     { return s.ctx }
func (s *fakeStream) SendMsg(_ any) error          { return nil }
func (s *fakeStream) RecvMsg(_ any) error          { return nil }
func (s *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeStream) SetTrailer(metadata.MD)       {}

func headerReq(hdrs map[string]string) *extprocv3.ProcessingRequest {
	h := &corev3.HeaderMap{}
	for k, v := range hdrs {
		h.Headers = append(h.Headers, &corev3.HeaderValue{Key: k, RawValue: []byte(v)})
	}
	return &extprocv3.ProcessingRequest{
		Request: &extprocv3.ProcessingRequest_RequestHeaders{
			RequestHeaders: &extprocv3.HttpHeaders{Headers: h},
		},
	}
}

func respHeaderReq(status string, ct string) *extprocv3.ProcessingRequest {
	h := &corev3.HeaderMap{
		Headers: []*corev3.HeaderValue{
			{Key: ":status", RawValue: []byte(status)},
			{Key: "content-type", RawValue: []byte(ct)},
		},
	}
	return &extprocv3.ProcessingRequest{
		Request: &extprocv3.ProcessingRequest_ResponseHeaders{
			ResponseHeaders: &extprocv3.HttpHeaders{Headers: h},
		},
	}
}

func reqBodyReq(body []byte, eos bool) *extprocv3.ProcessingRequest {
	return &extprocv3.ProcessingRequest{
		Request: &extprocv3.ProcessingRequest_RequestBody{
			RequestBody: &extprocv3.HttpBody{Body: body, EndOfStream: eos},
		},
	}
}

func respBodyReq(body []byte, eos bool) *extprocv3.ProcessingRequest {
	return &extprocv3.ProcessingRequest{
		Request: &extprocv3.ProcessingRequest_ResponseBody{
			ResponseBody: &extprocv3.HttpBody{Body: body, EndOfStream: eos},
		},
	}
}

func scrapeProcessorMetrics(proc *Processor) string {
	srv := httptest.NewServer(proc.Metrics().Handler())
	defer srv.Close()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/", nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := srv.Client().Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

// observer is a test spy that records terminal Dispatcher outcomes.
type observer struct {
	mu       sync.Mutex
	accepted atomic.Int32
	drops    map[DropReason]int
}

func newObserver() *observer {
	return &observer{drops: map[DropReason]int{}}
}

func (o *observer) OnAccepted(_ string, _ string) { o.accepted.Add(1) }
func (o *observer) OnDrop(_ string, reason DropReason, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.drops[reason]++
}

func (o *observer) DropCount(reason DropReason) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.drops[reason]
}

// teeObserver fans terminal outcomes out to two observers so a single
// Process run can assert both spy counts and real metric rows.
type teeObserver struct{ a, b Observer }

func (t teeObserver) OnAccepted(provider, requestID string) {
	t.a.OnAccepted(provider, requestID)
	t.b.OnAccepted(provider, requestID)
}

func (t teeObserver) OnDrop(provider string, reason DropReason, requestID string) {
	t.a.OnDrop(provider, reason, requestID)
	t.b.OnDrop(provider, reason, requestID)
}

// recordingObserver captures the last request ID seen on any drop, so tests
// can assert on ID-synthesis behavior.
type recordingObserver struct {
	mu            sync.Mutex
	lastRequestID string
}

func newRecordingObserver() *recordingObserver { return &recordingObserver{} }

func (o *recordingObserver) OnAccepted(_ string, requestID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lastRequestID = requestID
}

func (o *recordingObserver) OnDrop(_ string, _ DropReason, requestID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lastRequestID = requestID
}

var _ = Describe("Processor state machine", func() {
	var (
		proc       *Processor
		obs        *observer
		ingestSrv  *httptest.Server
		ingestBody atomic.Value // last body received
	)

	BeforeEach(func() {
		ingestBody = atomic.Value{}
		ingestSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			ingestBody.Store(b)
			w.WriteHeader(http.StatusAccepted)
		}))

		var err error
		proc, err = NewProcessor(Config{
			IngestURL:   ingestSrv.URL,
			MaxInflight: 4,
		})
		Expect(err).NotTo(HaveOccurred())

		obs = newObserver()
		proc.Dispatcher().SetObserver(obs)
	})

	AfterEach(func() {
		ingestSrv.Close()
	})

	Describe("reducerFor", func() {
		It("serves anthropic only on the messages endpoint", func() {
			_, ok := proc.reducerFor("anthropic", "messages")
			Expect(ok).To(BeTrue())

			// The Messages reducer must never be fed other wire formats
			// (e.g. OpenAI-format frames the AI Gateway translates for an
			// Anthropic backend on /v1/chat/completions).
			_, ok = proc.reducerFor("anthropic", "chat_completions")
			Expect(ok).To(BeFalse())

			_, ok = proc.reducerFor("anthropic", "messages_count_tokens")
			Expect(ok).To(BeFalse())
		})

		It("serves openai only on the responses endpoint", func() {
			_, ok := proc.reducerFor("openai", "responses")
			Expect(ok).To(BeTrue())

			// Chat Completions must keep the pre-Codex behavior: no
			// reducer, default BUFFERED mode, unknown_provider drop.
			// The Responses reducer cannot parse chat.completion frames.
			_, ok = proc.reducerFor("openai", "chat_completions")
			Expect(ok).To(BeFalse())

			_, ok = proc.reducerFor("openai", "other")
			Expect(ok).To(BeFalse())
		})

		It("rejects unknown providers", func() {
			_, ok := proc.reducerFor("ollama", "ollama_chat")
			Expect(ok).To(BeFalse())
		})
	})

	It("non-streaming one-shot: accumulates body and dispatches on EOS", func() {
		body := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}`)
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)

		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages", "x-tapes-agent-name": "test"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("200", "application/json"),
				respBodyReq(body, true),
			},
		}

		Expect(proc.Process(stream)).To(Succeed())

		Eventually(func() int32 { return obs.accepted.Load() }).
			WithTimeout(2 * time.Second).
			Should(Equal(int32(1)))
	})

	It("metrics: records accepted terminal outcomes after ingest accepts", func() {
		proc.Dispatcher().SetObserver(proc.Metrics().AsObserver())
		body := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}`)
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)

		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("200", "application/json"),
				respBodyReq(body, true),
			},
		}

		Expect(proc.Process(stream)).To(Succeed())
		Eventually(func() string {
			return scrapeProcessorMetrics(proc)
		}).WithTimeout(2 * time.Second).Should(And(
			ContainSubstring(`tapes_extproc_turns_terminal_total`),
			ContainSubstring(`outcome="accepted"`),
			ContainSubstring(`reason="accepted"`),
			ContainSubstring(`endpoint="messages"`),
			ContainSubstring(`model_family="claude-3-5-sonnet"`),
			ContainSubstring(`upstream_status_class="2xx"`),
			ContainSubstring(`tapes_extproc_body_bytes_by_outcome_bucket`),
		))
	})

	It("non-turn Anthropic endpoints: skips count_tokens instead of dispatching an empty response", func() {
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}]}`)
		respBody := []byte(`{"input_tokens":42}`)

		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/v1/messages/count_tokens"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("200", "application/json"),
				respBodyReq(respBody, true),
			},
		}

		Expect(proc.Process(stream)).To(Succeed())
		Expect(obs.DropCount(DropNonTurnRequest)).To(Equal(1))
		Expect(obs.accepted.Load()).To(Equal(int32(0)))
		Consistently(func() any { return ingestBody.Load() }).
			WithTimeout(100 * time.Millisecond).
			Should(BeNil())
	})

	It("metrics: classifies count_tokens as a non-turn Anthropic endpoint", func() {
		proc.Dispatcher().SetObserver(proc.Metrics().AsObserver())
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}]}`)

		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/v1/messages/count_tokens"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("200", "application/json"),
				respBodyReq([]byte(`{"input_tokens":42}`), true),
			},
		}

		Expect(proc.Process(stream)).To(Succeed())
		txt := scrapeProcessorMetrics(proc)
		Expect(txt).To(ContainSubstring(`tapes_extproc_turns_terminal_total`))
		Expect(txt).To(ContainSubstring(`endpoint="messages_count_tokens"`))
		Expect(txt).To(ContainSubstring(`model_family="claude-3-5-sonnet"`))
		Expect(txt).To(ContainSubstring(`outcome="dropped"`))
		Expect(txt).To(ContainSubstring(`reason="non_turn_request"`))
		Expect(txt).To(ContainSubstring(`stream="false"`))
		Expect(txt).To(ContainSubstring(`upstream_status_class="2xx"`))
	})

	It("non-turn methods: skips HEAD health probes on the messages path", func() {
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "HEAD", ":path": "/v1/messages"}),
				reqBodyReq(nil, true),
				respHeaderReq("200", "application/json"),
				respBodyReq(nil, true),
			},
		}

		Expect(proc.Process(stream)).To(Succeed())
		Expect(obs.DropCount(DropNonTurnRequest)).To(Equal(1))
		Expect(obs.accepted.Load()).To(Equal(int32(0)))
	})

	It("streaming: issues ModeOverride on RequestBody EOS when stream:true", func() {
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
			},
		}
		Expect(proc.Process(stream)).To(Succeed())

		responses := stream.Responses()
		Expect(len(responses)).To(BeNumerically(">=", 2))
		// The RequestBody response carries the ModeOverride.
		last := responses[len(responses)-1]
		Expect(last.ModeOverride).NotTo(BeNil(), "expected ModeOverride on RequestBody response")
		Expect(last.ModeOverride.ResponseBodyMode).To(Equal(processingmodev3.ProcessingMode_FULL_DUPLEX_STREAMED))
	})

	It("streaming: does NOT issue ModeOverride when stream:false", func() {
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"stream":false,"messages":[{"role":"user","content":"hi"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
			},
		}
		Expect(proc.Process(stream)).To(Succeed())
		for _, r := range stream.Responses() {
			Expect(r.ModeOverride).To(BeNil())
		}
	})

	It("streaming multi-chunk: accumulates with append, dispatches once on EOS", func() {
		// Split a well-formed SSE stream into 3 chunks. If the processor
		// overwrites respBuf instead of appending (the 4-week bug), only the
		// last chunk survives and the reducer will not produce a ChatResponse
		// with the accumulated text "Hello world".
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)

		chunk1 := `event: message_start
data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-3-5-sonnet-20241022","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

`
		chunk2 := `event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}

`
		chunk3 := `event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":2}}

event: message_stop
data: {"type":"message_stop"}

`

		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("200", "text/event-stream"),
				respBodyReq([]byte(chunk1), false),
				respBodyReq([]byte(chunk2), false),
				respBodyReq([]byte(chunk3), true),
			},
		}
		Expect(proc.Process(stream)).To(Succeed())

		Eventually(func() int32 { return obs.accepted.Load() }).
			WithTimeout(2 * time.Second).
			Should(Equal(int32(1)))

		// Inspect what ingest received to confirm both chunks' text landed.
		Eventually(func() string {
			if v := ingestBody.Load(); v != nil {
				return string(v.([]byte))
			}
			return ""
		}).WithTimeout(2 * time.Second).Should(And(
			ContainSubstring(`"text":"Hello world"`),
			ContainSubstring(`"prompt_tokens":1`),
			ContainSubstring(`"completion_tokens":2`),
			ContainSubstring(`"message":{`),          // response is canonical llm.ChatResponse for ingest
			Not(ContainSubstring(`"text":" world"`)), // would indicate only last chunk survived
		))
	})

	It("codex responses: full pipeline captures a streamed turn with item accumulation", func() {
		// Mirrors the real Codex wire shape end-to-end: a Responses
		// request on the direct-route path (no AI-gateway backend
		// selector header), an SSE body split across chunks, and the
		// chatgpt.com quirk of a terminal event whose output array is
		// empty — items must come from response.output_item.done.
		proc.Dispatcher().SetObserver(proc.Metrics().AsObserver())
		reqBody := []byte(`{"model":"gpt-5.5","stream":true,"instructions":"You are Codex.","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)

		chunk1 := `event: response.created
data: {"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1781221954,"status":"in_progress","model":"gpt-5.5","output":[]}}

event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"Hello"}

`
		chunk2 := `event: response.output_item.done
data: {"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Hello world"}]}}

event: response.completed
data: {"type":"response.completed","response":{"id":"resp_1","object":"response","created_at":1781221954,"status":"completed","model":"gpt-5.5","output":[],"usage":{"input_tokens":26,"input_tokens_details":{"cached_tokens":0},"output_tokens":21,"total_tokens":47}}}

`

		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/local-gw/v1/responses"}),
				reqBodyReq(reqBody, true),
				// chatgpt.com omits Content-Type on its SSE responses.
				respHeaderReq("200", ""),
				respBodyReq([]byte(chunk1), false),
				respBodyReq([]byte(chunk2), true),
			},
		}
		Expect(proc.Process(stream)).To(Succeed())

		// Streaming + capturable (provider openai via path, endpoint
		// responses) must flip the response body to FULL_DUPLEX_STREAMED.
		var sawOverride bool
		for _, r := range stream.Responses() {
			if r.ModeOverride != nil &&
				r.ModeOverride.ResponseBodyMode == processingmodev3.ProcessingMode_FULL_DUPLEX_STREAMED {
				sawOverride = true
			}
		}
		Expect(sawOverride).To(BeTrue(), "expected ModeOverride for streamed Responses turn")

		Eventually(func() string {
			if v := ingestBody.Load(); v != nil {
				return string(v.([]byte))
			}
			return ""
		}).WithTimeout(2 * time.Second).Should(And(
			ContainSubstring(`"provider":"openai"`),
			ContainSubstring(`"text":"Hello world"`), // from output_item.done, not the empty terminal output
			ContainSubstring(`"prompt_tokens":26`),
			ContainSubstring(`"completion_tokens":21`),
			ContainSubstring(`"stop_reason":"stop"`),
		))

		txt := scrapeProcessorMetrics(proc)
		Expect(txt).To(ContainSubstring(`endpoint="responses"`))
		Expect(txt).To(ContainSubstring(`model_family="gpt-5"`))
		Expect(txt).To(ContainSubstring(`outcome="accepted"`))
	})

	It("codex responses: chatgpt-plan route path classifies identically", func() {
		// The ChatGPT-plan route rewrites onto /<gw>/codex/... — the
		// /codex/responses suffix must classify exactly like
		// /v1/responses so both Codex auth modes capture.
		Expect(isTurnRequestPath("/local-gw/codex/responses")).To(BeTrue())
		Expect(classifyEndpoint("/local-gw/codex/responses")).To(Equal("responses"))
	})

	It("unknown provider: drops with reason=unknown_provider", func() {
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{
					":path":                    "/v1/messages",
					"x-ai-eg-selected-backend": "weird-backend",
				}),
				reqBodyReq([]byte(`{}`), true),
				respHeaderReq("200", "application/json"),
				respBodyReq([]byte(`{}`), true),
			},
		}
		// resolveProvider falls through to "anthropic", which has a reducer
		// in p.reducers — so dispatchTurn would just succeed. Force the
		// unknown-provider drop by pointing the provider map at a name not
		// in p.reducers.
		proc.SetProviderMap(map[string]string{"weird-backend": "totally-unknown-provider"})

		Expect(proc.Process(stream)).To(Succeed())
		Eventually(func() int { return obs.DropCount(DropUnknownProvider) }).
			WithTimeout(2 * time.Second).
			Should(Equal(1))
	})

	It("incomplete 200 response: salvages buffered bytes via the reducer instead of dropping", func() {
		// Under ext_proc Streamed mode the downstream client commonly
		// closes the connection right after parsing the SSE message_stop
		// event, before the upstream sends TCP FIN. The defer at the top
		// of Process() salvages this case by dispatching the buffered
		// bytes through the reducer rather than masquerading the
		// successful turn as client_disconnect.
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
		// A complete logical Anthropic SSE turn: message_start →
		// content_block (text "ok") → message_delta(end_turn) →
		// message_stop. No upstream EndOfStream signal is delivered.
		respSSE := []byte(strings.Join([]string{
			`event: message_start`,
			`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":0}}}`,
			``,
			`event: content_block_start`,
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
			``,
			`event: content_block_delta`,
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}`,
			``,
			`event: content_block_stop`,
			`data: {"type":"content_block_stop","index":0}`,
			``,
			`event: message_delta`,
			`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":1}}`,
			``,
			`event: message_stop`,
			`data: {"type":"message_stop"}`,
			``,
		}, "\n"))
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("200", "text/event-stream"),
				respBodyReq(respSSE, false), // no upstream EOS — client closed first.
			},
		}
		Expect(proc.Process(stream)).To(Succeed())
		Eventually(obs.accepted.Load).WithTimeout(2 * time.Second).Should(Equal(int32(1)))
		Expect(obs.DropCount(DropClientDisconnect)).To(Equal(0))
		Expect(obs.DropCount(DropUpstreamNoResponse)).To(Equal(0))
	})

	It("incomplete non-200 response: still drops as client_disconnect (salvage gated on 200 OK)", func() {
		// The salvage path only fires for HTTP 200 — a partial non-2xx
		// response with no EOS reflects a true upstream/client
		// disconnect, not a reducible turn.
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("502", "text/plain"),
				respBodyReq([]byte("bad gateway"), false), // no EOS
			},
		}
		Expect(proc.Process(stream)).To(Succeed())
		Expect(obs.DropCount(DropClientDisconnect)).To(Equal(1))
		Expect(obs.accepted.Load()).To(Equal(int32(0)))
	})

	It("upstream non-200 status: drops with reason=upstream_status and does not dispatch", func() {
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("500", "application/json"),
				respBodyReq([]byte(`{"error":"nope"}`), true),
			},
		}
		Expect(proc.Process(stream)).To(Succeed())
		Expect(obs.DropCount(DropUpstreamStatus)).To(Equal(1))
		Expect(obs.accepted.Load()).To(Equal(int32(0)))
	})

	It("metrics: labels upstream non-200 drops with status class and request metadata", func() {
		proc.Dispatcher().SetObserver(proc.Metrics().AsObserver())
		reqBody := []byte(`{"model":"claude-opus-4-7","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("503", "application/json"),
				respBodyReq([]byte(`{"error":"overloaded"}`), true),
			},
		}

		Expect(proc.Process(stream)).To(Succeed())
		txt := scrapeProcessorMetrics(proc)
		Expect(txt).To(ContainSubstring(`endpoint="messages"`))
		Expect(txt).To(ContainSubstring(`model_family="claude-opus-4"`))
		Expect(txt).To(ContainSubstring(`reason="upstream_status"`))
		Expect(txt).To(ContainSubstring(`stream="true"`))
		Expect(txt).To(ContainSubstring(`upstream_status_class="5xx"`))
		Expect(txt).To(ContainSubstring(`tapes_extproc_body_bytes_by_outcome_bucket`))
		Expect(txt).To(ContainSubstring(`outcome="dropped"`))
	})

	It("missing :status: drops with reason=missing_status instead of dispatching", func() {
		// Skip the response-headers frame entirely so statusCode stays 0.
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages"}),
				reqBodyReq([]byte(`{"model":"m","max_tokens":1,"messages":[{"role":"user","content":"x"}]}`), true),
				// no respHeaderReq here
				respBodyReq([]byte(`{}`), true),
			},
		}
		Expect(proc.Process(stream)).To(Succeed())
		Expect(obs.DropCount(DropMissingStatus)).To(Equal(1))
		Expect(obs.accepted.Load()).To(Equal(int32(0)))
	})

	It("upstream silence after request EOS: drops with reason=upstream_no_response", func() {
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
				// stream ends here — no response ever arrived
			},
		}
		Expect(proc.Process(stream)).To(Succeed())
		Expect(obs.DropCount(DropUpstreamNoResponse)).To(Equal(1))
		Expect(obs.DropCount(DropClientDisconnect)).To(Equal(0))
	})

	It("drop logs include model, endpoint, stream, size, and elapsed context", func() {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
		defer slog.SetDefault(previous)

		reqBody := []byte(`{"model":"claude-opus-4-7","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/v1/messages", "x-request-id": "req-log"}),
				reqBodyReq(reqBody, true),
			},
		}

		Expect(proc.Process(stream)).To(Succeed())
		line := logs.String()
		Expect(line).To(ContainSubstring(`"msg":"extproc drop"`))
		Expect(line).To(ContainSubstring(`"reason":"upstream_no_response"`))
		Expect(line).To(ContainSubstring(`"request_id":"req-log"`))
		Expect(line).To(ContainSubstring(`"endpoint":"messages"`))
		Expect(line).To(ContainSubstring(`"stream":"true"`))
		Expect(line).To(ContainSubstring(`"model":"claude-opus-4-7"`))
		Expect(line).To(ContainSubstring(`"model_family":"claude-opus-4"`))
		Expect(line).To(ContainSubstring(`"req_bytes":`))
		Expect(line).To(ContainSubstring(`"elapsed_ms":`))
	})

	It("empty 200 upstream body: drops with reason=empty_response, not reducer_error", func() {
		// Upstream completes the response phase with EOS and zero body
		// bytes. Distinct from upstream_no_response (which never reaches
		// ResponseBody at all) and from reducer-produced empty content
		// (which requires bytes that parsed). Classified as
		// expected-traffic so it does NOT take the reducer-error path.
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
				respHeaderReq("200", "application/json"),
				respBodyReq(nil, true),
			},
		}
		Expect(proc.Process(stream)).To(Succeed())
		Expect(obs.DropCount(DropEmptyResponse)).To(Equal(1))
		Expect(obs.DropCount(DropReducerError)).To(Equal(0))
		Expect(obs.DropCount(DropResponseDecode)).To(Equal(0))
		Expect(obs.accepted.Load()).To(Equal(int32(0)))
		Consistently(func() any { return ingestBody.Load() }).
			WithTimeout(100 * time.Millisecond).
			Should(BeNil())
	})

	It("does NOT issue ModeOverride for an unregistered provider even with stream:true", func() {
		proc.SetProviderMap(map[string]string{"weird-backend": "totally-unknown-provider"})
		reqBody := []byte(`{"model":"m","max_tokens":1,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{
					":path":                    "/unknown",
					"x-ai-eg-selected-backend": "weird-backend",
				}),
				reqBodyReq(reqBody, true),
			},
		}
		Expect(proc.Process(stream)).To(Succeed())
		for _, r := range stream.Responses() {
			Expect(r.ModeOverride).To(BeNil(),
				"should not issue ModeOverride without a registered reducer")
		}
	})

	It("synthesizes a request ID when x-request-id is missing so drops are triageable", func() {
		reqBody := []byte(`{"model":"m","max_tokens":1,"stream":true,"messages":[{"role":"user","content":"x"}]}`)
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":path": "/v1/messages"}),
				reqBodyReq(reqBody, true),
				// No response — force the upstream_no_response drop path.
			},
		}
		recorded := newRecordingObserver()
		proc.Dispatcher().SetObserver(recorded)

		Expect(proc.Process(stream)).To(Succeed())
		Expect(recorded.lastRequestID).NotTo(BeEmpty(),
			"request ID should be synthesized when upstream didn't set x-request-id")
		Expect(recorded.lastRequestID).To(HavePrefix("extproc-"))
	})

	It("observes Content-Length in onRequestHeaders before any body message arrives", func() {
		// Headers-only stream: no RequestBody frame ever exists, so a nonzero
		// histogram count can only have been observed at the headers phase.
		stream := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/v1/messages", "content-length": "12345"}),
			},
		}
		Expect(proc.Process(stream)).To(Succeed())

		txt := scrapeProcessorMetrics(proc)
		Expect(txt).To(ContainSubstring(`tapes_extproc_request_content_length_bytes_count{provider="anthropic"} 1`))
		Expect(txt).To(ContainSubstring(`tapes_extproc_request_content_length_bytes_sum{provider="anthropic"} 12345`))
	})

	It("counts absent and garbage Content-Length as unknown, with no histogram observation", func() {
		// Absent and unparseable headers both count as unknown; neither may
		// land in the histogram, not even as an Observe(0).
		absent := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/v1/messages"}),
			},
		}
		garbage := &fakeStream{
			ctx: context.Background(),
			toSend: []*extprocv3.ProcessingRequest{
				headerReq(map[string]string{":method": "POST", ":path": "/v1/messages", "content-length": "banana"}),
			},
		}
		Expect(proc.Process(absent)).To(Succeed())
		Expect(proc.Process(garbage)).To(Succeed())

		txt := scrapeProcessorMetrics(proc)
		Expect(txt).To(ContainSubstring(`tapes_extproc_request_content_length_unknown_total{provider="anthropic"} 2`))
		Expect(txt).NotTo(ContainSubstring(`tapes_extproc_request_content_length_bytes_count{provider="anthropic"}`))
	})

	It("reserves no reqBuf memory from a Content-Length before any body arrives", func() {
		// The header phase must not eagerly allocate on an unverified
		// Content-Length: buffers grow lazily from real bytes, bounded by the
		// accumulation budget, so a header-only stream costs nothing.
		lying := &streamState{}
		proc.onRequestHeaders(lying, headerReq(map[string]string{
			":method": "POST", ":path": "/v1/messages", "content-length": "99999999999",
		}).GetRequestHeaders())
		Expect(lying.reqBuf.Cap()).To(Equal(0))
	})

	It("over-budget request: acks every chunk, records exactly one request_over_budget, and never POSTs to ingest", func() {
		proc.Dispatcher().SetObserver(teeObserver{a: obs, b: proc.Metrics().AsObserver()})
		// 2 MiB chunks totalling just past requestCaptureBudget (~45.67 MiB),
		// so the buffer trips over budget mid-stream while every chunk is acked.
		const chunkSize = 2 << 20
		chunk := bytes.Repeat([]byte("a"), chunkSize)
		chunks := requestCaptureBudget/chunkSize + 1
		toSend := make([]*extprocv3.ProcessingRequest, 0, chunks+3)
		toSend = append(toSend, headerReq(map[string]string{
			":method": "POST", ":path": "/v1/messages",
			"content-length": strconv.Itoa(chunks * len(chunk)),
		}))
		for i := range chunks {
			toSend = append(toSend, reqBodyReq(chunk, i == chunks-1))
		}
		toSend = append(toSend,
			respHeaderReq("200", "application/json"),
			respBodyReq([]byte(`{}`), true),
		)
		stream := &fakeStream{ctx: context.Background(), toSend: toSend}

		Expect(proc.Process(stream)).To(Succeed())

		acks := 0
		var modeOverride *processingmodev3.ProcessingMode
		for _, r := range stream.Responses() {
			if rb := r.GetRequestBody(); rb != nil {
				acks++
				if r.ModeOverride != nil {
					modeOverride = r.ModeOverride
				}
			}
		}
		Expect(acks).To(Equal(chunks))

		// Forwarding must not degrade because capture was shed: the response
		// still flips to FULL_DUPLEX_STREAMED so a large streaming response is
		// never buffered on account of the over-budget request.
		Expect(modeOverride).NotTo(BeNil())
		Expect(modeOverride.ResponseBodyMode).To(Equal(processingmodev3.ProcessingMode_FULL_DUPLEX_STREAMED))

		Expect(obs.DropCount(DropRequestOverBudget)).To(Equal(1))
		Expect(obs.accepted.Load()).To(Equal(int32(0)))
		Consistently(func() any { return ingestBody.Load() }).
			WithTimeout(100 * time.Millisecond).
			Should(BeNil())

		txt := scrapeProcessorMetrics(proc)
		Expect(txt).To(ContainSubstring(`tapes_extproc_turns_dropped_total{provider="anthropic",reason="request_over_budget"} 1`))
	})

	It("sheds a compressed body that decodes past the budget as request_over_budget", func() {
		proc.Dispatcher().SetObserver(teeObserver{a: obs, b: proc.Metrics().AsObserver()})
		// Encoded bytes fit the accumulation budget; decoded bytes do not.
		// The envelope carries the decoded request, so this must shed rather
		// than marshal a turn ingest would 413.
		decoded := bytes.Repeat([]byte("a"), requestCaptureBudget+(1<<20))
		enc, err := zstd.NewWriter(nil)
		Expect(err).NotTo(HaveOccurred())
		compressed := enc.EncodeAll(decoded, nil)
		Expect(enc.Close()).To(Succeed())
		Expect(len(compressed)).To(BeNumerically("<", requestCaptureBudget))

		stream := &fakeStream{ctx: context.Background(), toSend: []*extprocv3.ProcessingRequest{
			headerReq(map[string]string{
				":method": "POST", ":path": "/v1/messages",
				"content-encoding": "zstd", "content-length": strconv.Itoa(len(compressed)),
			}),
			reqBodyReq(compressed, true),
			respHeaderReq("200", "application/json"),
			respBodyReq([]byte(`{}`), true),
		}}

		Expect(proc.Process(stream)).To(Succeed())

		Expect(obs.DropCount(DropRequestOverBudget)).To(Equal(1))
		Expect(obs.accepted.Load()).To(Equal(int32(0)))
		Consistently(func() any { return ingestBody.Load() }).
			WithTimeout(100 * time.Millisecond).
			Should(BeNil())
	})

	It("keeps the headers-phase ProcessingResponse byte-identical regardless of Content-Length", func() {
		// Observation must not alter forwarding: with and without a huge
		// Content-Length, the headers response and dispatch outcome match.
		body := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}`)
		reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)
		turnStream := func(hdrs map[string]string) *fakeStream {
			return &fakeStream{
				ctx: context.Background(),
				toSend: []*extprocv3.ProcessingRequest{
					headerReq(hdrs),
					reqBodyReq(reqBody, true),
					respHeaderReq("200", "application/json"),
					respBodyReq(body, true),
				},
			}
		}
		without := turnStream(map[string]string{":method": "POST", ":path": "/v1/messages"})
		with := turnStream(map[string]string{":method": "POST", ":path": "/v1/messages", "content-length": "99999999999"})

		Expect(proc.Process(without)).To(Succeed())
		Eventually(obs.accepted.Load).WithTimeout(2 * time.Second).Should(Equal(int32(1)))
		Expect(proc.Process(with)).To(Succeed())
		Eventually(obs.accepted.Load).WithTimeout(2 * time.Second).Should(Equal(int32(2)))
		for _, reason := range AllDropReasons() {
			Expect(obs.DropCount(reason)).To(BeZero(), "unexpected drop %q", reason)
		}

		withoutFirst, err := proto.Marshal(without.Responses()[0])
		Expect(err).NotTo(HaveOccurred())
		withFirst, err := proto.Marshal(with.Responses()[0])
		Expect(err).NotTo(HaveOccurred())
		Expect(withFirst).To(Equal(withoutFirst))
	})

	It("enumerates every DropReason exactly once so metric label rows stay closed", func() {
		reasons := AllDropReasons()
		seen := map[DropReason]bool{}
		for _, r := range reasons {
			Expect(seen[r]).To(BeFalse(), "duplicate reason %q", r)
			seen[r] = true
			Expect(strings.TrimSpace(string(r))).To(Equal(string(r)), "reason %q has whitespace", r)
		}
		Expect(len(reasons)).To(BeNumerically(">=", 8))
	})
})
