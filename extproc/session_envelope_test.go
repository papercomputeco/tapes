package extproc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// sessionEnvelopeFixture builds a minimal but well-formed turn that
// dispatches to the fake ingest, with a configurable header map for the
// RequestHeaders frame. Returns the processor, observer, the captured
// ingest body, and the captured stream responses so individual
// assertions don't need to repeat the wiring.
//
// Kept here (rather than in processor_test.go) so the session-tracking
// surface area is one reviewable diff and the existing test file's
// fakeStream / observer helpers are exercised verbatim.
func runWithRequestHeaders(reqHeaders map[string]string) (*observer, []byte, []*extprocv3.ProcessingResponse) {
	GinkgoHelper()

	var ingestBody atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ingestBody.Store(b)
		w.WriteHeader(http.StatusAccepted)
	}))
	DeferCleanup(srv.Close)

	proc, err := NewProcessor(Config{
		IngestURL:   srv.URL,
		MaxInflight: 4,
	})
	Expect(err).NotTo(HaveOccurred())

	obs := newObserver()
	proc.Dispatcher().SetObserver(obs)

	// Standard well-formed Anthropic turn — borrowed from processor_test.go's
	// one-shot test so the dispatch path runs end-to-end and ingest receives
	// a body we can inspect for the session block.
	body := []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}`)
	reqBody := []byte(`{"model":"claude-3-5-sonnet-20241022","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)

	// Default to :path so the request is recognized as a turn.
	if _, ok := reqHeaders[":path"]; !ok {
		reqHeaders[":path"] = "/v1/messages"
	}

	stream := &fakeStream{
		ctx: context.Background(),
		toSend: []*extprocv3.ProcessingRequest{
			headerReq(reqHeaders),
			reqBodyReq(reqBody, true),
			respHeaderReq("200", "application/json"),
			respBodyReq(body, true),
		},
	}

	Expect(proc.Process(stream)).To(Succeed())

	Eventually(func() int32 { return obs.accepted.Load() }).
		WithTimeout(2 * time.Second).
		Should(Equal(int32(1)))

	var captured []byte
	Eventually(func() any { return ingestBody.Load() }).
		WithTimeout(2 * time.Second).
		ShouldNot(BeNil())
	captured = ingestBody.Load().([]byte)

	return obs, captured, stream.Responses()
}

// requestHeadersMutation extracts the HeaderMutation.RemoveHeaders list
// from the RequestHeaders phase of stream.Responses(). Returns nil if
// the processor sent back a plain ack (no mutation).
func requestHeadersMutation(resps []*extprocv3.ProcessingResponse) []string {
	GinkgoHelper()
	for _, r := range resps {
		rh, ok := r.Response.(*extprocv3.ProcessingResponse_RequestHeaders)
		if !ok {
			continue
		}
		cr := rh.RequestHeaders.GetResponse()
		if cr == nil {
			return nil
		}
		hm := cr.GetHeaderMutation()
		if hm == nil {
			return nil
		}
		return hm.GetRemoveHeaders()
	}
	Fail("no RequestHeaders response observed")
	return nil
}

var _ = Describe("Session envelope", func() {
	Context("envelope stripping on RequestHeaders", func() {
		It("removes every X-Tapes-* header before forwarding upstream", func() {
			_, _, resps := runWithRequestHeaders(map[string]string{
				"x-tapes-harness-id":                "claude",
				"x-tapes-harness-session-id":        "db822441-baa9-4083-b5c4-e1bdcedd7d3f",
				"x-tapes-harness-version":           "2.1.145",
				"x-tapes-cwd":                       "/Users/matt/git/foo",
				"x-tapes-session-name":              "fix%20it",
				"x-tapes-parent-harness-session-id": "580620c8",
				"x-tapes-harness-metadata":          base64.RawURLEncoding.EncodeToString([]byte(`{}`)),
				"x-tapes-agent-name":                "claude-code",
				"x-tapes-future-unknown":            "forward-compat",
			})

			removed := requestHeadersMutation(resps)
			sort.Strings(removed)

			Expect(removed).To(ContainElements(
				"x-tapes-harness-id",
				"x-tapes-harness-session-id",
				"x-tapes-harness-version",
				"x-tapes-cwd",
				"x-tapes-session-name",
				"x-tapes-parent-harness-session-id",
				"x-tapes-harness-metadata",
				"x-tapes-agent-name",
				"x-tapes-future-unknown",
			), "every X-Tapes-* header (incl. forward-compat additions) must be on the remove list")

			// Defensive: nothing OUTSIDE the envelope should be removed.
			for _, k := range removed {
				Expect(strings.HasPrefix(strings.ToLower(k), "x-tapes-")).To(BeTrue(),
					"non-envelope header %q must not be on the remove list", k)
			}
		})

		It("sends a plain ack with no HeaderMutation when no envelope is present", func() {
			// Caller sent no X-Tapes-* header (e.g., a curl with no
			// envelope).
			_, _, resps := runWithRequestHeaders(map[string]string{})

			mut := requestHeadersMutation(resps)
			Expect(mut).To(BeNil(), "no envelope → no HeaderMutation, preserving the pre-session behavior")
		})
	})

	Context("ingest POST envelope shape", func() {
		It("attaches the session block verbatim when the X-Tapes-* envelope is present", func() {
			meta := []byte(`{"ai_title":"hello","kind":"interactive"}`)
			_, captured, _ := runWithRequestHeaders(map[string]string{
				"x-tapes-harness-id":                "claude",
				"x-tapes-harness-session-id":        "db822441-baa9-4083-b5c4-e1bdcedd7d3f",
				"x-tapes-harness-version":           "2.1.145",
				"x-tapes-cwd":                       "/Users/matt/git/foo",
				"x-tapes-session-name":              "fix%20my%20bug",
				"x-tapes-parent-harness-session-id": "580620c8-0074-4bc4-85a2-90caa8e7f498",
				"x-tapes-harness-metadata":          base64.RawURLEncoding.EncodeToString(meta),
				"x-paper-auth-org-id":               "0193c5e8-1111-7777-aaaa-bbbbbbbbbbbb",
				"x-paper-auth-subject":              "user_01HZJK3FG",
			})

			var env struct {
				Session *DispatchedSessionEnvelope `json:"session"`
			}
			Expect(json.Unmarshal(captured, &env)).To(Succeed())
			Expect(env.Session).NotTo(BeNil(), "session block must be present when X-Tapes-* arrived")

			s := env.Session
			Expect(s.OrgID).To(Equal("0193c5e8-1111-7777-aaaa-bbbbbbbbbbbb"))
			Expect(s.AuthSubject).To(Equal("user_01HZJK3FG"))
			Expect(s.HarnessID).To(Equal("claude"))
			Expect(s.HarnessSessionID).To(Equal("db822441-baa9-4083-b5c4-e1bdcedd7d3f"))
			Expect(s.HarnessVersion).To(Equal("2.1.145"))
			Expect(s.Cwd).To(Equal("/Users/matt/git/foo"))
			Expect(s.Name).To(Equal("fix my bug"), "session-name must be percent-decoded")
			Expect(s.ParentHarnessSessionID).To(Equal("580620c8-0074-4bc4-85a2-90caa8e7f498"))
			Expect(s.HarnessMetadata).To(HaveKeyWithValue("ai_title", "hello"))
			Expect(s.HarnessMetadata).To(HaveKeyWithValue("kind", "interactive"))
		})

		It("attaches the session block when only the pre-existing x-tapes-agent-name is present", func() {
			// x-tapes-agent-name shares the envelope prefix. The
			// open-prefix presence rule says: any x-tapes-* header on
			// the inbound request flips the session block on,
			// including agent-name alone — that keeps dispatch and
			// strip symmetric (both gated by the same prefix) and
			// forward-compatible with future envelope additions.
			_, captured, _ := runWithRequestHeaders(map[string]string{
				"x-tapes-agent-name":   "claude-code",
				"x-paper-auth-org-id":  "org",
				"x-paper-auth-subject": "sub",
			})

			var env struct {
				Session *DispatchedSessionEnvelope `json:"session"`
			}
			Expect(json.Unmarshal(captured, &env)).To(Succeed())
			Expect(env.Session).NotTo(BeNil(),
				"any x-tapes-* header (including agent-name) must trigger the session block")
			// HarnessID defaults to "unknown" because no harness-id
			// header arrived; agent-name carries no harness identity.
			Expect(env.Session.HarnessID).To(Equal("unknown"))
		})

		It("omits the session block entirely when no x-tapes-* header was sent", func() {
			// Caller sent neither the X-Tapes-* envelope nor the
			// older agent-name tag. The PaperAuth-* headers on their
			// own must not synthesize a session block; the X-Tapes-*
			// namespace is the trigger.
			_, captured, _ := runWithRequestHeaders(map[string]string{
				"x-paper-auth-org-id":  "org",
				"x-paper-auth-subject": "sub",
			})

			// Decode permissively into a map so the test asserts on
			// JSON key absence rather than struct presence — the
			// dispatcher's `omitempty` tag means the key simply isn't
			// emitted, and we want exactly that wire-level behavior.
			var asMap map[string]json.RawMessage
			Expect(json.Unmarshal(captured, &asMap)).To(Succeed())
			_, hasSession := asMap["session"]
			Expect(hasSession).To(BeFalse(),
				"ingest payload must omit `session` when no X-Tapes-* header arrived")
		})

		It("attaches the session block with HarnessID=unknown when the harness id header literally carries the string 'unknown'", func() {
			// Caller sends X-Tapes-Harness-Id=unknown and no
			// harness_session_id. The session block must still be
			// emitted so tapes-ingest sees the envelope.
			_, captured, _ := runWithRequestHeaders(map[string]string{
				"x-tapes-harness-id":   "unknown",
				"x-paper-auth-org-id":  "org-xyz",
				"x-paper-auth-subject": "sub-xyz",
			})

			var env struct {
				Session *DispatchedSessionEnvelope `json:"session"`
			}
			Expect(json.Unmarshal(captured, &env)).To(Succeed())
			Expect(env.Session).NotTo(BeNil())
			Expect(env.Session.HarnessID).To(Equal("unknown"))
			Expect(env.Session.HarnessSessionID).To(BeEmpty())
		})

		It("attaches the session block with HarnessID=unknown when only metadata was sent", func() {
			// Pathological but allowed: a future caller attaches
			// metadata without HarnessID. Missing HarnessID is
			// treated as "unknown" rather than rejected.
			_, captured, _ := runWithRequestHeaders(map[string]string{
				"x-tapes-harness-metadata": base64.RawURLEncoding.EncodeToString([]byte(`{"k":"v"}`)),
			})

			var env struct {
				Session *DispatchedSessionEnvelope `json:"session"`
			}
			Expect(json.Unmarshal(captured, &env)).To(Succeed())
			Expect(env.Session).NotTo(BeNil())
			Expect(env.Session.HarnessID).To(Equal("unknown"))
		})

		It("emits a session block with empty org_id/auth_subject when the gateway didn't populate JWT headers", func() {
			// If the upstream gateway isn't populating PaperAuthOrgID
			// / PaperAuthSubject, both arrive empty. The block must
			// STILL be emitted (the X-Tapes-* envelope is present)
			// with empty strings on those fields; the empty-strings
			// shape is a deliberate signal, not an accident.
			_, captured, _ := runWithRequestHeaders(map[string]string{
				"x-tapes-harness-id":         "claude",
				"x-tapes-harness-session-id": "abc",
			})

			var env struct {
				Session *DispatchedSessionEnvelope `json:"session"`
			}
			Expect(json.Unmarshal(captured, &env)).To(Succeed())
			Expect(env.Session).NotTo(BeNil())
			Expect(env.Session.OrgID).To(BeEmpty())
			Expect(env.Session.AuthSubject).To(BeEmpty())
			Expect(env.Session.HarnessID).To(Equal("claude"))
		})

		It("emits an org_id/auth_subject pair on the session block when JWT-derived headers are present", func() {
			_, captured, _ := runWithRequestHeaders(map[string]string{
				"x-tapes-harness-id":   "claude",
				"x-paper-auth-org-id":  "org-trusted",
				"x-paper-auth-subject": "auth0|user",
			})
			var env struct {
				Session *DispatchedSessionEnvelope `json:"session"`
			}
			Expect(json.Unmarshal(captured, &env)).To(Succeed())
			Expect(env.Session).NotTo(BeNil())
			Expect(env.Session.OrgID).To(Equal("org-trusted"))
			Expect(env.Session.AuthSubject).To(Equal("auth0|user"))
		})
	})

	Context("no response stamping", func() {
		It("does not emit a HeaderMutation on the ResponseHeaders phase", func() {
			// Belt-and-braces against accidentally regressing into
			// stamping a session id back onto the response. The
			// ResponseHeaders frame must be a plain ack, identical
			// to the pre-session behavior.
			_, _, resps := runWithRequestHeaders(map[string]string{
				"x-tapes-harness-id":         "claude",
				"x-tapes-harness-session-id": "abc",
			})
			var found bool
			for _, r := range resps {
				rh, ok := r.Response.(*extprocv3.ProcessingResponse_ResponseHeaders)
				if !ok {
					continue
				}
				found = true
				Expect(rh.ResponseHeaders.GetResponse()).To(BeNil(),
					"extproc must not mutate response headers")
			}
			Expect(found).To(BeTrue(), "test must observe a ResponseHeaders response")
		})
	})
})
