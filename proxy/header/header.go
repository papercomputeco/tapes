// Package header provides header filtering for the tapes proxy.
//
// This proxy sits between a client and an upstream LLM provider like so:
//
//	Client <--> Proxy <--> Upstream LLM Provider
//
// and headers are handled accordingly as each leg negotiates compression, hops,
// encoding, etc. independently.
package header

import (
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// Handler manages headers between proxy connections.
type Handler struct{}

// NewHandler creates a new header Handler.
func NewHandler() *Handler {
	return &Handler{}
}

// AgentNameHeader is the optional header used to tag agent requests.
const AgentNameHeader = "X-Tapes-Agent-Name"

// ThreadIDHeaders maps each harness's native sub-thread header onto the
// capture-side thread id. A harness that runs subagents fires their API calls
// with a per-thread identifier — Claude Code stamps x-claude-code-agent-id on
// every call made from a subagent context (including its security-monitor
// checks) and omits it on the main thread. Capturing it makes thread
// attribution deterministic at capture time instead of recovered by content
// joins downstream.
//
// The list is ordered; first present header wins. Add other harnesses'
// equivalents here as they are identified — the rest of the pipeline is
// harness-neutral and only sees the resolved thread id. Codex is NOT a member
// of this list: its sub-thread signal is a header *pair* (see the Codex
// constants below), resolved separately in ThreadID.
//
// This must stay in step with extproc/headers' harnessThreadIDHeaders: the two
// are independent capture paths for the same traffic, and a header one records
// and the other doesn't is a fidelity gap that only shows up as subagent turns
// mis-attributed to the main thread. They stayed separate through the extproc
// fold because they read from different transports — a fiber.Ctx here, an
// ext_proc HeaderMap there — so only the list itself could ever be shared.
//
// These headers are NOT stripped on the way upstream. They are the harness's
// own, addressed to the model provider; tapes only observes them.
//
// Canonical home: tapes-harnesses src/envelope.rs (CLAUDE_THREAD_ID_HEADERS,
// and HARNESS_THREAD_ID_RULES for the rule shape). The shared corpus at
// fixtures/thread/ pins the spelling and resolution across every reader
// (thread_corpus_test.go here; the authored-home gate lives in
// extproc/headers).
var ThreadIDHeaders = []string{
	"x-claude-code-agent-id",
}

// Codex's native identity headers. Unlike Claude Code, Codex stamps thread-id
// on EVERY call — root turns carry thread-id == session-id, and only spawned
// sub-thread (child) turns carry a distinct thread-id. So presence alone
// doesn't mean "subagent"; the root guard in ThreadID compares the pair and
// resolves root turns to "". Getting this wrong is not cosmetic: a non-empty
// thread_id on a root turn misroutes the root spine into tapes derive's
// threadCall path and silently degrades the session's derived status
// (terminalMainSpan requires ThreadID=="").
//
// Canonical home: tapes-harnesses src/envelope.rs (CODEX_THREAD_ID_HEADER /
// CODEX_SESSION_ID_HEADER); the lifecycle counterpart of the same identities
// is src/attribution/codex_app (session_id = the root session, agent_id =
// the child thread). Pinned cross-language by fixtures/thread/.
const (
	// CodexSessionID is the Codex harness's root session id, present on
	// every Codex call.
	CodexSessionID = "session-id"

	// CodexThreadID is the Codex harness's thread id for this call: equal to
	// session-id on root turns, a distinct id on sub-thread (spawned agent)
	// turns.
	CodexThreadID = "thread-id"
)

// ThreadID resolves the harness-native sub-thread id for this request, or ""
// for a main-thread call (or a harness with no known mapping). Fiber's Get is
// case-insensitive, so the constants are written in the lowercase HTTP/2 form
// that matches a packet capture.
func ThreadID(c *fiber.Ctx) string {
	for _, name := range ThreadIDHeaders {
		if v := strings.TrimSpace(c.Get(name)); v != "" {
			return v
		}
	}
	// Codex: sub-thread iff the thread-id / session-id pair diverges. Both
	// must be present — a lone thread-id without its session-id counterpart
	// isn't a recognized Codex shape, and treating it as a sub-thread would
	// risk misrouting root spines (see the root-guard comment on the Codex
	// constants above).
	if tid := strings.TrimSpace(c.Get(CodexThreadID)); tid != "" {
		if sid := strings.TrimSpace(c.Get(CodexSessionID)); sid != "" && tid != sid {
			return tid
		}
	}
	return ""
}

// skipRequest is the set of request headers (client --> proxy --> upstream)
// that are not forwarded to the upstream LLM provider.
var skipRequest = map[string]struct{}{
	// Hop-by-hop headers: only meaningful for a single transport-level connection.
	"Connection": {},

	// The Host header is rewritten by Go's http.Transport to match the
	// upstream URL. Forwarding the client's Host would confuse virtual-hosted
	// upstreams.
	"Host": {},

	// Accept-Encoding is stripped so that Go's http.Transport adds its own
	// "Accept-Encoding: gzip" and transparently decompresses the upstream
	// response.
	"Accept-Encoding": {},

	// Internal agent routing header.
	AgentNameHeader: {},
}

// skipResponse is the set of upstream response headers (client <-- proxy <-- upstream)
// that are not copied back to the downstream client.
var skipResponse = map[string]struct{}{
	// Hop-by-hop headers: only meaningful for a single transport-level connection.
	"Connection": {},

	// Hop-by-hop headers: fasthttp manages chunked transfer encoding for the
	// client-facing response independently.
	"Transfer-Encoding": {},

	// The proxy always reads a decompressed body (Go's http.Transport strips
	// Content-Encoding after auto-decompression). Forwarding a stale
	// Content-Encoding would claim an encoding the body no longer has.
	// Fiber's compress middleware sets the correct Content-Encoding when it
	// re-compresses the response back down to the client.
	"Content-Encoding": {},

	// The upstream Content-Length reflects the (possibly compressed) upstream
	// body size. After decompression the length changes, and Fiber's compress
	// middleware may re-compress to a different size. Letting Fiber compute
	// the final Content-Length avoids sending an incorrect value.
	"Content-Length": {},
}

// SetUpstreamRequestHeaders copies request headers from the Fiber context to
// the outgoing http.Request, filtering headers that the proxy should not forward
// to the upstream API.
func (h *Handler) SetUpstreamRequestHeaders(c *fiber.Ctx, req *http.Request) {
	c.Request().Header.VisitAll(func(key, value []byte) {
		k := string(key)
		if _, skip := skipRequest[k]; !skip {
			req.Header.Set(k, string(value))
		}
	})
}

// SetClientResponseHeaders copies response headers from the upstream API
// http.Response to the Fiber context, filtering headers that the proxy should
// not forward back down to the client.
func (h *Handler) SetClientResponseHeaders(c *fiber.Ctx, resp *http.Response) {
	for k, v := range resp.Header {
		if _, skip := skipResponse[k]; !skip {
			c.Set(k, strings.Join(v, ", "))
		}
	}
}
