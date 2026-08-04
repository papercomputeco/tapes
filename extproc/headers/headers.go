// Package headers names the HTTP / ext_proc header values that
// tapes-extproc reads from the upstream traffic Envoy hands it.
//
// extproc is a passive ext_proc observer — it doesn't issue outbound
// HTTP requests, so it has no header-copying rules to encode (unlike
// the tapes proxy's proxy/header package, which manages
// client→proxy→upstream header forwarding in both directions). What
// extproc does have is a small set of header names that the
// state-machine reads at different phases of every turn, plus the
// rationale for each. Centralizing both here keeps string literals
// out of processor.go and gives any future header-shaped behavior
// change a single named site to land at instead of three scattered
// reads.
package headers

import (
	"strconv"
	"strings"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

// HTTP/2 pseudo-headers. Per RFC 9113 §8.3 these are lowercase on the
// wire; Envoy preserves that case when serializing into the ext_proc
// HeaderValue list. Get() is case-insensitive so the constants here
// are written in the form that minimizes surprise when grep'd
// alongside tcpdump or Envoy access logs.
const (
	Status = ":status"
	Method = ":method"
	Path   = ":path"
)

// harnessIDUnknown is the harness id substituted when the header is absent
// or empty. It is a wire-visible value — it reaches ingest and is stored on
// the session — so it is a contract, not a placeholder.
const harnessIDUnknown = "unknown"

// Standard request/response headers that extproc reads.
const (
	// ContentType drives the reducer's reduceStream vs. reduceOneShot
	// dispatch — text/event-stream takes the SSE path, anything else
	// (typically application/json) takes the one-shot JSON path.
	ContentType = "content-type"

	// ContentEncoding gates request- and response-body decompression.
	// Reducers parse textual JSON / SSE, so any non-identity encoding
	// must be undone before captured bytes reach parsing or ingest.
	// If a new encoding shows up, this is the single read site to audit.
	ContentEncoding = "content-encoding"
)

// Extension headers (x-*) used by adjacent components.
const (
	// AgentName tags the captured turn with the caller's agent
	// identity (Claude Code, opencode, etc.). Optional; tapes-ingest
	// stores it on the TurnEnvelope but does not gate on it.
	AgentName = "x-tapes-agent-name"

	// AIGSelectedBackend is the Envoy AI Gateway's per-request
	// indication of which AIServiceBackend it selected. extproc
	// prefers it for provider resolution because it survives path
	// rewrites and unusual routing configurations.
	AIGSelectedBackend = "x-ai-eg-selected-backend"

	// RequestID is the per-request correlation handle Envoy and
	// downstream logs use. extproc echoes it on every drop / accept
	// log line so an operator can join across components.
	RequestID = "x-request-id"
)

// Session-tracking envelope headers. Lower-cased to match how Envoy
// hands them to ext_proc (HTTP/2 normalizes header names to lowercase
// on the wire). All members of this set are stripped from the request
// before it reaches the upstream LLM provider via the prefix-based
// match on TapesEnvelopePrefix.
const (
	// TapesEnvelopePrefix is the common prefix for every session
	// envelope header. envelope-stripping uses prefix matching so
	// new optional members (forward compatibility) drop out of the
	// upstream request automatically.
	TapesEnvelopePrefix = "x-tapes-"

	// TapesHarnessID identifies the harness flavor. Missing or empty
	// is parsed as harnessIDUnknown.
	TapesHarnessID = "x-tapes-harness-id"

	// TapesHarnessSessionID is the harness's session id (a UUID for
	// claude).
	TapesHarnessSessionID = "x-tapes-harness-session-id"

	// TapesHarnessVersion is opaque (e.g. claude version string).
	TapesHarnessVersion = "x-tapes-harness-version"

	// TapesCwd is the harness's working directory. UTF-8 path.
	TapesCwd = "x-tapes-cwd"

	// TapesSessionName is the user-given session label. Percent-
	// encoded UTF-8 on the wire (RFC 3986).
	TapesSessionName = "x-tapes-session-name"

	// TapesParentHarnessSessionID is the fork parent's harness session
	// id, when known. Same id-space as TapesHarnessSessionID.
	TapesParentHarnessSessionID = "x-tapes-parent-harness-session-id"

	// TapesHarnessMetadata is a base64url-encoded JSON object with
	// harness-specific metadata. The parser accepts whatever arrived
	// (size-wise) and surfaces malformed payloads as a non-fatal
	// drop of the field.
	TapesHarnessMetadata = "x-tapes-harness-metadata"
)

// harnessThreadIDHeaders maps each harness's native sub-thread header
// onto the capture-side thread id. A harness that runs subagents fires
// their API calls with a per-thread identifier — Claude Code stamps
// x-claude-code-agent-id on every call made from a subagent context
// (including its security-monitor checks) and omits it on the main
// thread. Capturing it makes thread attribution DETERMINISTIC at
// capture time instead of recovered by content joins downstream.
//
// The list is ordered; first present header wins. Add other harnesses'
// equivalents here as they're identified — the rest of the pipeline is
// harness-neutral and only sees the resolved thread_id. Codex is NOT a
// member of this list: its sub-thread signal is a header *pair* (see
// the Codex constants below), resolved separately in ThreadID.
var harnessThreadIDHeaders = []string{
	"x-claude-code-agent-id",
}

// Codex's native identity headers. Unlike Claude Code, Codex stamps
// thread-id on EVERY call — root turns carry thread-id == session-id,
// and only spawned sub-thread (child) turns carry a distinct thread-id.
// So presence alone doesn't mean "subagent"; the root guard in ThreadID
// compares the pair and resolves root turns to "". Getting this wrong
// is not cosmetic: a non-empty thread_id on a root turn misroutes the
// root spine into tapes derive's threadCall path and silently degrades
// the session's derived status (terminalMainSpan requires ThreadID=="").
const (
	// CodexSessionID is the Codex harness's root session id, present
	// on every Codex call.
	CodexSessionID = "session-id"

	// CodexThreadID is the Codex harness's thread id for this call:
	// equal to session-id on root turns, a distinct id on sub-thread
	// (spawned agent) turns.
	CodexThreadID = "thread-id"
)

// ThreadID resolves the harness-native sub-thread id for this request,
// or "" for a main-thread call (or a harness with no known mapping).
func ThreadID(hdrs *extprocv3.HttpHeaders) string {
	for _, name := range harnessThreadIDHeaders {
		if v := Get(hdrs, name); v != "" {
			return v
		}
	}
	// Codex: sub-thread iff the thread-id / session-id pair diverges.
	// Both must be present — a lone thread-id without its session-id
	// counterpart isn't a recognized Codex shape, and treating it as a
	// sub-thread would risk misrouting root spines (see the root-guard
	// comment on the Codex constants above).
	if tid := Get(hdrs, CodexThreadID); tid != "" {
		if sid := Get(hdrs, CodexSessionID); sid != "" && tid != sid {
			return tid
		}
	}
	return ""
}

// Server-trusted identity headers, populated by the upstream gateway
// from validated JWT claims (Envoy's claim_to_headers feature). They
// are NOT part of the X-Tapes-* envelope and must not be sent by
// clients — that constraint is the gateway's responsibility to
// enforce. If the gateway is not configured to populate them, the
// fields arrive empty and propagate through to the dispatched
// session envelope as empty strings.
const (
	// PaperAuthOrgID carries the `org_id` JWT claim (the WorkOS token
	// names it org_id, not org).
	PaperAuthOrgID = "x-paper-auth-org-id"

	// PaperAuthSubject carries the `sub` JWT claim — the WorkOS user id,
	// persisted as the captured session's auth_subject.
	PaperAuthSubject = "x-paper-auth-subject"
)

// Get returns the value of the named header from HttpHeaders,
// case-insensitive. Returns "" when not present. Prefers RawValue
// (the bytes Envoy received over the wire) and falls back to Value
// for older Envoy versions that didn't populate RawValue.
func Get(hdrs *extprocv3.HttpHeaders, name string) string {
	for _, h := range hdrs.GetHeaders().GetHeaders() {
		if strings.EqualFold(h.GetKey(), name) {
			if rv := h.GetRawValue(); len(rv) > 0 {
				return string(rv)
			}
			return h.GetValue()
		}
	}
	return ""
}

// StatusCode returns the parsed :status pseudo-header as an int.
// Returns 0 when the header is missing or unparseable — the caller is
// expected to treat 0 as "status unknown" and drop the turn rather
// than implying a 200.
func StatusCode(hdrs *extprocv3.HttpHeaders) int {
	code, _ := strconv.Atoi(Get(hdrs, Status))
	return code
}
