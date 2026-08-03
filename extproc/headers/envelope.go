// envelope.go parses the X-Tapes-* session envelope headers off an
// inbound LLM request. The parser is intentionally non-fatal: a
// malformed metadata blob or session-name does not fail the request,
// it just lands as the empty/raw value on the captured turn.
// tapes-ingest is the validation surface; this layer just extracts
// what it can.

package headers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

// SessionEnvelope is the parsed view of the X-Tapes-* headers on
// the inbound request. Field names match the JSON-on-the-wire names
// so the processor can hand the struct straight to the dispatcher
// with no further translation.
//
// Present == true means at least one X-Tapes-* header was observed
// on the inbound request, including the case where only HarnessID
// was present. Present == false means no X-Tapes-* header arrived;
// the dispatcher must omit the entire session block from the
// envelope POST in that case.
//
// HarnessID is always populated when Present == true: missing or
// empty harness-id is treated as "unknown" rather than rejected,
// so the parser substitutes "unknown" when the header is absent.
type SessionEnvelope struct {
	Present bool

	HarnessID                string
	HarnessSessionID         string
	HarnessVersion           string
	Cwd                      string
	Name                     string
	ParentHarnessSessionID   string
	HarnessMetadata          map[string]any
	HarnessMetadataMalformed bool
}

// ParseSessionEnvelope reads every session-envelope header from
// hdrs. Returns a zero SessionEnvelope (Present=false) when no
// X-Tapes-* header is observed at all.
//
// Presence detection uses the open TapesEnvelopePrefix (any
// `x-tapes-*` header flips Present=true), mirroring the strip path
// in EnvelopeHeaderKeysFromRequest. This keeps dispatch and strip
// symmetric: a future forward-compat envelope header (e.g.
// `x-tapes-trace-id`) gets both stripped AND dispatched, instead
// of stripped-but-silently-dropped from the ingest POST.
//
// The pre-existing X-Tapes-Agent-Name header (see AgentName in
// headers.go) shares the x-tapes-* prefix and therefore also flips
// Present=true.
func ParseSessionEnvelope(hdrs *extprocv3.HttpHeaders) SessionEnvelope {
	out := SessionEnvelope{}

	for _, h := range hdrs.GetHeaders().GetHeaders() {
		if strings.HasPrefix(strings.ToLower(h.GetKey()), TapesEnvelopePrefix) {
			out.Present = true
			break
		}
	}
	if !out.Present {
		return out
	}

	// Missing harness-id is treated as "unknown" rather than
	// rejected. Empty string is the same as missing.
	out.HarnessID = Get(hdrs, TapesHarnessID)
	if out.HarnessID == "" {
		out.HarnessID = "unknown"
	}

	out.HarnessSessionID = Get(hdrs, TapesHarnessSessionID)
	out.HarnessVersion = Get(hdrs, TapesHarnessVersion)
	out.ParentHarnessSessionID = Get(hdrs, TapesParentHarnessSessionID)

	if raw := Get(hdrs, TapesCwd); raw != "" {
		out.Cwd = decodeEnvelopeHeaderValue("cwd", raw)
	}

	if raw := Get(hdrs, TapesSessionName); raw != "" {
		out.Name = decodeEnvelopeHeaderValue("session-name", raw)
	}

	if raw := Get(hdrs, TapesHarnessMetadata); raw != "" {
		md, err := decodeMetadata(raw)
		switch {
		case err != nil:
			// Malformed metadata is non-fatal: drop the field from the
			// dispatched envelope (the nil map flows through
			// buildSessionEnvelope and json.Marshal omits the key via
			// omitempty), flag it, and continue. The header name is
			// logged so an operator grepping for "extproc:
			// harness-metadata decode failed" can identify which
			// header tripped the decoder.
			slog.Warn("extproc: harness-metadata decode failed",
				"header", TapesHarnessMetadata,
				"error", err,
				"raw_len", len(raw),
			)
			out.HarnessMetadataMalformed = true
		default:
			out.HarnessMetadata = md
		}
	}

	return out
}

// decodeEnvelopeHeaderValue turns one percent-encoded session-envelope
// header value into the logical value that gets stored. cwd and
// session-name share it because they share the contract.
//
// The contract: the stored envelope value is the *logical* value.
// Paths on macOS/Linux can contain non-ASCII bytes that RFC 7230
// forbids in raw header values, so the emitter escapes them —
// percent-encoding is transport framing that exists only to survive
// the header hop, and it dies here. Storing the escaped form would
// leak wire encoding into the data model and force a decoder into
// every consumer.
//
// PathUnescape matches the RFC 3986 path-segment contract the emitter
// applies; QueryUnescape would corrupt a literal `+` into a space.
//
// The emitter escapes control bytes so that no second header can be
// smuggled across the header hop. Decoding hands that byte back, so
// the injection defense moves from representation to validation: a
// decoded value carrying any C0 control (< 0x20) or DEL (0x7F) is
// refused outright — logged and dropped to empty — rather than passed
// on raw. Refusing beats sanitizing, because a path that needed a
// control byte to be expressed is not a path anyone meant to record.
//
// Malformed encoding stays non-fatal — fall back to the raw value so
// the row still records *something* recognisable. It passes the same
// control-byte gate, so no path through this function returns a
// control byte.
//
// The tapes reader (pkg/backfill.sessionEnvelopeFromHeaders) applies
// the identical transform; the shared fixture corpus under
// testdata/envelope is what proves the two stay interchangeable.
func decodeEnvelopeHeaderValue(field, raw string) string {
	value := raw
	if decoded, err := url.PathUnescape(raw); err == nil {
		value = decoded
	} else {
		slog.Warn("extproc: envelope header percent-decode failed; using raw value",
			"field", field,
			"error", err,
			"raw_len", len(raw),
		)
	}
	if hasControlRune(value) {
		// Log the length, never the value: the point of the guard is to
		// keep raw control bytes out of everything downstream, and a log
		// sink is downstream.
		slog.Warn("extproc: envelope header contains control bytes after decoding; dropping the value",
			"field", field,
			"decoded_len", len(value),
		)
		return ""
	}
	return value
}

// hasControlRune reports whether s contains a C0 control character
// (< 0x20) or DEL (0x7F) — the runes that could forge header structure
// if a stored value were ever re-emitted into a header-shaped context.
func hasControlRune(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool {
		return r < 0x20 || r == 0x7F
	})
}

// decodeMetadata parses a base64url(no-pad)-encoded JSON object — the
// one alphabet the contract declares, and the only one the tapes
// reader accepts. This parser used to fall back through padded and
// std alphabets "so emitter changes don't break decoding", which is
// exactly how identical wire bytes came to store metadata through this
// path and drop it through the other; permissive-parser drift is how
// those divergences are born. Returns an error when the bytes don't
// decode or don't parse to a JSON object.
func decodeMetadata(raw string) (map[string]any, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("harness metadata is not base64url(no-pad): %w", err)
	}

	// Unmarshal into map[string]any (not bare any) so non-object
	// payloads — arrays, scalars, null — fail at this gate. The
	// dispatched wire shape declares this field as a JSON object;
	// rejecting non-objects here keeps that contract intact rather
	// than handing tapes-ingest a surprising shape. The caller
	// treats this error the same as a base64 failure: flag
	// malformed, drop the field.
	var obj map[string]any
	if jerr := json.Unmarshal(decoded, &obj); jerr != nil {
		return nil, jerr
	}
	return obj, nil
}

// EnvelopeHeaderKeysFromRequest scans hdrs for every header whose name
// starts with TapesEnvelopePrefix and returns the original keys (the
// case-as-received form). The processor passes the returned slice to
// Envoy as a HeaderMutation.remove_headers list so the entire envelope
// is stripped before the request reaches the upstream provider,
// including any forward-compatibility x-tapes-* additions that may
// arrive in the future.
//
// Returned keys preserve the case Envoy delivered to ext_proc. Header
// removal in Envoy is case-insensitive, but echoing the as-received
// form makes operator-side packet captures grep cleanly when
// diagnosing why a header did or didn't get stripped.
func EnvelopeHeaderKeysFromRequest(hdrs *extprocv3.HttpHeaders) []string {
	var keys []string
	for _, h := range hdrs.GetHeaders().GetHeaders() {
		k := h.GetKey()
		// Envoy normalizes to lowercase, but be defensive against
		// any path that hands ext_proc the original case.
		if strings.HasPrefix(strings.ToLower(k), TapesEnvelopePrefix) {
			keys = append(keys, k)
		}
	}
	return keys
}
