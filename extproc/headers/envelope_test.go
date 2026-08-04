package headers

import (
	"encoding/base64"
	"reflect"
	"sort"
	"strings"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

// mkEnvelopeHeaders is a tiny helper that turns a name→value map into
// the HttpHeaders shape ext_proc hands to ParseSessionEnvelope. Map
// iteration is randomized, but order doesn't matter for header lookup.
func mkEnvelopeHeaders(pairs map[string]string) *extprocv3.HttpHeaders {
	hm := &corev3.HeaderMap{}
	for k, v := range pairs {
		hm.Headers = append(hm.Headers, &corev3.HeaderValue{
			Key:      k,
			RawValue: []byte(v),
		})
	}
	return &extprocv3.HttpHeaders{Headers: hm}
}

// b64url encodes bytes as base64url-without-padding.
func b64url(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func TestParseSessionEnvelope_AbsentEnvelope(t *testing.T) {
	// No X-Tapes-* headers at all → Present must be false so the
	// dispatcher omits the session block entirely. Other namespaces
	// (e.g. x-paper-auth-*) must NOT trip presence detection.
	hdrs := mkEnvelopeHeaders(map[string]string{
		":path":               "/v1/messages",
		"x-paper-auth-org-id": "org-123",
	})
	got := ParseSessionEnvelope(hdrs)
	if got.Present {
		t.Fatalf("no x-tapes-* header should not mark envelope present: %+v", got)
	}
}

func TestParseSessionEnvelope_AgentNameAlonePresent(t *testing.T) {
	// x-tapes-agent-name shares the envelope prefix. The open-prefix
	// presence rule (any x-tapes-* header flips Present=true) asserts
	// that the parser and the strip path agree on the same prefix:
	// every x-tapes-* header is both stripped from the upstream
	// request AND surfaced on the dispatched session block.
	hdrs := mkEnvelopeHeaders(map[string]string{
		":path":              "/v1/messages",
		"x-tapes-agent-name": "claude-code",
	})
	got := ParseSessionEnvelope(hdrs)
	if !got.Present {
		t.Fatalf("any x-tapes-* header should flip Present=true: %+v", got)
	}
	// HarnessID still defaults to "unknown" because no harness-id
	// header arrived — agent-name carries no harness identity.
	if got.HarnessID != "unknown" {
		t.Errorf("HarnessID: got %q want %q", got.HarnessID, "unknown")
	}
}

func TestParseSessionEnvelope_WellFormedFull(t *testing.T) {
	meta := []byte(`{"ai_title":"hello world","kind":"interactive"}`)
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID:              "claude",
		TapesHarnessSessionID:       "db822441-baa9-4083-b5c4-e1bdcedd7d3f",
		TapesHarnessVersion:         "2.1.145",
		TapesCwd:                    "/Users/matt/git/foo",
		TapesSessionName:            "fix%20my%20bug", // percent-encoded "fix my bug"
		TapesParentHarnessSessionID: "580620c8-0074-4bc4-85a2-90caa8e7f498",
		TapesHarnessMetadata:        b64url(meta),
	})

	got := ParseSessionEnvelope(hdrs)

	if !got.Present {
		t.Fatalf("Present should be true when every envelope header is set")
	}
	if got.HarnessID != "claude" {
		t.Errorf("HarnessID: got %q want %q", got.HarnessID, "claude")
	}
	if got.HarnessSessionID != "db822441-baa9-4083-b5c4-e1bdcedd7d3f" {
		t.Errorf("HarnessSessionID: got %q", got.HarnessSessionID)
	}
	if got.HarnessVersion != "2.1.145" {
		t.Errorf("HarnessVersion: got %q", got.HarnessVersion)
	}
	if got.Cwd != "/Users/matt/git/foo" {
		t.Errorf("Cwd: got %q", got.Cwd)
	}
	if got.Name != "fix my bug" {
		t.Errorf("Name: percent-decode failed. got %q want %q", got.Name, "fix my bug")
	}
	if got.ParentHarnessSessionID != "580620c8-0074-4bc4-85a2-90caa8e7f498" {
		t.Errorf("ParentHarnessSessionID: got %q", got.ParentHarnessSessionID)
	}
	if got.HarnessMetadataMalformed {
		t.Errorf("metadata should have parsed cleanly")
	}
	if got.HarnessMetadata["ai_title"] != "hello world" {
		t.Errorf("metadata.ai_title: got %v", got.HarnessMetadata["ai_title"])
	}
	if got.HarnessMetadata["kind"] != "interactive" {
		t.Errorf("metadata.kind: got %v", got.HarnessMetadata["kind"])
	}
}

func TestParseSessionEnvelope_MissingHarnessIDDefaultsToUnknown(t *testing.T) {
	// Only the metadata header was attached. Missing HarnessID is
	// treated as "unknown" (NOT rejected) so the envelope still
	// parses.
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessMetadata: b64url([]byte(`{}`)),
	})
	got := ParseSessionEnvelope(hdrs)
	if !got.Present {
		t.Fatalf("Present must be true when any envelope header is set")
	}
	if got.HarnessID != "unknown" {
		t.Errorf("missing harness id should default to 'unknown', got %q", got.HarnessID)
	}
}

func TestParseSessionEnvelope_EmptyHarnessIDDefaultsToUnknown(t *testing.T) {
	// Header present but value empty: same treatment as missing.
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID:        "",
		TapesHarnessSessionID: "some-id",
	})
	got := ParseSessionEnvelope(hdrs)
	if got.HarnessID != "unknown" {
		t.Errorf("empty harness id should default to 'unknown', got %q", got.HarnessID)
	}
	if got.HarnessSessionID != "some-id" {
		t.Errorf("HarnessSessionID: got %q", got.HarnessSessionID)
	}
}

func TestParseSessionEnvelope_MalformedMetadataIsNonFatal(t *testing.T) {
	// Bytes that aren't valid base64url → metadata drops out, malformed
	// flag set, the rest of the envelope still extracts.
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID:       "claude",
		TapesHarnessMetadata: "!!!not-base64!!!",
	})
	got := ParseSessionEnvelope(hdrs)
	if !got.HarnessMetadataMalformed {
		t.Errorf("malformed metadata should set HarnessMetadataMalformed=true")
	}
	if got.HarnessMetadata != nil {
		t.Errorf("malformed metadata should leave HarnessMetadata=nil, got %v", got.HarnessMetadata)
	}
	if got.HarnessID != "claude" {
		t.Errorf("malformed metadata must not affect HarnessID extraction; got %q", got.HarnessID)
	}
}

func TestParseSessionEnvelope_MetadataValidBase64ButNotJSON(t *testing.T) {
	// Decodes as base64 but the bytes aren't a JSON object: also
	// non-fatal, also flagged malformed.
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID:       "claude",
		TapesHarnessMetadata: b64url([]byte(`not-json-at-all`)),
	})
	got := ParseSessionEnvelope(hdrs)
	if !got.HarnessMetadataMalformed {
		t.Errorf("non-JSON metadata payload should flag malformed")
	}
	if got.HarnessMetadata != nil {
		t.Errorf("non-JSON metadata should leave HarnessMetadata=nil")
	}
}

func TestParseSessionEnvelope_MetadataNonObjectJSONRejected(t *testing.T) {
	// harness_metadata is declared base64url(JSON OBJECT) on the
	// dispatched wire shape (see DispatchedSessionEnvelope). Reject
	// non-objects at the parser so the dispatched envelope never
	// carries a payload that violates that declared shape.
	// Note: JSON `null` is omitted from this list — encoding/json
	// Unmarshals null into a nil map[string]any without error, so
	// the parser treats it the same as "no metadata attached"
	// (HarnessMetadata stays nil, malformed flag stays false). Only
	// payloads that fail Unmarshal are surfaced as malformed.
	for _, payload := range []string{
		`[1, 2, 3]`,  // array
		`"a string"`, // scalar
		`42`,         // number
		`true`,       // bool
	} {
		hdrs := mkEnvelopeHeaders(map[string]string{
			TapesHarnessID:       "claude",
			TapesHarnessMetadata: b64url([]byte(payload)),
		})
		got := ParseSessionEnvelope(hdrs)
		if !got.HarnessMetadataMalformed {
			t.Errorf("non-object JSON %q should flag HarnessMetadataMalformed=true", payload)
		}
		if got.HarnessMetadata != nil {
			t.Errorf("non-object JSON %q should leave HarnessMetadata=nil, got %v", payload, got.HarnessMetadata)
		}
		if got.HarnessID != "claude" {
			t.Errorf("non-object JSON %q must not affect HarnessID extraction; got %q", payload, got.HarnessID)
		}
	}
}

func TestParseSessionEnvelope_MetadataRequiresTheDeclaredAlphabet(t *testing.T) {
	// The contract's alphabet is base64url with no padding, and it is the
	// only one this parser accepts (PCC-1066). The old permissive test was
	// vacuous: its 9-byte payload needed no padding and produced no
	// alphabet-distinct characters, so all four encoders emitted the same
	// string. These payloads are chosen so they cannot be: `{"a":1}` is 7
	// bytes (padding required in padded encodings), and `>>>` encodes to
	// `Pj4+` in std vs `Pj4-` in url.
	accept := base64.RawURLEncoding.EncodeToString([]byte(`{"a":">>>"}`))
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID:       "claude",
		TapesHarnessMetadata: accept,
	})
	got := ParseSessionEnvelope(hdrs)
	if got.HarnessMetadataMalformed || got.HarnessMetadata["a"] != ">>>" {
		t.Errorf("declared alphabet must decode; got malformed=%v metadata=%v",
			got.HarnessMetadataMalformed, got.HarnessMetadata)
	}

	for name, encoded := range map[string]string{
		"padded url": base64.URLEncoding.EncodeToString([]byte(`{"a":1}`)),
		"raw std":    base64.RawStdEncoding.EncodeToString([]byte(`{"a":">>>"}`)),
		"padded std": base64.StdEncoding.EncodeToString([]byte(`{"a":">>>"}`)),
	} {
		hdrs := mkEnvelopeHeaders(map[string]string{
			TapesHarnessID:       "claude",
			TapesHarnessMetadata: encoded,
		})
		got := ParseSessionEnvelope(hdrs)
		if !got.HarnessMetadataMalformed {
			t.Errorf("%s %q: a non-declared alphabet must be refused", name, encoded)
		}
		if got.HarnessMetadata != nil {
			t.Errorf("%s: refused metadata must not be stored; got %v", name, got.HarnessMetadata)
		}
		if got.HarnessID != "claude" {
			t.Errorf("%s: refusal must not affect the rest of the envelope", name)
		}
	}
}

func TestParseSessionEnvelope_OversizedMetadataIsNotRejected(t *testing.T) {
	// extproc imposes no size cap on metadata — whatever arrived
	// is parsed. A "large" metadata blob (here ~8 KiB raw JSON,
	// ~11 KiB base64) must parse cleanly and land on the envelope.
	big := strings.Repeat("a", 8000)
	jsonBlob := []byte(`{"big":"` + big + `"}`)
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID:       "claude",
		TapesHarnessMetadata: b64url(jsonBlob),
	})
	got := ParseSessionEnvelope(hdrs)
	if got.HarnessMetadataMalformed {
		t.Fatalf("large but well-formed metadata should not be flagged malformed")
	}
	if v, ok := got.HarnessMetadata["big"].(string); !ok || len(v) != 8000 {
		t.Errorf("metadata.big: round-trip failed; len=%d ok=%v", len(v), ok)
	}
}

func TestParseSessionEnvelope_LiteralPlusInSessionNamePreserved(t *testing.T) {
	// The session-name decode uses RFC 3986 path-segment semantics, so
	// a literal `+` that the emitter did not percent-encode must survive
	// the decode intact (form-urlencoded `+`→space conversion is wrong
	// for this header). Guards against regressing to url.QueryUnescape.
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID:   "claude",
		TapesSessionName: "c++ refactor",
	})
	got := ParseSessionEnvelope(hdrs)
	if got.Name != "c++ refactor" {
		t.Errorf("literal '+' should survive decode; got %q want %q", got.Name, "c++ refactor")
	}
}

func TestParseSessionEnvelope_MalformedPercentEncodingFallsBackToRaw(t *testing.T) {
	// A name with a malformed percent escape decodes as the raw
	// header value so the row still gets a label. The behavior is
	// stable so a downstream operator can see what arrived.
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID:   "claude",
		TapesSessionName: "bad%ZZescape",
	})
	got := ParseSessionEnvelope(hdrs)
	if got.Name != "bad%ZZescape" {
		t.Errorf("malformed percent-encoding should fall back to raw; got %q", got.Name)
	}
}

func TestParseSessionEnvelope_NonASCIICwdRoundTrips(t *testing.T) {
	// The emitter percent-encodes cwd because RFC 7230 forbids non-ASCII
	// bytes in raw header values. We must decode it back so storage sees
	// the natural path, not the encoded form.
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID: "claude",
		TapesCwd:       "/Users/%E6%9D%BE/code", // /Users/松/code
	})
	got := ParseSessionEnvelope(hdrs)
	if got.Cwd != "/Users/松/code" {
		t.Errorf("Cwd: got %q want %q", got.Cwd, "/Users/松/code")
	}
}

func TestParseSessionEnvelope_MalformedCwdEncodingFallsBackToRaw(t *testing.T) {
	// Same fallback policy as session name: a malformed percent escape
	// keeps the raw header value so the row still records something.
	hdrs := mkEnvelopeHeaders(map[string]string{
		TapesHarnessID: "claude",
		TapesCwd:       "/Users/bad%ZZ/code",
	})
	got := ParseSessionEnvelope(hdrs)
	if got.Cwd != "/Users/bad%ZZ/code" {
		t.Errorf("malformed percent-encoding should fall back to raw; got %q", got.Cwd)
	}
}

func TestParseSessionEnvelope_HeaderNamesAreCaseInsensitive(t *testing.T) {
	// Envoy lowercases HTTP/2 header names but ext_proc clients in
	// the wild may not. The parser must match either form.
	hdrs := mkEnvelopeHeaders(map[string]string{
		"X-Tapes-Harness-Id":         "claude",
		"X-Tapes-Harness-Session-Id": "abc",
	})
	got := ParseSessionEnvelope(hdrs)
	if !got.Present || got.HarnessID != "claude" || got.HarnessSessionID != "abc" {
		t.Errorf("case-insensitive lookup failed: %+v", got)
	}
}

func TestEnvelopeHeaderKeysFromRequest_StripsAllTapesPrefixHeaders(t *testing.T) {
	hdrs := mkEnvelopeHeaders(map[string]string{
		":path":                         "/v1/messages",
		"content-type":                  "application/json",
		"x-tapes-harness-id":            "claude",
		"x-tapes-harness-session-id":    "abc",
		"x-tapes-future-unknown-header": "yes", // forward-compat: not a known envelope member
		"x-tapes-agent-name":            "test",
		"x-paper-auth-org-id":           "org-1",            // must NOT be stripped
		"authorization":                 "Bearer model-key", // must NOT be stripped
	})

	got := EnvelopeHeaderKeysFromRequest(hdrs)
	sort.Strings(got)

	want := []string{
		"x-tapes-agent-name",
		"x-tapes-future-unknown-header",
		"x-tapes-harness-id",
		"x-tapes-harness-session-id",
	}
	sort.Strings(want)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("EnvelopeHeaderKeysFromRequest:\n got %v\nwant %v", got, want)
	}
}

func TestEnvelopeHeaderKeysFromRequest_NoEnvelope(t *testing.T) {
	// Pure non-envelope traffic: returns an empty (nil) slice so the
	// processor sends a plain ack with no HeaderMutation block.
	hdrs := mkEnvelopeHeaders(map[string]string{
		":path":        "/v1/messages",
		"content-type": "application/json",
	})
	got := EnvelopeHeaderKeysFromRequest(hdrs)
	if len(got) != 0 {
		t.Errorf("expected zero keys, got %v", got)
	}
}

func TestEnvelopeHeaderKeysFromRequest_MixedCasePreserved(t *testing.T) {
	// Echo the case as-received so packet captures grep cleanly.
	hdrs := mkEnvelopeHeaders(map[string]string{
		"X-Tapes-Harness-Id": "claude",
		"x-tapes-cwd":        "/x",
	})
	got := EnvelopeHeaderKeysFromRequest(hdrs)
	sort.Strings(got)
	want := []string{"X-Tapes-Harness-Id", "x-tapes-cwd"}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("case preservation: got %v want %v", got, want)
	}
}
