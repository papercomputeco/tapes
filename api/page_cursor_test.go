package api

import (
	"encoding/base64"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("page cursors", func() {
	It("round-trips a cursor", func() {
		// The three pagers share one codec; each keeps only its boundary
		// type. A minted token must decode to what minted it, and be
		// opaque on the wire: base64url with no padding, so it rides in a
		// query string unescaped.
		traces := tracesPageCursor{Session: "s1", TraceID: "t1", StartedAt: time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)}
		token := encodeTracesPageCursor(traces)
		Expect(token).NotTo(ContainSubstring("="))
		_, err := base64.RawURLEncoding.DecodeString(token)
		Expect(err).NotTo(HaveOccurred())
		Expect(decodeTracesPageCursor(token)).To(Equal(traces))

		trace := tracePageCursor{TraceID: "t1", Seq: 7, StartedAt: traces.StartedAt, SpanID: "sp7"}
		Expect(decodeTracePageCursor(encodeTracePageCursor(trace))).To(Equal(trace))

		raw := rawTurnsPageCursor{Session: "s1", ID: 42}
		Expect(decodeRawTurnsPageCursor(encodeRawTurnsPageCursor(raw))).To(Equal(raw))
	})

	It("rejects garbage and missing fields", func() {
		// Not base64, and base64 of not-JSON, are malformed tokens; the
		// error names the cursor so the 400 reads as one.
		_, err := decodeCursor[rawTurnsPageCursor]("not base64!")
		Expect(err).To(MatchError(HavePrefix("invalid cursor:")))
		_, err = decodeCursor[rawTurnsPageCursor](base64.RawURLEncoding.EncodeToString([]byte("{")))
		Expect(err).To(MatchError(HavePrefix("invalid cursor:")))

		// Well-formed JSON that names no boundary is each pager's own
		// check — a hand-crafted or truncated token, not a legacy client.
		_, err = decodeTracesPageCursor(encodeCursor(tracesPageCursor{Session: "s1"}))
		Expect(err).To(MatchError("invalid cursor: missing trace boundary"))
		_, err = decodeTracePageCursor(encodeCursor(tracePageCursor{TraceID: "t1"}))
		Expect(err).To(MatchError("invalid cursor: missing span boundary"))
		_, err = decodeRawTurnsPageCursor(encodeCursor(rawTurnsPageCursor{Session: "s1"}))
		Expect(err).To(MatchError("invalid cursor: missing raw turn boundary"))
		_, err = decodeRawTurnsPageCursor(encodeCursor(rawTurnsPageCursor{ID: 3}))
		Expect(err).To(MatchError("invalid cursor: missing raw turn boundary"))
	})

	It("parses and clamps limits", func() {
		Expect(parseLimit("", 50, 200)).To(Equal(50), "empty takes the default")
		Expect(parseLimit("7", 50, 200)).To(Equal(7))
		Expect(parseLimit("200", 50, 200)).To(Equal(200), "the ceiling itself is allowed")
		Expect(parseLimit("201", 50, 200)).To(Equal(200), "past the ceiling is clamped, not rejected")

		for _, raw := range []string{"0", "-1", "abc", "1.5", " 7"} {
			_, err := parseLimit(raw, 50, 200)
			Expect(err).To(MatchError("limit must be a positive integer"), raw)
		}
	})
})
