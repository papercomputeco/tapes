package postgres_test

// The raw-turn header listing is the operator's wire log: sizes without
// payloads. These specs pin where the sizes come from — the capture
// adapter's meta, never the stored blobs — because the cheap listing is
// only cheap while nothing in it detoasts a payload.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("raw turn headers [postgres]", func() {
	const (
		harnessID        = "claude-code"
		harnessSessionID = "5b0d5e66-5b4a-4f2a-9d0c-2a4a6a7f1c11"
	)

	var (
		driver *postgres.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()

		d, err := postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		driver = d

		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE raw_turns RESTART IDENTITY")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	// record builds a wire row whose payloads are deliberately tiny, so a
	// size that matches them could only have been measured from them.
	record := func(requestID string, meta string) storage.RawTurnRecord {
		return storage.RawTurnRecord{
			Source:           storage.RawTurnSourceWire,
			Provider:         "anthropic",
			AgentName:        "claude",
			HarnessID:        harnessID,
			HarnessSessionID: harnessSessionID,
			RequestID:        requestID,
			RawRequest:       json.RawMessage(`{"model":"claude-test","messages":[]}`),
			Response:         json.RawMessage(`{"model":"claude-test","stop_reason":"end_turn"}`),
			Meta:             json.RawMessage(meta),
			SessionEnvelope:  json.RawMessage(`{"harness_id":"` + harnessID + `","harness_session_id":"` + harnessSessionID + `"}`),
		}
	}

	put := func(rec storage.RawTurnRecord) {
		GinkgoHelper()
		inserted, err := driver.PutRawTurn(ctx, rec)
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())
	}

	list := func() []storage.RawTurnHeader {
		GinkgoHelper()
		headers, err := driver.ListRawTurnHeaders(ctx, "", harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
		return headers
	}

	It("reads request and response sizes from capture metadata, not the payloads", func() {
		put(record("req-sized", `{"request_id":"req-sized","request_bytes":12345,"response_bytes":678}`))

		headers := list()
		Expect(headers).To(HaveLen(1))
		Expect(headers[0].RequestBytes).To(Equal(int64(12345)),
			"request_bytes must be what the capture adapter recorded, not the stored request's length")
		Expect(headers[0].ResponseBytes).To(Equal(int64(678)),
			"response_bytes must be what the capture adapter recorded, not the stored response's length")
	})

	It("reports zero when the producer recorded no sizes", func() {
		put(record("req-unsized", `{"request_id":"req-unsized","model":"claude-test"}`))

		headers := list()
		Expect(headers).To(HaveLen(1))
		Expect(headers[0].RequestBytes).To(BeZero())
		Expect(headers[0].ResponseBytes).To(BeZero())
	})

	It("tolerates malformed sizes in meta instead of failing the listing", func() {
		// One row per way a size can be unusable: not a number, negative,
		// too wide for bigint, and a string that happens to be digits (which
		// is usable). A single bad value must not take the session's whole
		// wire log down with it.
		put(record("req-text", `{"request_id":"req-text","request_bytes":"lots","response_bytes":678}`))
		put(record("req-negative", `{"request_id":"req-negative","request_bytes":-5,"response_bytes":1.5}`))
		put(record("req-wide", `{"request_id":"req-wide","request_bytes":99999999999999999999,"response_bytes":678}`))
		put(record("req-digits", `{"request_id":"req-digits","request_bytes":"42","response_bytes":"7"}`))

		headers := list()
		Expect(headers).To(HaveLen(4))
		byRequest := map[string]storage.RawTurnHeader{}
		for _, h := range headers {
			byRequest[h.RequestID] = h
		}
		Expect(byRequest["req-text"].RequestBytes).To(BeZero())
		Expect(byRequest["req-text"].ResponseBytes).To(Equal(int64(678)))
		Expect(byRequest["req-negative"].RequestBytes).To(BeZero())
		Expect(byRequest["req-negative"].ResponseBytes).To(BeZero())
		Expect(byRequest["req-wide"].RequestBytes).To(BeZero())
		Expect(byRequest["req-wide"].ResponseBytes).To(Equal(int64(678)))
		Expect(byRequest["req-digits"].RequestBytes).To(Equal(int64(42)))
		Expect(byRequest["req-digits"].ResponseBytes).To(Equal(int64(7)))
	})

	It("reports raw response bytes and the dropped flag", func() {
		kept := record("req-kept", `{"request_id":"req-kept","request_bytes":10,"response_bytes":20}`)
		kept.RawResponse = []byte(`{"verbatim":"upstream bytes, kept as received"}`)
		kept.RawResponseEncoding = "identity"
		put(kept)

		dropped := record("req-dropped", `{"request_id":"req-dropped","request_bytes":10,"response_bytes":20}`)
		dropped.RawResponseDropped = true
		put(dropped)

		absent := record("req-absent", `{"request_id":"req-absent"}`)
		put(absent)

		headers := list()
		Expect(headers).To(HaveLen(3))
		byRequest := map[string]storage.RawTurnHeader{}
		for _, h := range headers {
			byRequest[h.RequestID] = h
		}
		Expect(byRequest["req-kept"].RawResponseBytes).To(Equal(int64(len(kept.RawResponse))))
		Expect(byRequest["req-kept"].RawResponseDropped).To(BeFalse())

		Expect(byRequest["req-dropped"].RawResponseBytes).To(BeZero())
		Expect(byRequest["req-dropped"].RawResponseDropped).To(BeTrue(),
			"a dropped raw response is a fidelity gap the header must disclose")

		Expect(byRequest["req-absent"].RawResponseBytes).To(BeZero())
		Expect(byRequest["req-absent"].RawResponseDropped).To(BeFalse(),
			"a turn that never had verbatim bytes is not a dropped one")
	})

	It("header query never casts payloads to text", func() {
		// The property the sizes-from-meta change buys is that listing a
		// session's wire log detoasts nothing. length(jsonb::text) would
		// undo that per row, so pin the shipped query text: sqlc embeds the
		// SQL verbatim in the generated file, which is what runs.
		query := listRawTurnHeadersQueryText()

		Expect(query).NotTo(MatchRegexp(`(raw_request|response)\s*::\s*text`),
			"a payload column cast to text is a detoast per row")
		Expect(query).NotTo(MatchRegexp(`length\(\s*r\.(raw_request|response)\b`),
			"measuring a payload column reads the whole payload")
		Expect(query).To(ContainSubstring(`r.meta->>'request_bytes'`))
		Expect(query).To(ContainSubstring(`r.meta->>'response_bytes'`))
		Expect(query).To(ContainSubstring(`octet_length(r.raw_response)`),
			"the bytea length is read from the header, not the bytes, and is the one size the store measures")
	})
})

// listRawTurnHeadersQueryText returns the SQL sqlc generated for
// ListRawTurnHeadersBySession, read from the generated source so the spec
// asserts on the text that ships rather than on a copy that could drift.
func listRawTurnHeadersQueryText() string {
	GinkgoHelper()
	_, file, _, ok := runtime.Caller(0)
	Expect(ok).To(BeTrue(), "runtime.Caller failed")

	generated, err := os.ReadFile(filepath.Join(filepath.Dir(file), "gensqlc", "raw_turns.sql.go"))
	Expect(err).NotTo(HaveOccurred())

	match := regexp.MustCompile("(?s)const listRawTurnHeadersBySession = `(.*?)`").FindSubmatch(generated)
	Expect(match).To(HaveLen(2), "ListRawTurnHeadersBySession query text not found in gensqlc/raw_turns.sql.go")
	return string(match[1])
}
