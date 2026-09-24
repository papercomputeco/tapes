package postgres_test

// Payload columns compress with lz4: a value written through the ordinary
// write paths that lands past the TOAST threshold reports 'lz4' from
// pg_column_compression, while a value small enough to stay inline reports
// no method at all — only TOASTed values carry one.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("payload compression [postgres]", func() {
	var (
		ctx    context.Context
		driver *postgres.Driver
	)

	const (
		harnessID        = "claude-code"
		harnessSessionID = "dddddddd-4444-4444-8444-dddddddddddd"
		sessionRowID     = "01900000-0000-7000-8000-00000000000d"
	)

	// Well past the ~2 KiB TOAST threshold and highly repetitive, so the
	// value is both eligible for compression and worth compressing.
	filler := strings.Repeat("the quick brown fox jumps over the lazy dog ", 512)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = postgres.NewDriver(ctx, testPostgresDSN)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(driver.Close)

		for _, stmt := range []string{
			"TRUNCATE TABLE derive_queue",
			"TRUNCATE TABLE raw_turns RESTART IDENTITY",
			"TRUNCATE TABLE sessions CASCADE",
		} {
			_, err = driver.DB().Exec(ctx, stmt)
			Expect(err).NotTo(HaveOccurred())
		}

		_, err = driver.DB().Exec(ctx, `
			INSERT INTO sessions (id, org_id, auth_subject, harness_id, harness_session_id, started_at, last_seen_at)
			VALUES ($1, '00000000-0000-0000-0000-000000000000', 'test', $2, $3, NOW(), NOW())`,
			sessionRowID, harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())
	})

	// putTurn writes a main (conversation-spine) call carrying text in the
	// user prompt and the assistant reply, through the driver's normal path.
	putTurn := func(requestID, text string) {
		quoted, err := json.Marshal(text)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.PutRawTurn(ctx, storage.RawTurnRecord{
			Source:           storage.RawTurnSourceWire,
			Provider:         "anthropic",
			AgentName:        "claude",
			HarnessID:        harnessID,
			HarnessSessionID: harnessSessionID,
			RequestID:        requestID,
			RawRequest: json.RawMessage(
				`{"model":"claude-test","max_tokens":4096,"stream":true,` +
					`"tools":[{"name":"Bash","description":"run","input_schema":{"type":"object"}}],` +
					`"messages":[{"role":"user","content":[{"type":"text","text":` + string(quoted) + `}]}]}`),
			Response: json.RawMessage(
				`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":` + string(quoted) + `}]},"stop_reason":"end_turn"}`),
			SessionEnvelope: json.RawMessage(fmt.Sprintf(
				`{"harness_id":%q,"harness_session_id":%q}`, harnessID, harnessSessionID)),
		})
		Expect(err).NotTo(HaveOccurred())
	}

	// rawTurnMethods reads the compression method Postgres recorded for the
	// two payload columns of the turn with the given request id. NULL means
	// the value was stored inline, uncompressed.
	rawTurnMethods := func(requestID string) (request, response *string) {
		Expect(driver.DB().QueryRow(ctx, `
			SELECT pg_column_compression(raw_request), pg_column_compression(response)
			FROM raw_turns WHERE request_id = $1`, requestID).Scan(&request, &response)).To(Succeed())
		return request, response
	}

	It("new payload rows compress with lz4", func() {
		putTurn("req-lz4-large", filler)
		request, response := rawTurnMethods("req-lz4-large")
		Expect(request).To(HaveValue(Equal("lz4")), "raw_turns.raw_request")
		Expect(response).To(HaveValue(Equal("lz4")), "raw_turns.response")

		// The same for the span the deriver projects from that turn: its
		// input echoes the prompt and its output the reply, both TOASTed.
		_, err := driver.RederiveSession(ctx, "", "", harnessID, harnessSessionID)
		Expect(err).NotTo(HaveOccurred())

		var input, output *string
		Expect(driver.DB().QueryRow(ctx, `
			SELECT pg_column_compression(input), pg_column_compression(output)
			FROM spans_20260615 WHERE session_id = $1 AND kind = 'llm'`, sessionRowID).Scan(&input, &output)).To(Succeed())
		Expect(input).To(HaveValue(Equal("lz4")), "spans.input")
		Expect(output).To(HaveValue(Equal("lz4")), "spans.output")

		// A value that never leaves the heap tuple is not compressed at
		// all, so it reports no method: the column setting only speaks
		// for values large enough to TOAST.
		putTurn("req-lz4-small", "hi")
		request, response = rawTurnMethods("req-lz4-small")
		Expect(request).To(BeNil(), "an inline raw_request carries no compression method")
		Expect(response).To(BeNil(), "an inline response carries no compression method")
	})
})
