package postgres_test

import (
	"context"
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("RawTurnStore", func() {
	var (
		driver *postgres.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).ToNot(HaveOccurred())

		d, err := postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		driver = d

		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE raw_turns RESTART IDENTITY")
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE nodes")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	rawRecord := func(requestID string) storage.RawTurnRecord {
		return storage.RawTurnRecord{
			Source:           storage.RawTurnSourceWire,
			Provider:         "anthropic",
			AgentName:        "claude",
			HarnessID:        "claude-code",
			HarnessSessionID: "0ea3c2cc-fe9d-41ff-aab1-4134ad00c350",
			RequestID:        requestID,
			RawRequest:       json.RawMessage(`{"model":"claude-test","max_tokens":64,"system":"You are a security monitor.","messages":[{"role":"user","content":"<transcript>Bash ls</transcript>"}]}`),
			Response:         json.RawMessage(`{"model":"claude-test","message":{"role":"assistant","content":[{"type":"text","text":"<block>no"}]},"stop_reason":"end_turn"}`),
			Meta:             json.RawMessage(`{"request_id":"` + requestID + `","model":"claude-test","stream":"false","upstream_status":200,"elapsed_seconds":1.5,"unknown_future_field":"survives"}`),
			SessionEnvelope:  json.RawMessage(`{"harness_id":"claude-code","harness_session_id":"0ea3c2cc-fe9d-41ff-aab1-4134ad00c350"}`),
		}
	}

	It("appends and reads back the verbatim envelope", func() {
		inserted, err := driver.PutRawTurn(ctx, rawRecord("req-1"))
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())

		rows, err := driver.ListRawTurns(ctx, 0, 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(rows).To(HaveLen(1))

		row := rows[0]
		Expect(row.Source).To(Equal("wire"))
		Expect(row.Provider).To(Equal("anthropic"))
		Expect(row.HarnessSessionID).To(Equal("0ea3c2cc-fe9d-41ff-aab1-4134ad00c350"))
		Expect(row.RequestID).To(Equal("req-1"))
		Expect(row.ReceivedAt).NotTo(BeZero())

		// Verbatim survival of fields this build doesn't know about —
		// the property that makes the raw layer the iteration substrate.
		var meta map[string]any
		Expect(json.Unmarshal(row.Meta, &meta)).To(Succeed())
		Expect(meta).To(HaveKeyWithValue("unknown_future_field", "survives"))
		Expect(meta).To(HaveKeyWithValue("upstream_status", float64(200)))

		var req map[string]any
		Expect(json.Unmarshal(row.RawRequest, &req)).To(Succeed())
		Expect(req).To(HaveKeyWithValue("system", "You are a security monitor."))
	})

	It("dedupes retried writes by (org, request_id) but appends distinct calls", func() {
		inserted, err := driver.PutRawTurn(ctx, rawRecord("req-dup"))
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())

		inserted, err = driver.PutRawTurn(ctx, rawRecord("req-dup"))
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeFalse())

		inserted, err = driver.PutRawTurn(ctx, rawRecord("req-other"))
		Expect(err).NotTo(HaveOccurred())
		Expect(inserted).To(BeTrue())

		count, err := driver.CountRawTurns(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(int64(2)))
	})

	It("appends without dedup when request_id is empty", func() {
		rec := rawRecord("")
		_, err := driver.PutRawTurn(ctx, rec)
		Expect(err).NotTo(HaveOccurred())
		_, err = driver.PutRawTurn(ctx, rec)
		Expect(err).NotTo(HaveOccurred())

		count, err := driver.CountRawTurns(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(int64(2)))
	})

	It("pages the scan in insertion order", func() {
		for _, id := range []string{"a", "b", "c"} {
			_, err := driver.PutRawTurn(ctx, rawRecord("req-"+id))
			Expect(err).NotTo(HaveOccurred())
		}

		page1, err := driver.ListRawTurns(ctx, 0, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(page1).To(HaveLen(2))

		page2, err := driver.ListRawTurns(ctx, page1[1].ID, 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(page2).To(HaveLen(1))
		Expect(page2[0].RequestID).To(Equal("req-c"))
	})

	It("lists raw turn header sizes for JSONB payloads", func() {
		rec := rawRecord("req-headers")
		_, err := driver.PutRawTurn(ctx, rec)
		Expect(err).NotTo(HaveOccurred())

		headers, err := driver.ListRawTurnHeaders(ctx, "", rec.HarnessID, rec.HarnessSessionID)
		Expect(err).NotTo(HaveOccurred())
		Expect(headers).To(HaveLen(1))
		Expect(headers[0].RequestID).To(Equal("req-headers"))
		Expect(headers[0].RequestBytes).To(BeNumerically(">", 0))
		Expect(headers[0].ResponseBytes).To(BeNumerically(">", 0))
	})
})

var _ = Describe("node request params round-trip", func() {
	var (
		driver *postgres.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn, err := testPostgresDSN()
		Expect(err).ToNot(HaveOccurred())

		d, err := postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		driver = d

		_, err = driver.DB().Exec(ctx, "TRUNCATE TABLE nodes")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	It("persists and reads back the promoted request params", func() {
		maxTokens := 64
		stream := false
		toolCount := 0
		node := merkle.NewNode(postgresTestBucket("judge this"), nil, merkle.NodeOptions{
			Request: &llm.RequestParams{
				System:    "You are a security monitor.",
				MaxTokens: &maxTokens,
				Stream:    &stream,
				ToolCount: &toolCount,
			},
		})

		_, err := driver.Put(ctx, node)
		Expect(err).NotTo(HaveOccurred())

		got, err := driver.Get(ctx, node.Hash)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Request).NotTo(BeNil())
		Expect(got.Request.System).To(Equal("You are a security monitor."))
		Expect(got.Request.MaxTokens).To(HaveValue(Equal(64)))
		Expect(got.Request.Stream).To(HaveValue(BeFalse()))
		Expect(got.Request.ToolCount).To(HaveValue(Equal(0)))
		Expect(got.Request.Temperature).To(BeNil())
	})

	It("reads legacy rows (no params) back as nil", func() {
		node := merkle.NewNode(postgresTestBucket("legacy"), nil)
		_, err := driver.Put(ctx, node)
		Expect(err).NotTo(HaveOccurred())

		got, err := driver.Get(ctx, node.Hash)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Request).To(BeNil())
	})
})
