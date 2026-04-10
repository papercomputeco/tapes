package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var _ = Describe("GET /v1/sessions/summary", func() {
	var (
		server   *Server
		inMem    *inmemory.Driver
		ctx      context.Context
		baseTime time.Time
	)

	BeforeEach(func() {
		logger := tapeslogger.NewNoop()
		inMem = inmemory.NewDriver()
		ctx = context.Background()
		baseTime = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

		cfg := Config{
			ListenAddr: ":0",
			Pricing: sessions.PricingTable{
				"test-model": {Input: 10.0, Output: 30.0},
			},
		}
		var err error
		server, err = NewServer(cfg, inMem, logger)
		Expect(err).NotTo(HaveOccurred())
	})

	// Build a two-turn session ending in a "stop"-reasoned assistant response.
	// The `tag` parameter ensures unique content across sessions so
	// content-addressing doesn't dedupe them into the same node.
	seedSession := func(tag string, offset time.Duration, inputTokens, outputTokens int, stop string) *merkle.Node {
		userBucket := merkle.Bucket{
			Type:    "message",
			Role:    "user",
			Content: []llm.ContentBlock{{Type: "text", Text: "hello " + tag}},
			Model:   "test-model",
		}
		user := merkle.NewNode(userBucket, nil, merkle.NodeOptions{Project: "tapes"})
		user.CreatedAt = baseTime.Add(offset)
		_, err := inMem.Put(ctx, user)
		Expect(err).NotTo(HaveOccurred())

		assistantBucket := merkle.Bucket{
			Type:    "message",
			Role:    "assistant",
			Content: []llm.ContentBlock{{Type: "text", Text: "hi " + tag}},
			Model:   "test-model",
		}
		assistant := merkle.NewNode(assistantBucket, user, merkle.NodeOptions{
			Project:    "tapes",
			StopReason: stop,
			Usage: &llm.Usage{
				PromptTokens:     inputTokens,
				CompletionTokens: outputTokens,
			},
		})
		assistant.CreatedAt = baseTime.Add(offset + time.Second)
		_, err = inMem.Put(ctx, assistant)
		Expect(err).NotTo(HaveOccurred())

		return assistant
	}

	decodeSummary := func(path string) SessionSummaryListResponse {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		var body SessionSummaryListResponse
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
		return body
	}

	It("returns an empty list for an empty store", func() {
		body := decodeSummary("/v1/sessions/summary")
		Expect(body.Items).To(BeEmpty())
		Expect(body.NextCursor).To(BeEmpty())
	})

	It("builds per-session summaries with cost, status, and duration", func() {
		leaf := seedSession("big", 0, 1_000_000, 500_000, "stop")

		body := decodeSummary("/v1/sessions/summary")
		Expect(body.Items).To(HaveLen(1))

		item := body.Items[0]
		Expect(item.ID).To(Equal(leaf.Hash))
		Expect(item.Status).To(Equal(sessions.StatusCompleted))
		Expect(item.MessageCount).To(Equal(2))
		Expect(item.InputTokens).To(Equal(int64(1_000_000)))
		Expect(item.OutputTokens).To(Equal(int64(500_000)))
		// 1M * $10/M + 0.5M * $30/M = $10 + $15 = $25
		Expect(item.TotalCost).To(BeNumerically("~", 25.0, 0.0001))
		Expect(item.Duration).To(Equal(time.Second))
		Expect(item.Project).To(Equal("tapes"))
	})

	It("returns summaries newest-first and paginates", func() {
		leaf1 := seedSession("a", 0, 100_000, 50_000, "stop")               // oldest
		leaf2 := seedSession("b", 5*time.Minute, 200_000, 100_000, "stop")  //
		leaf3 := seedSession("c", 10*time.Minute, 300_000, 150_000, "stop") // newest

		page := decodeSummary("/v1/sessions/summary")
		Expect(page.Items).To(HaveLen(3))
		Expect(page.Items[0].ID).To(Equal(leaf3.Hash))
		Expect(page.Items[1].ID).To(Equal(leaf2.Hash))
		Expect(page.Items[2].ID).To(Equal(leaf1.Hash))

		page1 := decodeSummary("/v1/sessions/summary?limit=2")
		Expect(page1.Items).To(HaveLen(2))
		Expect(page1.NextCursor).NotTo(BeEmpty())

		page2 := decodeSummary("/v1/sessions/summary?limit=2&cursor=" + page1.NextCursor)
		Expect(page2.Items).To(HaveLen(1))
		Expect(page2.Items[0].ID).To(Equal(leaf1.Hash))
		Expect(page2.NextCursor).To(BeEmpty())
	})

	It("respects filters", func() {
		seedSession("a", 0, 100_000, 50_000, "stop")
		// Seed a second session with a different project so the project
		// filter narrows the result set.
		other := merkle.NewNode(merkle.Bucket{
			Type:    "message",
			Role:    "assistant",
			Content: []llm.ContentBlock{{Type: "text", Text: "other project"}},
			Model:   "test-model",
		}, nil, merkle.NodeOptions{
			Project:    "other",
			StopReason: "stop",
		})
		other.CreatedAt = baseTime.Add(30 * time.Minute)
		_, err := inMem.Put(ctx, other)
		Expect(err).NotTo(HaveOccurred())

		body := decodeSummary("/v1/sessions/summary?project=tapes")
		Expect(body.Items).To(HaveLen(1))
		Expect(body.Items[0].Project).To(Equal("tapes"))
	})

	It("returns 400 for an invalid cursor", func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/v1/sessions/summary?cursor=not-a-real-cursor!!!", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
	})

	It("does not shadow /v1/sessions/:hash with the static summary route", func() {
		leaf := seedSession("a", 0, 100_000, 50_000, "stop")

		// Hitting /v1/sessions/<realhash> should still resolve to the detail
		// endpoint, not the summary endpoint.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/v1/sessions/"+leaf.Hash, nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

		raw, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		var session SessionResponse
		Expect(json.Unmarshal(raw, &session)).To(Succeed())
		Expect(session.Hash).To(Equal(leaf.Hash))
		Expect(session.Turns).NotTo(BeEmpty())
	})
})
