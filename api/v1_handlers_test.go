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
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// v1TestBucket builds a bucket with enough identifying fields for list/filter tests.
func v1TestBucket(role, text, model, provider, agent string) merkle.Bucket {
	return merkle.Bucket{
		Type:      "message",
		Role:      role,
		Content:   []llm.ContentBlock{{Type: "text", Text: text}},
		Model:     model,
		Provider:  provider,
		AgentName: agent,
	}
}

var _ = Describe("v1 session handlers", func() {
	var (
		server *Server
		inMem  storage.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		logger := tapeslogger.NewNoop()
		inMem = inmemory.NewDriver()
		ctx = context.Background()

		var err error
		server, err = NewServer(Config{ListenAddr: ":0"}, inMem, logger)
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("GET /v1/sessions", func() {
		Context("empty store", func() {
			It("returns an empty list and no cursor", func() {
				req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/sessions", nil)
				Expect(err).NotTo(HaveOccurred())

				resp, err := server.app.Test(req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

				var body SessionListResponse
				Expect(json.NewDecoder(resp.Body).Decode(&body)).To(Succeed())
				Expect(body.Items).To(BeEmpty())
				Expect(body.NextCursor).To(BeEmpty())
			})
		})

		Context("with a root and three leaves", func() {
			var (
				root                *merkle.Node
				leafA, leafB, leafC *merkle.Node
				baseTime            time.Time
			)

			BeforeEach(func() {
				baseTime = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
				root = merkle.NewNode(v1TestBucket("user", "kickoff", "claude-opus-4-6", "anthropic", "claude"), nil, merkle.NodeOptions{Project: "tapes"})
				root.CreatedAt = baseTime
				Expect(putNode(ctx, inMem, root)).To(Succeed())

				leafC = merkle.NewNode(v1TestBucket("assistant", "oldest answer", "claude-opus-4-6", "anthropic", "claude"), root, merkle.NodeOptions{Project: "tapes"})
				leafC.CreatedAt = baseTime.Add(1 * time.Minute)
				Expect(putNode(ctx, inMem, leafC)).To(Succeed())

				leafB = merkle.NewNode(v1TestBucket("assistant", "middle answer", "claude-sonnet-4-6", "anthropic", "opencode"), root, merkle.NodeOptions{Project: "other"})
				leafB.CreatedAt = baseTime.Add(2 * time.Minute)
				Expect(putNode(ctx, inMem, leafB)).To(Succeed())

				leafA = merkle.NewNode(v1TestBucket("assistant", "newest answer", "gpt-4o", "openai", "claude"), root, merkle.NodeOptions{Project: "tapes"})
				leafA.CreatedAt = baseTime.Add(3 * time.Minute)
				Expect(putNode(ctx, inMem, leafA)).To(Succeed())
			})

			It("returns leaves newest-first and excludes the root", func() {
				body := decodeList(server, "/v1/sessions")
				Expect(itemHashes(body.Items)).To(Equal([]string{leafA.Hash, leafB.Hash, leafC.Hash}))
				Expect(body.NextCursor).To(BeEmpty())
			})

			It("populates lean fields from the leaf node", func() {
				body := decodeList(server, "/v1/sessions")
				item := body.Items[0]
				Expect(item.Hash).To(Equal(leafA.Hash))
				Expect(item.HeadRole).To(Equal("assistant"))
				Expect(item.Model).To(Equal("gpt-4o"))
				Expect(item.Provider).To(Equal("openai"))
				Expect(item.AgentName).To(Equal("claude"))
				Expect(item.Project).To(Equal("tapes"))
				Expect(item.Preview).To(Equal("newest answer"))
				Expect(item.UpdatedAt).To(Equal(leafA.CreatedAt))
			})

			It("paginates with ?limit and ?cursor", func() {
				page1 := decodeList(server, "/v1/sessions?limit=2")
				Expect(itemHashes(page1.Items)).To(Equal([]string{leafA.Hash, leafB.Hash}))
				Expect(page1.NextCursor).NotTo(BeEmpty())

				page2 := decodeList(server, "/v1/sessions?limit=2&cursor="+page1.NextCursor)
				Expect(itemHashes(page2.Items)).To(Equal([]string{leafC.Hash}))
				Expect(page2.NextCursor).To(BeEmpty())
			})

			It("filters by project", func() {
				body := decodeList(server, "/v1/sessions?project=tapes")
				Expect(itemHashes(body.Items)).To(ConsistOf(leafA.Hash, leafC.Hash))
			})

			It("filters by agent_name", func() {
				body := decodeList(server, "/v1/sessions?agent_name=opencode")
				Expect(itemHashes(body.Items)).To(ConsistOf(leafB.Hash))
			})

			It("filters by model", func() {
				body := decodeList(server, "/v1/sessions?model=gpt-4o")
				Expect(itemHashes(body.Items)).To(ConsistOf(leafA.Hash))
			})

			It("filters by provider", func() {
				body := decodeList(server, "/v1/sessions?provider=anthropic")
				Expect(itemHashes(body.Items)).To(ConsistOf(leafB.Hash, leafC.Hash))
			})

			It("filters by since/until RFC3339", func() {
				since := baseTime.Add(2 * time.Minute).Format(time.RFC3339)
				body := decodeList(server, "/v1/sessions?since="+since)
				Expect(itemHashes(body.Items)).To(ConsistOf(leafA.Hash, leafB.Hash))
			})

			It("returns 400 for invalid limit", func() {
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/sessions?limit=notanumber", nil)
				resp, err := server.app.Test(req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
			})

			It("returns 400 for invalid since", func() {
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/sessions?since=not-a-date", nil)
				resp, err := server.app.Test(req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
			})

			It("returns 400 for an invalid cursor", func() {
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/sessions?cursor=not-a-real-cursor!!!", nil)
				resp, err := server.app.Test(req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
			})
		})
	})

	Describe("GET /v1/sessions/:hash", func() {
		var root, mid, leaf *merkle.Node

		BeforeEach(func() {
			root = merkle.NewNode(v1TestBucket("user", "q1", "m", "p", "claude"), nil)
			mid = merkle.NewNode(v1TestBucket("assistant", "a1", "m", "p", "claude"), root)
			leaf = merkle.NewNode(v1TestBucket("user", "q2", "m", "p", "claude"), mid)
			Expect(putNode(ctx, inMem, root)).To(Succeed())
			Expect(putNode(ctx, inMem, mid)).To(Succeed())
			Expect(putNode(ctx, inMem, leaf)).To(Succeed())
		})

		It("returns the full chain in chronological order", func() {
			body := decodeSession(server, "/v1/sessions/"+leaf.Hash)
			Expect(body.Hash).To(Equal(leaf.Hash))
			Expect(body.Depth).To(Equal(3))
			Expect(body.Turns).To(HaveLen(3))
			Expect(body.Turns[0].Hash).To(Equal(root.Hash))
			Expect(body.Turns[1].Hash).To(Equal(mid.Hash))
			Expect(body.Turns[2].Hash).To(Equal(leaf.Hash))
		})

		It("links parent hashes correctly", func() {
			body := decodeSession(server, "/v1/sessions/"+leaf.Hash)
			Expect(body.Turns[0].ParentHash).To(BeNil())
			Expect(body.Turns[1].ParentHash).NotTo(BeNil())
			Expect(*body.Turns[1].ParentHash).To(Equal(root.Hash))
			Expect(*body.Turns[2].ParentHash).To(Equal(mid.Hash))
		})

		It("honors ?depth=N to return only the last N turns", func() {
			body := decodeSession(server, "/v1/sessions/"+leaf.Hash+"?depth=2")
			Expect(body.Depth).To(Equal(3))
			Expect(body.Turns).To(HaveLen(2))
			// Last 2 turns in chronological order: mid, leaf.
			Expect(body.Turns[0].Hash).To(Equal(mid.Hash))
			Expect(body.Turns[1].Hash).To(Equal(leaf.Hash))
		})

		It("?depth=1 returns only the head turn", func() {
			body := decodeSession(server, "/v1/sessions/"+leaf.Hash+"?depth=1")
			Expect(body.Turns).To(HaveLen(1))
			Expect(body.Turns[0].Hash).To(Equal(leaf.Hash))
		})

		It("returns 404 for an unknown hash", func() {
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/sessions/does-not-exist", nil)
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusNotFound))
		})

		It("returns 400 for invalid depth", func() {
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/sessions/"+leaf.Hash+"?depth=-1", nil)
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
		})

		Context("when an ancestor's parent is missing from the store", func() {
			var (
				phantomParent string
				orphan        *merkle.Node
				orphanChild   *merkle.Node
			)

			BeforeEach(func() {
				// Build a chain whose root points at a parent that
				// was never inserted. The inmemory driver has no FK
				// enforcement so this simulates the production
				// dangling shape directly.
				phantomParent = "ffff000000000000000000000000000000000000000000000000000000000000"
				orphanBucket := v1TestBucket("user", "orphan turn", "m", "p", "claude")
				orphan = merkle.NewNode(orphanBucket, &merkle.Node{Hash: phantomParent}, merkle.NodeOptions{Project: "tapes"})
				Expect(putNode(ctx, inMem, orphan)).To(Succeed())

				orphanChild = merkle.NewNode(v1TestBucket("assistant", "answer", "m", "p", "claude"), orphan, merkle.NodeOptions{Project: "tapes"})
				Expect(putNode(ctx, inMem, orphanChild)).To(Succeed())
			})

			It("marks the detail response truncated and names the missing parent", func() {
				body := decodeSession(server, "/v1/sessions/"+orphanChild.Hash)
				Expect(body.Truncated).To(BeTrue())
				Expect(body.MissingParent).To(Equal(phantomParent))
				// The two resolvable nodes still come back in
				// chronological order.
				Expect(body.Turns).To(HaveLen(2))
				Expect(body.Turns[0].Hash).To(Equal(orphan.Hash))
				Expect(body.Turns[1].Hash).To(Equal(orphanChild.Hash))
			})

			It("leaves clean sessions with no truncation marker", func() {
				// The outer BeforeEach built leaf with a real root,
				// so it must round-trip clean even while a dangling
				// chain also lives in the same store.
				body := decodeSession(server, "/v1/sessions/"+leaf.Hash)
				Expect(body.Truncated).To(BeFalse())
				Expect(body.MissingParent).To(BeEmpty())
			})
		})
	})

	Describe("GET /v1/stats", func() {
		Context("with bare seed data (no usage, no stop_reason)", func() {
			BeforeEach(func() {
				root := merkle.NewNode(v1TestBucket("user", "root", "m", "p", "claude"), nil, merkle.NodeOptions{Project: "tapes"})
				Expect(putNode(ctx, inMem, root)).To(Succeed())

				leafA := merkle.NewNode(v1TestBucket("assistant", "a", "m", "p", "claude"), root, merkle.NodeOptions{Project: "tapes"})
				leafB := merkle.NewNode(v1TestBucket("assistant", "b", "m", "p", "claude"), root, merkle.NodeOptions{Project: "other"})
				Expect(putNode(ctx, inMem, leafA)).To(Succeed())
				Expect(putNode(ctx, inMem, leafB)).To(Succeed())
			})

			It("returns unfiltered totals with zero aggregates", func() {
				body := decodeStats(server, "/v1/stats")
				Expect(body.SessionCount).To(Equal(2))
				Expect(body.TurnCount).To(Equal(3))
				Expect(body.RootCount).To(Equal(1))
				// No Usage on any node, no pricing match for model "m", no
				// stop_reason → completed_count and the aggregates stay zero.
				Expect(body.CompletedCount).To(Equal(0))
				Expect(body.TotalCost).To(Equal(0.0))
				Expect(body.InputTokens).To(Equal(int64(0)))
				Expect(body.OutputTokens).To(Equal(int64(0)))
				Expect(body.ToolCalls).To(Equal(0))
			})

			It("applies the project filter", func() {
				body := decodeStats(server, "/v1/stats?project=tapes")
				Expect(body.SessionCount).To(Equal(1))
				Expect(body.TurnCount).To(Equal(2))
				Expect(body.RootCount).To(Equal(1))
				Expect(body.CompletedCount).To(Equal(0))
			})
		})

		Context("with rich session data (usage, stop_reason, pricing)", func() {
			var (
				richServer *Server
				richMem    storage.Driver
				richCtx    context.Context
				baseTime   time.Time
				offset     time.Duration
			)

			BeforeEach(func() {
				logger := tapeslogger.NewNoop()
				richMem = inmemory.NewDriver()
				richCtx = context.Background()
				baseTime = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
				offset = 0

				cfg := Config{
					ListenAddr: ":0",
					Pricing: sessions.PricingTable{
						"test-model": {Input: 10.0, Output: 30.0},
					},
				}
				var err error
				richServer, err = NewServer(cfg, richMem, logger)
				Expect(err).NotTo(HaveOccurred())
			})

			type seedOpts struct {
				tag         string
				project     string
				inputTokens int
				outputTok   int
				durationNs  int64
				stop        string
				toolUses    int
			}

			// Seeds a two-turn session (user → assistant). Per-node Usage
			// drives the SQL-aggregate semantic the handler now uses, so
			// duration goes on the assistant Usage, not on CreatedAt.
			seedSession := func(o seedOpts) {
				offset += time.Second
				userBucket := merkle.Bucket{
					Type:    "message",
					Role:    "user",
					Content: []llm.ContentBlock{{Type: "text", Text: "q " + o.tag}},
					Model:   "test-model",
				}
				user := merkle.NewNode(userBucket, nil, merkle.NodeOptions{Project: o.project})
				user.CreatedAt = baseTime.Add(offset)
				_, err := richMem.Put(richCtx, user)
				Expect(err).NotTo(HaveOccurred())

				blocks := []llm.ContentBlock{{Type: "text", Text: "a " + o.tag}}
				for i := 0; i < o.toolUses; i++ {
					blocks = append(blocks, llm.ContentBlock{Type: "tool_use", ToolName: "fake_tool"})
				}
				assistantBucket := merkle.Bucket{
					Type:    "message",
					Role:    "assistant",
					Content: blocks,
					Model:   "test-model",
				}
				assistant := merkle.NewNode(assistantBucket, user, merkle.NodeOptions{
					Project:    o.project,
					StopReason: o.stop,
					Usage: &llm.Usage{
						PromptTokens:     o.inputTokens,
						CompletionTokens: o.outputTok,
						TotalDurationNs:  o.durationNs,
					},
				})
				offset += time.Second
				assistant.CreatedAt = baseTime.Add(offset)
				_, err = richMem.Put(richCtx, assistant)
				Expect(err).NotTo(HaveOccurred())
			}

			It("aggregates tokens, duration, tool_calls, and folds cost from the per-model rollup", func() {
				// Session 1: completed (stop), 1M in / 0.5M out → $25; 1s LLM time; 2 tool_use blocks.
				seedSession(seedOpts{tag: "a", project: "tapes", inputTokens: 1_000_000, outputTok: 500_000, durationNs: int64(time.Second), stop: "stop", toolUses: 2})
				// Session 2: pending (no stop_reason), 0.5M in / 0.25M out → $12.50; 2s LLM time; no tool calls.
				seedSession(seedOpts{tag: "b", project: "tapes", inputTokens: 500_000, outputTok: 250_000, durationNs: 2 * int64(time.Second), stop: ""})

				body := decodeStats(richServer, "/v1/stats")
				Expect(body.SessionCount).To(Equal(2))
				Expect(body.TurnCount).To(Equal(4))
				Expect(body.RootCount).To(Equal(2))
				Expect(body.InputTokens).To(Equal(int64(1_500_000)))
				Expect(body.OutputTokens).To(Equal(int64(750_000)))
				Expect(body.TotalCost).To(BeNumerically("~", 37.5, 0.0001))
				Expect(body.TotalDurationNs).To(Equal(3 * int64(time.Second)))
				Expect(body.ToolCalls).To(Equal(2))
				// Leaf-status: session 1's assistant leaf has stop_reason "stop"; session 2's is empty.
				Expect(body.CompletedCount).To(Equal(1))
			})

			It("narrows folded aggregates by the project filter", func() {
				seedSession(seedOpts{tag: "a", project: "tapes", inputTokens: 1_000_000, outputTok: 500_000, durationNs: int64(time.Second), stop: "stop"})
				seedSession(seedOpts{tag: "b", project: "other", inputTokens: 999_999, outputTok: 999_999, durationNs: 99 * int64(time.Second), stop: "stop"})

				body := decodeStats(richServer, "/v1/stats?project=tapes")
				Expect(body.SessionCount).To(Equal(1))
				Expect(body.InputTokens).To(Equal(int64(1_000_000)))
				Expect(body.OutputTokens).To(Equal(int64(500_000)))
				Expect(body.TotalCost).To(BeNumerically("~", 25.0, 0.0001))
				Expect(body.TotalDurationNs).To(Equal(int64(time.Second)))
				Expect(body.CompletedCount).To(Equal(1))
			})

			It("classifies completed_count using leaf stop_reason only", func() {
				// Sessions across the spectrum of leaf states.
				seedSession(seedOpts{tag: "completed-stop", project: "tapes", stop: "stop"})
				seedSession(seedOpts{tag: "completed-end_turn", project: "tapes", stop: "end_turn"})
				seedSession(seedOpts{tag: "completed-eos", project: "tapes", stop: "EOS"}) // case-insensitive
				seedSession(seedOpts{tag: "failed-length", project: "tapes", stop: "length"})
				seedSession(seedOpts{tag: "unknown", project: "tapes", stop: ""})

				body := decodeStats(richServer, "/v1/stats")
				Expect(body.SessionCount).To(Equal(5))
				Expect(body.CompletedCount).To(Equal(3))
			})

			It("returns zeros across every field for an empty store", func() {
				body := decodeStats(richServer, "/v1/stats")
				Expect(body.SessionCount).To(Equal(0))
				Expect(body.TurnCount).To(Equal(0))
				Expect(body.RootCount).To(Equal(0))
				Expect(body.CompletedCount).To(Equal(0))
				Expect(body.TotalCost).To(Equal(0.0))
				Expect(body.InputTokens).To(Equal(int64(0)))
				Expect(body.OutputTokens).To(Equal(int64(0)))
				Expect(body.TotalDurationNs).To(Equal(int64(0)))
				Expect(body.ToolCalls).To(Equal(0))
			})
		})
	})
})

func putNode(ctx context.Context, d storage.Driver, n *merkle.Node) error {
	_, err := d.Put(ctx, n)
	return err
}

func decodeList(server *Server, path string) SessionListResponse {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
	defer resp.Body.Close()
	var body SessionListResponse
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(json.Unmarshal(raw, &body)).To(Succeed())
	return body
}

func decodeSession(server *Server, path string) SessionResponse {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
	defer resp.Body.Close()
	var body SessionResponse
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(json.Unmarshal(raw, &body)).To(Succeed())
	return body
}

func decodeStats(server *Server, path string) StatsResponse {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
	defer resp.Body.Close()
	var body StatsResponse
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	Expect(json.Unmarshal(raw, &body)).To(Succeed())
	return body
}

func itemHashes(items []SessionListItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Hash
	}
	return out
}
