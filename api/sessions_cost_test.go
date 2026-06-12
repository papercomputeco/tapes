package api

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// assistantCostNode builds a standalone assistant node carrying a model and
// token usage — the only fields sessionCostFromNodes reads.
func assistantCostNode(model string, in, out, cacheCreate, cacheRead int) *merkle.Node {
	bucket := v1TestBucket("assistant", "ok", model, "anthropic", "")
	return merkle.NewNode(bucket, nil, merkle.NodeOptions{
		Usage: &llm.Usage{
			PromptTokens:             in,
			CompletionTokens:         out,
			CacheCreationInputTokens: cacheCreate,
			CacheReadInputTokens:     cacheRead,
		},
	})
}

var _ = Describe("sessionCostFromNodes", func() {
	pricing := sessions.DefaultPricing()

	It("folds a single priced model's tokens into USD", func() {
		// claude-sonnet-4.6: $3/M in, $15/M out. 1M in + 1M out = $18.
		nodes := []*merkle.Node{
			assistantCostNode("claude-sonnet-4.6", 600_000, 400_000, 0, 0),
			assistantCostNode("claude-sonnet-4.6", 400_000, 600_000, 0, 0),
		}
		Expect(sessionCostFromNodes(nodes, pricing)).To(BeNumerically("~", 18.0, 0.0001))
	})

	It("sums cost across multiple models", func() {
		// sonnet 1M in → $3; haiku-4.5 ($1/M in, $5/M out) 1M out → $5; total $8.
		nodes := []*merkle.Node{
			assistantCostNode("claude-sonnet-4.6", 1_000_000, 0, 0, 0),
			assistantCostNode("claude-haiku-4.5", 0, 1_000_000, 0, 0),
		}
		Expect(sessionCostFromNodes(nodes, pricing)).To(BeNumerically("~", 8.0, 0.0001))
	})

	It("ignores nodes without usage and unpriceable models", func() {
		userBucket := v1TestBucket("user", "hi", "", "", "")
		nodes := []*merkle.Node{
			merkle.NewNode(userBucket, nil),                                        // no usage → 0
			assistantCostNode("totally-unknown-model", 1_000_000, 1_000_000, 0, 0), // unpriced → 0
			assistantCostNode("claude-sonnet-4.6", 1_000_000, 0, 0, 0),             // $3
		}
		Expect(sessionCostFromNodes(nodes, pricing)).To(BeNumerically("~", 3.0, 0.0001))
	})

	It("is zero for no nodes", func() {
		Expect(sessionCostFromNodes(nil, pricing)).To(Equal(0.0))
	})
})
