package derive_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

func chainRequest() *llm.ChatRequest {
	maxTokens := 4096
	temp := 0.7
	stream := true
	return &llm.ChatRequest{
		Model:  "claude-test",
		System: "You are a helpful assistant.",
		Messages: []llm.Message{
			{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "hello"}}},
			{Role: "assistant", Content: []llm.ContentBlock{{Type: "text", Text: "hi there"}}},
			{Role: "user", Content: []llm.ContentBlock{{Type: "text", Text: "do the thing"}}},
		},
		MaxTokens:   &maxTokens,
		Temperature: &temp,
		Stream:      &stream,
		Tools:       []json.RawMessage{json.RawMessage(`{"name":"Bash"}`), json.RawMessage(`{"name":"Read"}`)},
	}
}

func chainResponse() *llm.ChatResponse {
	return &llm.ChatResponse{
		Model: "claude-test",
		Message: llm.Message{
			Role:    "assistant",
			Content: []llm.ContentBlock{{Type: "text", Text: "done"}},
		},
		StopReason: "end_turn",
		Usage:      &llm.Usage{PromptTokens: 10, CompletionTokens: 2, TotalTokens: 12},
	}
}

var _ = Describe("TurnChain", func() {
	It("builds a root→leaf chain with one node per message plus the response", func() {
		chain := derive.TurnChain(derive.CallContext{Provider: "anthropic", AgentName: "claude", Project: "proj"}, chainRequest(), chainResponse())
		Expect(chain).To(HaveLen(4))

		Expect(chain[0].ParentHash).To(BeNil())
		for i := 1; i < len(chain); i++ {
			Expect(chain[i].ParentHash).NotTo(BeNil())
			Expect(*chain[i].ParentHash).To(Equal(chain[i-1].Hash))
		}

		leaf := chain[len(chain)-1]
		Expect(leaf.Bucket.Role).To(Equal("assistant"))
		Expect(leaf.StopReason).To(Equal("end_turn"))
		Expect(leaf.Usage).NotTo(BeNil())
	})

	It("stamps the request params on every node without changing hashes", func() {
		req := chainRequest()
		resp := chainResponse()
		chain := derive.TurnChain(derive.CallContext{Provider: "anthropic", AgentName: "claude", Project: "proj"}, req, resp)

		for _, node := range chain {
			Expect(node.Request).NotTo(BeNil())
			Expect(node.Request.System).To(Equal("You are a helpful assistant."))
			Expect(node.Request.MaxTokens).To(HaveValue(Equal(4096)))
			Expect(node.Request.Stream).To(HaveValue(BeTrue()))
			Expect(node.Request.ToolCount).To(HaveValue(Equal(2)))
		}

		// The params are call metadata, NOT hashed content: the same
		// logical turn sent by a different kind of call (no tools, no
		// stream, different system prompt) must produce identical
		// hashes, or the conversation chain would fork per call kind.
		bare := &llm.ChatRequest{Model: req.Model, Messages: req.Messages}
		bareChain := derive.TurnChain(derive.CallContext{Provider: "anthropic", AgentName: "claude", Project: "proj"}, bare, resp)
		Expect(bareChain).To(HaveLen(len(chain)))
		for i := range chain {
			Expect(bareChain[i].Hash).To(Equal(chain[i].Hash))
		}
	})

	It("reports zero tools as a concrete count, distinct from absent params", func() {
		req := chainRequest()
		req.Tools = nil
		chain := derive.TurnChain(derive.CallContext{Provider: "anthropic", AgentName: "claude"}, req, chainResponse())
		Expect(chain[0].Request.ToolCount).To(HaveValue(Equal(0)))
	})

	It("returns nil for a missing request or response", func() {
		Expect(derive.TurnChain(derive.CallContext{Provider: "anthropic"}, nil, chainResponse())).To(BeNil())
		Expect(derive.TurnChain(derive.CallContext{Provider: "anthropic"}, chainRequest(), nil)).To(BeNil())
	})

	It("re-derives identical hashes from the same raw provider request", func() {
		// The raw→derived round-trip in miniature: the same verbatim
		// provider JSON parsed twice must chain to the same hashes.
		// This is the property the admin derive/verify endpoint checks
		// against the live store.
		rawRequest := []byte(`{
			"model": "claude-test",
			"max_tokens": 64,
			"system": "You are a security monitor.",
			"messages": [
				{"role": "user", "content": "<transcript>Bash ls -la</transcript>"}
			]
		}`)
		prov, err := provider.New("anthropic")
		Expect(err).NotTo(HaveOccurred())

		req1, err := prov.ParseRequest(rawRequest)
		Expect(err).NotTo(HaveOccurred())
		req2, err := prov.ParseRequest(rawRequest)
		Expect(err).NotTo(HaveOccurred())

		resp := chainResponse()
		chain1 := derive.TurnChain(derive.CallContext{Provider: "anthropic", AgentName: "claude", Project: "proj"}, req1, resp)
		chain2 := derive.TurnChain(derive.CallContext{Provider: "anthropic", AgentName: "claude", Project: "proj"}, req2, resp)
		Expect(chain1).To(HaveLen(2))
		for i := range chain1 {
			Expect(chain2[i].Hash).To(Equal(chain1[i].Hash))
		}

		// And the security-monitor tells survive onto the node.
		Expect(chain1[0].Request.System).To(Equal("You are a security monitor."))
		Expect(chain1[0].Request.MaxTokens).To(HaveValue(Equal(64)))
		Expect(chain1[0].Request.ToolCount).To(HaveValue(Equal(0)))
		Expect(chain1[0].Request.Stream).To(BeNil())
	})
})

var _ = Describe("merkle.Node request params", func() {
	It("does not let params participate in the content hash", func() {
		bucket := merkle.Bucket{
			Type:    "message",
			Role:    "user",
			Content: []llm.ContentBlock{{Type: "text", Text: "same content"}},
		}
		mt := 64
		withParams := merkle.NewNode(bucket, nil, merkle.NodeOptions{
			Request: &llm.RequestParams{System: "You are a security monitor.", MaxTokens: &mt},
		})
		withoutParams := merkle.NewNode(bucket, nil)
		Expect(withParams.Hash).To(Equal(withoutParams.Hash))
	})
})
