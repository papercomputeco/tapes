package capture_test

import (
	"bytes"
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/capture"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider/anthropic"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// bucketFromResp builds the merkle.Bucket the worker pool would assemble
// from a ChatResponse. Provider and AgentName are test-fixed so both sides
// of the equivalence comparison hash against the same bucket shape — the
// only inputs that should vary are the reducer outputs themselves.
//
// Matches the projection in proxy/worker/pool.go: the DAG hash depends on
// {Type, Role, Content, Model, Provider, AgentName} and nothing else. Fields
// like CreatedAt, RawResponse, Done, StopReason, Usage, Extra ride outside
// the Bucket and are intentionally excluded.
func bucketFromResp(resp *llm.ChatResponse) merkle.Bucket {
	return merkle.Bucket{
		Type:      "message",
		Role:      resp.Message.Role,
		Content:   resp.Message.Content,
		Model:     resp.Model,
		Provider:  "anthropic",
		AgentName: "canonical-equivalence",
	}
}

var _ = Describe("Canonical equivalence", func() {
	ctx := context.Background()
	r := capture.NewAnthropicReducer()

	type pair struct {
		name        string
		oneshotFile string
		streamFile  string
	}

	// Each case asserts that reducing the streamed form of a turn and
	// parsing the oneshot form produce buckets that hash to the same
	// merkle node — i.e. the DAG dedups them. The hash is computed via
	// merkle.NewNode, which is the exact code path the worker pool
	// invokes in production, so passing this test means the property the
	// DAG actually cares about holds.
	DescribeTable("stream and oneshot reduce to the same merkle hash",
		func(p pair) {
			oneshotRaw := readFixture("canonical_equivalence/" + p.oneshotFile)
			streamRaw := readFixture("canonical_equivalence/" + p.streamFile)

			prov := anthropic.New()
			parsedOneshot, err := prov.ParseResponse(oneshotRaw)
			Expect(err).NotTo(HaveOccurred())

			reducedStream, err := r.Reduce(ctx, nil, bytes.NewReader(streamRaw), "text/event-stream")
			Expect(err).NotTo(HaveOccurred())

			oneshotNode := merkle.NewNode(bucketFromResp(parsedOneshot), nil)
			streamNode := merkle.NewNode(bucketFromResp(reducedStream), nil)

			if oneshotNode.Hash != streamNode.Hash {
				Fail("merkle hash differs for " + p.name +
					"\n  oneshot: " + oneshotNode.Hash +
					"\n  stream : " + streamNode.Hash)
			}
		},
		Entry("turn_01 text-only",
			pair{"turn_01 text-only", "turn_01_oneshot.json", "turn_01_stream.sse"}),
		Entry("turn_02 tool_use",
			pair{"turn_02 tool_use", "turn_02_tool_use_oneshot.json", "turn_02_tool_use_stream.sse"}),
		Entry("turn_03 thinking",
			pair{"turn_03 thinking", "turn_03_thinking_oneshot.json", "turn_03_thinking_stream.sse"}),
		Entry("turn_04 server_tool_use + web_search_tool_result",
			pair{"turn_04 server_tool_use + web_search_tool_result", "turn_04_server_tool_oneshot.json", "turn_04_server_tool_stream.sse"}),
	)
})
