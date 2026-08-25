// Package derive turns immutable raw captures into the derived layer:
// content-addressed node chains today; semantic typing (node_kind),
// fork edges, and offshoot classification as the reconciled
// conversation tree lands (design/agent-session-reconciliation.md).
//
// Everything in this package is a pure, re-runnable function of the
// raw capture. The ingest worker uses the same TurnChain at capture
// time that the re-deriver uses offline, so "re-deriving nodes from
// raw_turns reproduces the captured nodes" holds by construction.
package derive

import (
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/llm/provider/openai"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// TurnChain materializes the ordered (root → leaf) chain of nodes for a
// single captured call: one node per request message, followed by the
// assistant response node. ParentHash linkage is set via merkle.NewNode
// so every node's hash is stable before any I/O.
//
// Every node is stamped with the call's classified kind (ClassifyCall)
// and the request-envelope parameters — both as non-hashed metadata. On
// insert, nodes already present from an earlier call keep that call's
// stamp (ON CONFLICT DO NOTHING), so each stored node carries the
// classification of the call that first captured it.
//
// Injected-context messages (MCP instructions, skills lists, mode
// banners — see ClassifyInjected) become SIDE-BRANCH nodes: they hang
// off the spine at the position they appeared but the next message's
// parent bypasses them. These blocks drift between turns of the same
// conversation, so chaining them would fork the spine at every drift;
// as side branches they are preserved, marked injected:*, and inert.
// CallContext carries the capture-side identity of one API call into
// chain construction: who routed it (provider/agent), which harness
// sub-thread fired it, and the project tag. All non-hashed metadata.
type CallContext struct {
	Provider  string
	AgentName string
	ThreadID  string
	Project   string
}

func TurnChain(call CallContext, req *llm.ChatRequest, resp *llm.ChatResponse) []*merkle.Node {
	return turnChain(call, req, resp, chainOptions{
		kind:             ClassifyCall(req, resp),
		classifyInjected: true,
		request:          req.Params(),
	})
}

// chainOptions lets transcript normalization reuse the canonical message and
// response node constructor without pretending a transcript carried a wire
// request envelope. Transcript records already classify context individually
// and carry per-record timestamps, so their caller supplies those details and
// keeps context on its observed causal spine.
type chainOptions struct {
	kind             string
	classifyInjected bool
	request          *llm.RequestParams
	messageKinds     []string
	capturedAt       []time.Time
}

func turnChain(call CallContext, req *llm.ChatRequest, resp *llm.ChatResponse, opts chainOptions) []*merkle.Node {
	if req == nil || resp == nil {
		return nil
	}

	chain := make([]*merkle.Node, 0, len(req.Messages)+1)
	var parent *merkle.Node

	for i, msg := range req.Messages {
		bucket := merkle.Bucket{
			Type:      "message",
			Role:      msg.Role,
			Content:   msg.Content,
			Model:     req.Model,
			Provider:  call.Provider,
			AgentName: call.AgentName,
		}
		node := merkle.NewNode(bucket, parent, merkle.NodeOptions{
			Project: call.Project,
			Request: opts.request,
		})
		node.ThreadID = call.ThreadID
		if i < len(opts.capturedAt) {
			node.CreatedAt = opts.capturedAt[i]
		}
		if opts.classifyInjected {
			if injectedKind := ClassifyInjected(msg); injectedKind != "" {
				// Side branch: keep the node, mark it, do NOT advance the
				// spine. On the conversation spine this stops injected
				// drift from forking the chain; on shadow calls it stops
				// a shared context block (every permission check opens
				// with the same <user_claude_md> blob) from fusing
				// otherwise-independent calls into one fan.
				node.Kind = injectedKind
				chain = append(chain, node)
				continue
			}
		}
		node.Kind = opts.kind
		if i < len(opts.messageKinds) && opts.messageKinds[i] != "" {
			node.Kind = opts.messageKinds[i]
		}
		chain = append(chain, node)
		parent = node
	}

	// OpenAI Responses items the reducer preserved verbatim
	// (custom_tool_call, custom_tool_call_output) normalize to
	// canonical tool blocks HERE — the shared constructor — so
	// capture-time ingest and offline re-derive produce identical
	// hashes, and sessions captured before those item types were
	// cataloged heal on re-derive.
	responseContent := resp.Message.Content
	if call.Provider == "openai" {
		responseContent = openai.NormalizeResponsesContent(responseContent)
	}

	responseBucket := merkle.Bucket{
		Type:      "message",
		Role:      resp.Message.Role,
		Content:   responseContent,
		Model:     resp.Model,
		Provider:  call.Provider,
		AgentName: call.AgentName,
	}
	responseNode := merkle.NewNode(
		responseBucket,
		parent,
		merkle.NodeOptions{
			StopReason: resp.StopReason,
			Usage:      resp.Usage,
			Project:    call.Project,
			Request:    opts.request,
		},
	)
	responseNode.Kind = opts.kind
	responseNode.ThreadID = call.ThreadID
	if len(opts.capturedAt) > len(req.Messages) {
		responseNode.CreatedAt = opts.capturedAt[len(req.Messages)]
	}
	chain = append(chain, responseNode)
	return chain
}
