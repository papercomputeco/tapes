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
	"github.com/papercomputeco/tapes/pkg/llm"
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
func TurnChain(provider, agentName, project string, req *llm.ChatRequest, resp *llm.ChatResponse) []*merkle.Node {
	if req == nil || resp == nil {
		return nil
	}

	kind := ClassifyCall(req, resp)
	params := req.Params()
	chain := make([]*merkle.Node, 0, len(req.Messages)+1)
	var parent *merkle.Node

	for _, msg := range req.Messages {
		bucket := merkle.Bucket{
			Type:      "message",
			Role:      msg.Role,
			Content:   msg.Content,
			Model:     req.Model,
			Provider:  provider,
			AgentName: agentName,
		}
		node := merkle.NewNode(bucket, parent, merkle.NodeOptions{
			Project: project,
			Request: params,
		})
		if injectedKind := ClassifyInjected(msg); injectedKind != "" && kind == KindMain {
			// Side branch: keep the node, mark it, do NOT advance the
			// spine. Only meaningful on the conversation spine — a
			// shadow call's whole body is already typed by its kind.
			node.Kind = injectedKind
			chain = append(chain, node)
			continue
		}
		node.Kind = kind
		chain = append(chain, node)
		parent = node
	}

	responseBucket := merkle.Bucket{
		Type:      "message",
		Role:      resp.Message.Role,
		Content:   resp.Message.Content,
		Model:     resp.Model,
		Provider:  provider,
		AgentName: agentName,
	}
	responseNode := merkle.NewNode(
		responseBucket,
		parent,
		merkle.NodeOptions{
			StopReason: resp.StopReason,
			Usage:      resp.Usage,
			Project:    project,
			Request:    params,
		},
	)
	responseNode.Kind = kind
	chain = append(chain, responseNode)
	return chain
}
