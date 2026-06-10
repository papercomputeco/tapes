// Package merkle is an implementation of a Merkel DAG
package merkle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Node represents a single content-addressed node in a Merkle DAG
type Node struct {
	// Hash is the content-addressed identifier (SHA-256, hex-encoded)
	Hash string `json:"hash"`

	// ParentHash links to the previous node hash.
	// This will be nil for root nodes.
	ParentHash *string `json:"parent_hash"`

	// Bucket is the hashable content for the node.
	Bucket Bucket `json:"bucket"`

	// StopReason indicates why generation stopped (only for responses)
	// Values: "stop", "length", "tool_use", "end_turn", etc.
	StopReason string `json:"stop_reason,omitempty"`

	// Usage contains token counts and timing (only for responses)
	Usage *llm.Usage `json:"usage,omitempty"`

	// Project is the git repository or project name that produced this node
	Project string `json:"project,omitempty"`

	// Kind is the semantic classification of the call (or injected
	// block) that produced this node — 'main', 'offshoot:…',
	// 'injected:…' per the design taxonomy. Derived metadata, NOT part
	// of the content-addressed hash, and recomputable from the raw
	// layer at any time.
	Kind string `json:"node_kind,omitempty"`

	// ThreadID is the harness sub-thread that fired the capturing
	// call (Claude Code: the subagent agent-id), "" for main-thread
	// calls. Captured deterministically at the wire; non-hashed.
	ThreadID string `json:"thread_id,omitempty"`

	// ParentToolUseID is the semantic fork/attach edge: the tool_use id
	// this node's call relates to (a permission verdict points at the
	// tool_use it judged; a subagent fork points at its Task tool_use).
	// Derived, non-hashed, recomputable.
	ParentToolUseID string `json:"parent_tool_use_id,omitempty"`

	// Request carries the request-envelope parameters of the API call
	// that captured this node (system prompt, max_tokens, temperature,
	// stream, tool count). Like StopReason/Usage it is call metadata,
	// NOT part of the content-addressed hash: the same logical turn
	// keeps the same hash regardless of which kind of call sent it.
	Request *llm.RequestParams `json:"request_params,omitempty"`

	// CreatedAt is the time the node was persisted to storage. It is populated
	// by the storage layer (not by NewNode) and is NOT part of the content hash.
	// Zero value means "unknown" — typically for nodes constructed in-memory
	// that have not yet been Put.
	CreatedAt time.Time `json:"created_at,omitzero"`
}

// NodeOptions contains optional metadata for a new node that is stored
// but does not affect the content-addressable hashing.
type NodeOptions struct {
	StopReason string
	Usage      *llm.Usage
	Project    string
	Request    *llm.RequestParams
}

// NewNode creates a new node with the computed hash for the provided bucket.
// The optional NodeOptions parameter allows for setting metadata (StopReason, Usage, etc.)
// outside of the content addressable Bucket
func NewNode(bucket Bucket, parent *Node, opts ...NodeOptions) *Node {
	n := &Node{
		Bucket: bucket,
	}

	if parent != nil {
		n.ParentHash = &parent.Hash
	}

	// Apply optional metadata if provided
	if len(opts) > 0 {
		n.StopReason = opts[0].StopReason
		n.Usage = opts[0].Usage
		n.Project = opts[0].Project
		n.Request = opts[0].Request
	}

	n.Hash = n.computeHash()
	return n
}

// ComputeHash calculates the content-addressed hash for a node
func (n *Node) computeHash() string {
	parent := ""
	if n.ParentHash != nil {
		parent = *n.ParentHash
	}

	// Marshal to JSON using an inline struct for hash computation.
	//
	// The hash is computed over a projection of the bucket, not the
	// bucket itself, so that "the same logical conversation turn"
	// hashes the same across the kinds of drift agent harnesses
	// introduce between requests in a single conversation. Concretely:
	//
	//   * Content text inside <system-reminder>, <command-*>, and
	//     <local-command-*> spans is stripped (the harness rotates
	//     this preamble — clock ticks, skills load, MCP inventory
	//     shifts).
	//   * Blank-line whitespace drift in the surviving prose is
	//     normalized (the harness re-serializes user text and
	//     occasionally inserts a stray newline).
	//   * Zero-valued keys are pruned from tool_use ToolInput so the
	//     streamed capture (which has Edit's "replace_all": false)
	//     dedups with the re-sent history (which omits the default).
	//   * ThinkingSignature is dropped — present on the live stream,
	//     absent when the harness re-sends the same thinking block.
	//   * Routing metadata — Model, Provider, AgentName — is dropped
	//     entirely. A single conversation may legitimately fan a
	//     pre-flight call to Haiku and the main work to Opus; that is
	//     a routing decision, not a new turn.
	//
	// The raw n.Bucket is left untouched so storage / display / labels
	// / search still see exactly what the model received, including
	// model identity. See PCC-562.
	type hashableBucket struct {
		Type    string             `json:"type"`
		Role    string             `json:"role"`
		Content []llm.ContentBlock `json:"content"`
	}

	data, err := json.Marshal(struct {
		Parent  string         `json:"parent"`
		Content hashableBucket `json:"content"`
	}{
		Parent: parent,
		Content: hashableBucket{
			Type:    n.Bucket.Type,
			Role:    n.Bucket.Role,
			Content: ProjectContent(n.Bucket.Content),
		},
	})
	if err != nil {
		panic("failed to marshal hash input: " + err.Error())
	}

	// Canonicalize the nodes content JSON according to RFC 8785.
	// This, as of Go 1.25.x, requires "GOEXPERIMENT=jsonv2" for the new json v2
	// and jsontext packages to properly canonicalize the payload.
	// This effectively ensures that JSON blob hexes from one proxy run to the next
	// are the same.
	j := jsontext.Value(data)
	err = j.Canonicalize()
	if err != nil {
		panic("failed to canonicalize JSON: " + err.Error())
	}

	h := sha256.Sum256(j)
	return hex.EncodeToString(h[:])
}
