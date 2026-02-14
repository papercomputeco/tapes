// Package merkle is an implementation of a Merkel DAG
package merkle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"

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
}

// NodeMeta contains optional metadata for a node that is stored
// but does not affect the content-addressable hash.
type NodeMeta struct {
	StopReason string
	Usage      *llm.Usage
	Project    string
}

// NewNode creates a new node with the computed hash for the provided bucket.
// The optional NodeOptions parameter allows for setting metadata (StopReason, Usage, etc.)
// outside of the content addressable Bucket
func NewNode(bucket Bucket, parent *Node, metas ...NodeMeta) *Node {
	n := &Node{
		Bucket: bucket,
	}

	if parent != nil {
		n.ParentHash = &parent.Hash
	}

	// Apply optional metadata if provided
	if len(metas) > 0 {
		n.StopReason = metas[0].StopReason
		n.Usage = metas[0].Usage
		n.Project = metas[0].Project
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

	// Marshal to JSON using an inline struct for hash computation
	data, err := json.Marshal(struct {
		Parent  string `json:"parent"`
		Content Bucket `json:"content"`
	}{
		Parent:  parent,
		Content: n.Bucket,
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
