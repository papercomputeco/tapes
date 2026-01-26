// Package merkle is an implementation of a Merkel DAG
package merkle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
)

// Node represents a single content-addressed node in a Merkle DAG
type Node struct {
	// Hash is the content-addressed identifier (SHA-256, hex-encoded)
	Hash string `json:"hash"`

	// ParentHash links to the previous node hash.
	// This will be nil for root nodes.
	ParentHash *string `json:"parent_hash"`

	// Bucket is the hashable content for the node
	Bucket Bucket `json:"bucket"`
}

// NewNode creates a new node with the computed hash for the provided bucket
func NewNode(bucket Bucket, parent *Node) *Node {
	n := &Node{
		Bucket: bucket,
	}

	if parent != nil {
		n.ParentHash = &parent.Hash
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
