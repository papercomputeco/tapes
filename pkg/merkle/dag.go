package merkle

import (
	"errors"
	"fmt"
)

// Dag is an in-memory view of a single-rooted Merkle DAG (i.e., a single branch
// within the graph's plane).
//
// It is loaded on-demand from a provided BranchLoader.
type Dag struct {
	// Root is the single root node of this directed graph
	Root *DagNode

	// index provides O(1) lookup by node hash
	index map[string]*DagNode
}

// DagNode wraps a "Node" with structural relationships for efficient traversal.
type DagNode struct {
	*Node

	// Parent is the parent node in the DAG (nil for root)
	Parent *DagNode

	// Children are the child nodes (empty for leaves)
	Children []*DagNode
}

func NewDag() *Dag {
	return &Dag{
		index: make(map[string]*DagNode),
	}
}

// Get returns the DagNode with the given hash, or nil if not found.
func (d *Dag) Get(hash string) *DagNode {
	return d.index[hash]
}

// Size returns the total number of nodes in the DAG.
func (d *Dag) Size() int {
	return len(d.index)
}

// Leaves returns all leaf nodes (nodes with no children).
func (d *Dag) Leaves() []*DagNode {
	leaves := []*DagNode{}

	for _, node := range d.index {
		if len(node.Children) == 0 {
			leaves = append(leaves, node)
		}
	}

	return leaves
}

// Walk traverses the DAG depth-first from root, calling fn for each node.
// If the provided function returns false, traversal stops.
// If the provided function errors, traversal stops and the error is propagated.
func (d *Dag) Walk(f func(*DagNode) (bool, error)) error {
	if d.Root == nil {
		return nil
	}

	_, err := d.walkNode(d.Root, f)
	return err
}

// walkNode recursively, depth first, traverses the given node with the provided
// function
func (d *Dag) walkNode(node *DagNode, f func(*DagNode) (bool, error)) (bool, error) {
	ok, err := f(node)
	if !ok || err != nil {
		return false, err
	}

	for _, child := range node.Children {
		ok, err := d.walkNode(child, f)
		if !ok || err != nil {
			return false, err
		}
	}

	return true, nil
}

// Ancestors returns the path from the given node up to the root.
// The returned slice is ordered from the node to root (node first, root last).
// Returns nil if the hash is not found.
func (d *Dag) Ancestors(hash string) []*DagNode {
	node := d.Get(hash)
	if node == nil {
		return nil
	}

	ancestors := []*DagNode{}
	current := node
	for current != nil {
		ancestors = append(ancestors, current)
		current = current.Parent
	}

	return ancestors
}

// Descendants returns all descendants of the given node (children, grandchildren, etc.).
// The returned slice is ordered by depth-first traversal.
// Returns nil if the hash is not found.
func (d *Dag) Descendants(hash string) []*DagNode {
	node := d.Get(hash)
	if node == nil {
		return nil
	}

	descendants := []*DagNode{}
	_ = d.Walk(func(n *DagNode) (bool, error) {
		descendants = append(descendants, n.Children...)
		return true, nil
	})

	return descendants
}

// IsBranching returns true if the node with the given hash has multiple children.
// Returns false if the hash is not found or has 0-1 children.
func (d *Dag) IsBranching(hash string) bool {
	node := d.Get(hash)
	if node == nil {
		return false
	}
	return len(node.Children) > 1
}

// BranchPoints returns all nodes that have more than one child.
func (d *Dag) BranchPoints() []*DagNode {
	branchPoints := []*DagNode{}
	for _, node := range d.index {
		if len(node.Children) > 1 {
			branchPoints = append(branchPoints, node)
		}
	}
	return branchPoints
}

// AddNode adds a node to the given DAG.
// The node's ParentHash must reference an existing node in the DAG,
// or be nil (making it the root).
//
// This method returns an error if:
//   - The provided node is nil: this is a programmer error.
//   - The parent hash references a node not in the DAG (this node does not belong
//     in this branch)
//   - Adding a root node (where node.Parent is empty) when one already exists
//
// This method is a noop if:
//   - The node already exists in the DAG
func (d *Dag) AddNode(node *Node) (*DagNode, error) {
	if node == nil {
		return nil, errors.New("cannot add nil node to dag")
	}

	dagNode, ok := d.index[node.Hash]
	if ok {
		return dagNode, nil
	}

	dagNode = &DagNode{
		Node:     node,
		Children: make([]*DagNode, 0),
	}

	if node.ParentHash == nil {
		// This is a root node
		if d.Root != nil {
			return nil, errors.New("DAG already has a root node")
		}

		d.Root = dagNode
	} else {
		// Find and link to parent
		parent, ok := d.index[*node.ParentHash]
		if !ok {
			return nil, fmt.Errorf("parent node %s not found in dag", *node.ParentHash)
		}

		dagNode.Parent = parent
		parent.Children = append(parent.Children, dagNode)
	}

	d.index[node.Hash] = dagNode
	return dagNode, nil
}
