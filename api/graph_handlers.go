package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

const (
	defaultGraphMaxNodes = 500
	upperGraphMaxNodes   = 5000
)

// GraphResponse is the graph-shaped projection for GET /v1/stems/:hash/graph.
type GraphResponse struct {
	// Hash is the session/node hash requested by the caller.
	Hash string `json:"hash"`

	// RootHash is the top-most resolvable node included in the response.
	RootHash string `json:"root_hash"`

	// Scope describes which portion of the graph was loaded: root, branch, or ancestry.
	Scope string `json:"scope"`

	// NodeLimit is the maximum number of nodes the server will include.
	NodeLimit int `json:"node_limit"`

	// Nodes is the flat node list consumed by graph visualizers.
	Nodes []GraphNode `json:"nodes"`

	// Links contains parent -> child edges between included nodes.
	Links []GraphLink `json:"links"`

	// Leaves names included nodes that have no children in storage.
	Leaves []string `json:"leaves"`

	// BranchPoints names included nodes with more than one child in storage.
	BranchPoints []string `json:"branch_points"`

	// Truncated is true when the graph hit NodeLimit or the ancestry chain is incomplete.
	Truncated bool `json:"truncated,omitempty"`

	// MissingParent names the unresolved parent hash when the ancestry is incomplete.
	MissingParent string `json:"missing_parent,omitempty"`

	// CycleDetected is true when storage guarded the ancestry walk out of a cycle.
	CycleDetected bool `json:"cycle_detected,omitempty"`
}

// GraphNode is the per-node shape used by the web UI graph visualization.
type GraphNode struct {
	ID            string     `json:"id"`
	ParentID      *string    `json:"parent_id,omitempty"`
	ParentHash    *string    `json:"parent_hash,omitempty"`
	Type          string     `json:"type,omitempty"`
	Role          string     `json:"role,omitempty"`
	Preview       string     `json:"preview,omitempty"`
	Model         string     `json:"model,omitempty"`
	Provider      string     `json:"provider,omitempty"`
	AgentName     string     `json:"agent_name,omitempty"`
	Project       string     `json:"project,omitempty"`
	StopReason    string     `json:"stop_reason,omitempty"`
	Usage         *llm.Usage `json:"usage,omitempty"`
	CreatedAt     time.Time  `json:"created_at,omitzero"`
	Depth         int        `json:"depth"`
	ChildrenCount int        `json:"children_count"`
	IsRoot        bool       `json:"is_root"`
	IsLeaf        bool       `json:"is_leaf"`
	IsBranchPoint bool       `json:"is_branch_point"`
	Selected      bool       `json:"selected"`

	// NodeKind / ParentToolUseID surface the derived semantic typing
	// (additive; empty for nodes that predate the deriver).
	NodeKind        string `json:"node_kind,omitempty"`
	ParentToolUseID string `json:"parent_tool_use_id,omitempty"`
	ThreadID        string `json:"thread_id,omitempty"`
}

// GraphLink is a directed parent -> child edge between two included GraphNode IDs.
type GraphLink struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

// handleGetStemGraph handles GET /v1/stems/:hash/graph.
//
//	@Summary		Get a stem graph
//	@Description	Returns a graph-shaped projection of the Merkle DAG around a node hash (the same ancestry returned by GET /v1/stems/{hash}). scope=root loads the requested node's resolvable root and all descendants, scope=branch loads the ancestry plus descendants of the requested node, and scope=ancestry loads only the parent chain.
//	@Tags			stems
//	@Produce		json
//	@Param			hash		path	string	true	"Node (stem head) hash"
//	@Param			scope		query	string	false	"Graph scope: root, branch, or ancestry" Enums(root, branch, ancestry)
//	@Param			max_nodes	query	int		false	"Maximum number of graph nodes to include" minimum(1) maximum(5000)
//	@Success		200			{object}	GraphResponse
//	@Failure		400			{object}	llm.ErrorResponse	"Missing hash or invalid query parameters"
//	@Failure		404			{object}	llm.ErrorResponse	"Stem not found"
//	@Failure		500			{object}	llm.ErrorResponse	"Failed to load stem graph"
//	@Router			/v1/stems/{hash}/graph [get]
func (s *Server) handleGetStemGraph(c *fiber.Ctx) error {
	hash, chain, err := s.loadSessionChain(c)
	if err != nil {
		return s.handleLoadSessionChainError(c, hash, err)
	}

	scope := c.Query("scope", "root")
	if !validGraphScope(scope) {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "scope must be one of: root, branch, ancestry"})
	}

	maxNodes, err := parseGraphMaxNodes(c.Query("max_nodes"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
	}

	builder := newGraphResponseBuilder(s.driver, hash, scope, maxNodes)
	resp, err := builder.build(c.Context(), chain)
	if err != nil {
		s.logger.Error("build session graph response", "hash", hash, "scope", scope, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session graph"})
	}

	return c.JSON(resp)
}

func validGraphScope(scope string) bool {
	switch scope {
	case "root", "branch", "ancestry":
		return true
	default:
		return false
	}
}

func parseGraphMaxNodes(raw string) (int, error) {
	if raw == "" {
		return defaultGraphMaxNodes, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, errors.New("max_nodes must be a positive integer")
	}
	if parsed > upperGraphMaxNodes {
		return 0, fmt.Errorf("max_nodes must be less than or equal to %d", upperGraphMaxNodes)
	}
	return parsed, nil
}

type graphResponseBuilder struct {
	driver storage.Driver

	requestedHash string
	scope         string
	maxNodes      int

	resp     *GraphResponse
	seenNode map[string]struct{}
	seenLink map[string]struct{}
	children map[string][]*merkle.Node
}

func newGraphResponseBuilder(driver storage.Driver, requestedHash, scope string, maxNodes int) *graphResponseBuilder {
	return &graphResponseBuilder{
		driver:        driver,
		requestedHash: requestedHash,
		scope:         scope,
		maxNodes:      maxNodes,
		resp: &GraphResponse{
			Hash:         requestedHash,
			Scope:        scope,
			NodeLimit:    maxNodes,
			Nodes:        []GraphNode{},
			Links:        []GraphLink{},
			Leaves:       []string{},
			BranchPoints: []string{},
		},
		seenNode: make(map[string]struct{}),
		seenLink: make(map[string]struct{}),
		children: make(map[string][]*merkle.Node),
	}
}

func (b *graphResponseBuilder) build(ctx context.Context, chain *storage.Chain) (*GraphResponse, error) {
	rootFirst := reverseNodes(chain.Nodes)
	b.resp.RootHash = rootFirst[0].Hash
	b.resp.Truncated = chain.Incomplete
	b.resp.MissingParent = chain.MissingParent
	b.resp.CycleDetected = chain.CycleDetected

	switch b.scope {
	case "root":
		if ok, err := b.addAncestryPath(ctx, rootFirst); err != nil || !ok {
			return b.resp, err
		}
		pathSeen := map[string]struct{}{}
		for i, node := range rootFirst {
			pathSeen[node.Hash] = struct{}{}
			var skip map[string]struct{}
			if i+1 < len(rootFirst) {
				skip = map[string]struct{}{rootFirst[i+1].Hash: {}}
			}
			if err := b.addDescendants(ctx, node, i, pathSeen, skip); err != nil {
				return nil, err
			}
		}
	case "branch":
		if ok, err := b.addAncestryPath(ctx, rootFirst); err != nil || !ok {
			return b.resp, err
		}
		if err := b.addDescendants(ctx, rootFirst[len(rootFirst)-1], len(rootFirst)-1, hashesFromNodes(rootFirst), nil); err != nil {
			return nil, err
		}
	case "ancestry":
		if _, err := b.addAncestryPath(ctx, rootFirst); err != nil {
			return b.resp, err
		}
	}

	return b.resp, nil
}

func (b *graphResponseBuilder) addAncestryPath(ctx context.Context, rootFirst []*merkle.Node) (bool, error) {
	for i, node := range rootFirst {
		var parentID *string
		if i > 0 {
			parentID = stringPtr(rootFirst[i-1].Hash)
		}
		if ok, err := b.addNode(ctx, node, parentID, i); err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

func (b *graphResponseBuilder) addDescendants(ctx context.Context, parent *merkle.Node, parentDepth int, pathSeen, skip map[string]struct{}) error {
	children, err := b.childrenOf(ctx, parent.Hash)
	if err != nil {
		return err
	}

	for _, child := range children {
		if _, ok := skip[child.Hash]; ok {
			b.addLink(stringPtr(parent.Hash), child.Hash)
			continue
		}
		if _, ok := pathSeen[child.Hash]; ok {
			b.resp.Truncated = true
			b.resp.CycleDetected = true
			continue
		}

		ok, err := b.addNode(ctx, child, stringPtr(parent.Hash), parentDepth+1)
		if err != nil || !ok {
			return err
		}

		pathSeen[child.Hash] = struct{}{}
		if err := b.addDescendants(ctx, child, parentDepth+1, pathSeen, nil); err != nil {
			return err
		}
		delete(pathSeen, child.Hash)
	}

	return nil
}

func (b *graphResponseBuilder) addNode(ctx context.Context, node *merkle.Node, parentID *string, depth int) (bool, error) {
	if _, ok := b.seenNode[node.Hash]; ok {
		b.addLink(parentID, node.Hash)
		return true, nil
	}
	if len(b.resp.Nodes) >= b.maxNodes {
		b.resp.Truncated = true
		return false, nil
	}

	children, err := b.childrenOf(ctx, node.Hash)
	if err != nil {
		return false, err
	}
	childrenCount := len(children)

	b.seenNode[node.Hash] = struct{}{}
	b.resp.Nodes = append(b.resp.Nodes, GraphNode{
		ID:            node.Hash,
		ParentID:      copyStringPtr(parentID),
		ParentHash:    copyStringPtr(node.ParentHash),
		Type:          node.Bucket.Type,
		Role:          node.Bucket.Role,
		Preview:       makePreview(node),
		Model:         node.Bucket.Model,
		Provider:      node.Bucket.Provider,
		AgentName:     node.Bucket.AgentName,
		Project:       node.Project,
		StopReason:    node.StopReason,
		Usage:         node.Usage,
		CreatedAt:     node.CreatedAt,
		Depth:         depth,
		ChildrenCount: childrenCount,
		IsRoot:        parentID == nil,
		IsLeaf:        childrenCount == 0,
		IsBranchPoint: childrenCount > 1,
		Selected:      node.Hash == b.requestedHash,

		NodeKind:        node.Kind,
		ParentToolUseID: node.ParentToolUseID,
		ThreadID:        node.ThreadID,
	})

	b.addLink(parentID, node.Hash)
	if childrenCount == 0 {
		b.resp.Leaves = append(b.resp.Leaves, node.Hash)
	}
	if childrenCount > 1 {
		b.resp.BranchPoints = append(b.resp.BranchPoints, node.Hash)
	}
	return true, nil
}

func (b *graphResponseBuilder) childrenOf(ctx context.Context, hash string) ([]*merkle.Node, error) {
	if children, ok := b.children[hash]; ok {
		return children, nil
	}

	children, err := b.driver.GetByParent(ctx, &hash)
	if err != nil {
		return nil, fmt.Errorf("getting children of %s: %w", hash, err)
	}
	sort.Slice(children, func(i, j int) bool {
		return merkleNodeLess(children[i], children[j])
	})
	b.children[hash] = children
	return children, nil
}

func (b *graphResponseBuilder) addLink(parentID *string, childID string) {
	if parentID == nil {
		return
	}
	key := *parentID + "\x00" + childID
	if _, ok := b.seenLink[key]; ok {
		return
	}
	b.seenLink[key] = struct{}{}
	b.resp.Links = append(b.resp.Links, GraphLink{Source: *parentID, Target: childID})
}

func merkleNodeLess(a, b *merkle.Node) bool {
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return a.Hash < b.Hash
}

func hashesFromNodes(nodes []*merkle.Node) map[string]struct{} {
	out := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		out[n.Hash] = struct{}{}
	}
	return out
}

func stringPtr(s string) *string {
	v := s
	return &v
}

func copyStringPtr(in *string) *string {
	if in == nil {
		return nil
	}
	v := *in
	return &v
}
