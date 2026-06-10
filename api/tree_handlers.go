package api

import (
	"regexp"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// SessionTreeResponse is the reconciled conversation tree for one
// session: every captured node typed by kind, spine links from the
// chain hashes, fork/rejoin edges recovered from the transcript
// reconciliation, and offshoot attachments (permission verdicts,
// web summaries) pointing at the tool_use they relate to.
type SessionTreeResponse struct {
	SessionID        string `json:"session_id"`
	HarnessSessionID string `json:"harness_session_id,omitempty"`

	Nodes []TreeNode `json:"nodes"`
	Links []GraphLink `json:"links"`

	// Forks are subagent threads: a chain whose root carries the Task
	// tool_use that spawned it. SourceID is the node holding the
	// tool_use block; RejoinID the node holding its tool_result.
	Forks []TreeFork `json:"forks"`

	// Attachments link offshoot calls to the tool_use they relate to:
	// permission checks to the judged action, web summaries to their
	// fetch/search.
	Attachments []TreeAttachment `json:"attachments"`

	// KindCounts summarizes the session's call mix.
	KindCounts map[string]int `json:"kind_counts"`

	// Roots are the parentless chain roots, main thread first.
	Roots []string `json:"roots"`
}

// TreeNode is one captured node plus its semantic typing.
type TreeNode struct {
	ID              string     `json:"id"`
	ParentID        *string    `json:"parent_id,omitempty"`
	Role            string     `json:"role,omitempty"`
	NodeKind        string     `json:"node_kind,omitempty"`
	ParentToolUseID string     `json:"parent_tool_use_id,omitempty"`
	Preview         string     `json:"preview,omitempty"`
	Model           string     `json:"model,omitempty"`
	StopReason      string     `json:"stop_reason,omitempty"`
	Usage           *llm.Usage `json:"usage,omitempty"`
	CreatedAt       time.Time  `json:"created_at,omitzero"`
	IsRoot          bool       `json:"is_root"`
	IsLeaf          bool       `json:"is_leaf"`

	// ToolUses lists the tool_use blocks this node carries (assistant
	// turns), so clients can anchor forks/attachments without parsing
	// content.
	ToolUses []TreeToolUse `json:"tool_uses,omitempty"`

	// Verdict is set on permission-check response nodes: the monitor's
	// disposition for the judged action.
	Verdict *TreeVerdict `json:"verdict,omitempty"`
}

// TreeToolUse is a tool invocation surfaced from a node's content.
type TreeToolUse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// TreeVerdict is a security-monitor disposition.
type TreeVerdict struct {
	Disposition string `json:"disposition"` // ALLOW | BLOCK
	Stage       int    `json:"stage"`
	Reasoned    bool   `json:"reasoned"`
}

// TreeFork is a subagent thread's fork/rejoin edge pair.
type TreeFork struct {
	ToolUseID string `json:"tool_use_id"`
	RootID    string `json:"root_id"`
	SourceID  string `json:"source_id,omitempty"`
	RejoinID  string `json:"rejoin_id,omitempty"`
}

// TreeAttachment links an offshoot call to the tool_use it annotates.
type TreeAttachment struct {
	NodeID    string `json:"node_id"`
	NodeKind  string `json:"node_kind"`
	ToolUseID string `json:"tool_use_id"`
	TargetID  string `json:"target_id,omitempty"`
}

var blockVerdictPattern = regexp.MustCompile(`(?i)<block>\s*(yes|no)`)

// handleGetSessionTree handles GET /v1/sessions/:id/tree.
//
//	@Summary		Get a session's reconciled conversation tree
//	@Description	Returns every captured node for the session typed by node_kind, with spine links, subagent fork/rejoin edges, and offshoot attachments (permission verdicts, web summaries) anchored to the tool_use they relate to.
//	@Tags			sessions
//	@Produce		json
//	@Param			id	path		string	true	"Session id (UUID)"
//	@Success		200	{object}	SessionTreeResponse
//	@Failure		400	{object}	llm.ErrorResponse	"Missing or malformed id"
//	@Failure		404	{object}	llm.ErrorResponse	"Session not found"
//	@Failure		500	{object}	llm.ErrorResponse	"Failed to load session"
//	@Failure		501	{object}	llm.ErrorResponse	"Sessions not supported by this backend"
//	@Router			/v1/sessions/{id}/tree [get]
func (s *Server) handleGetSessionTree(c *fiber.Ctx) error {
	reader, ok := s.driver.(sessionsReader)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "sessions not supported by this backend"})
	}

	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id parameter required"})
	}
	if _, err := uuid.Parse(id); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "id must be a valid UUID"})
	}

	orgID := orgIDFromCtx(c)
	sess, err := reader.GetSessionRecord(c.Context(), orgID, id)
	if err != nil {
		s.logger.Error("get session for tree", "id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session"})
	}
	if sess == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "session not found"})
	}

	nodes, err := reader.ListNodesBySession(c.Context(), id)
	if err != nil {
		s.logger.Error("list nodes for tree", "session_id", id, "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to load session nodes"})
	}

	resp := buildSessionTree(id, sess.HarnessSessionID, nodes)
	return c.JSON(resp)
}

// buildSessionTree assembles the reconciled tree from a session's
// derived nodes. Pure projection — every edge here was computed by the
// deriver; this only renders it.
func buildSessionTree(sessionID, harnessSessionID string, nodes []*merkle.Node) *SessionTreeResponse {
	resp := &SessionTreeResponse{
		SessionID:        sessionID,
		HarnessSessionID: harnessSessionID,
		Nodes:            make([]TreeNode, 0, len(nodes)),
		Links:            []GraphLink{},
		Forks:            []TreeFork{},
		Attachments:      []TreeAttachment{},
		KindCounts:       map[string]int{},
		Roots:            []string{},
	}

	inSession := make(map[string]*merkle.Node, len(nodes))
	hasChild := map[string]bool{}
	for _, n := range nodes {
		inSession[n.Hash] = n
	}
	for _, n := range nodes {
		if n.ParentHash != nil && inSession[*n.ParentHash] != nil {
			hasChild[*n.ParentHash] = true
		}
	}

	// Index tool_use → carrying node and tool_result → consuming node,
	// for fork/attachment anchoring.
	toolUseNode := map[string]string{}
	toolResultNode := map[string]string{}
	for _, n := range nodes {
		for _, b := range n.Bucket.Content {
			switch b.Type {
			case "tool_use", "server_tool_use":
				if b.ToolUseID != "" {
					if _, ok := toolUseNode[b.ToolUseID]; !ok {
						toolUseNode[b.ToolUseID] = n.Hash
					}
				}
			case "tool_result", "web_search_tool_result":
				if b.ToolResultID != "" {
					if _, ok := toolResultNode[b.ToolResultID]; !ok {
						toolResultNode[b.ToolResultID] = n.Hash
					}
				}
			}
		}
	}

	for _, n := range nodes {
		kind := n.Kind
		resp.KindCounts[kind]++

		var parentID *string
		if n.ParentHash != nil && inSession[*n.ParentHash] != nil {
			parentID = n.ParentHash
		}
		isRoot := parentID == nil

		tn := TreeNode{
			ID:              n.Hash,
			ParentID:        copyStringPtr(parentID),
			Role:            n.Bucket.Role,
			NodeKind:        kind,
			ParentToolUseID: n.ParentToolUseID,
			Preview:         makePreview(n),
			Model:           n.Bucket.Model,
			StopReason:      n.StopReason,
			Usage:           n.Usage,
			CreatedAt:       n.CreatedAt,
			IsRoot:          isRoot,
			IsLeaf:          !hasChild[n.Hash],
		}
		for _, b := range n.Bucket.Content {
			if (b.Type == "tool_use" || b.Type == "server_tool_use") && b.ToolUseID != "" {
				tn.ToolUses = append(tn.ToolUses, TreeToolUse{ID: b.ToolUseID, Name: b.ToolName})
			}
		}
		if v := verdictFromNode(n); v != nil {
			tn.Verdict = v
		}
		resp.Nodes = append(resp.Nodes, tn)

		if parentID != nil {
			resp.Links = append(resp.Links, GraphLink{Source: *parentID, Target: n.Hash})
		}
		if isRoot {
			resp.Roots = append(resp.Roots, n.Hash)
		}

		// Fork edges: a conversation chain root pointing at the Task
		// tool_use that spawned it.
		if isRoot && kind == merkleKindMain && n.ParentToolUseID != "" {
			resp.Forks = append(resp.Forks, TreeFork{
				ToolUseID: n.ParentToolUseID,
				RootID:    n.Hash,
				SourceID:  toolUseNode[n.ParentToolUseID],
				RejoinID:  toolResultNode[n.ParentToolUseID],
			})
			continue
		}

		// Offshoot attachments: stamp once per offshoot call, on its
		// response node (role=assistant keeps it one per call).
		if strings.HasPrefix(kind, "offshoot:") && n.ParentToolUseID != "" && n.Bucket.Role == "assistant" {
			resp.Attachments = append(resp.Attachments, TreeAttachment{
				NodeID:    n.Hash,
				NodeKind:  kind,
				ToolUseID: n.ParentToolUseID,
				TargetID:  toolUseNode[n.ParentToolUseID],
			})
		}
	}

	return resp
}

const merkleKindMain = "main"

// verdictFromNode extracts the security monitor's disposition from a
// permission-check response node.
func verdictFromNode(n *merkle.Node) *TreeVerdict {
	if n.Bucket.Role != "assistant" || !strings.HasPrefix(n.Kind, "offshoot:permission-check") {
		return nil
	}
	var text strings.Builder
	for _, b := range n.Bucket.Content {
		if b.Text != "" {
			text.WriteString(b.Text)
		}
	}
	m := blockVerdictPattern.FindStringSubmatch(text.String())
	if m == nil {
		return nil
	}
	v := &TreeVerdict{Disposition: "ALLOW", Stage: 1}
	if strings.EqualFold(m[1], "yes") {
		v.Disposition = "BLOCK"
	}
	if strings.HasSuffix(n.Kind, "stage2") {
		v.Stage = 2
	}
	if strings.Contains(text.String(), "<thinking>") {
		v.Reasoned = true
	}
	return v
}
