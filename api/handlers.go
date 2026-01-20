package api

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// HistoryResponse contains the conversation history for a given node.
type HistoryResponse struct {
	// Messages in chronological order (oldest first, up to and including the requested node)
	Messages []HistoryMessage `json:"messages"`
	// HeadHash is the hash of the node that was requested
	HeadHash string `json:"head_hash"`
	// Depth is the number of messages in the history
	Depth int `json:"depth"`
}

// HistoryMessage represents a message in the conversation history.
type HistoryMessage struct {
	Hash       string         `json:"hash"`
	ParentHash *string        `json:"parent_hash,omitempty"`
	Role       string         `json:"role"`
	Content    any            `json:"content"` // Can be string or []ContentBlock
	Model      string         `json:"model,omitempty"`
	Provider   string         `json:"provider,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// handlePing returns a simple health check response.
func (s *Server) handlePing(c *fiber.Ctx) error {
	return c.JSON("pong")
}

// handleDAGStats returns statistics about the DAG.
func (s *Server) handleDAGStats(c *fiber.Ctx) error {
	ctx := c.Context()

	nodes, err := s.storer.List(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list nodes"})
	}

	roots, err := s.storer.Roots(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get roots"})
	}

	leaves, err := s.storer.Leaves(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get leaves"})
	}

	stats := map[string]any{
		"total_nodes": len(nodes),
		"root_count":  len(roots),
		"leaf_count":  len(leaves),
	}

	return c.JSON(stats)
}

// handleGetNode returns a single node by its hash.
func (s *Server) handleGetNode(c *fiber.Ctx) error {
	hash := c.Params("hash")
	if hash == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "hash parameter required"})
	}

	node, err := s.storer.Get(c.Context(), hash)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "node not found"})
	}

	return c.JSON(node)
}

// handleListHistories returns all conversation histories (one per leaf node).
func (s *Server) handleListHistories(c *fiber.Ctx) error {
	ctx := c.Context()

	leaves, err := s.storer.Leaves(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to get leaves"})
	}

	histories := make([]HistoryResponse, 0, len(leaves))
	for _, leaf := range leaves {
		history, err := s.buildHistory(ctx, leaf.Hash)
		if err != nil {
			s.logger.Warn("failed to build history for leaf",
				zap.String("hash", leaf.Hash),
				zap.Error(err),
			)
			continue
		}
		histories = append(histories, *history)
	}

	return c.JSON(map[string]any{
		"count":     len(histories),
		"histories": histories,
	})
}

// handleGetHistory returns the full conversation history leading up to a given node.
func (s *Server) handleGetHistory(c *fiber.Ctx) error {
	hash := c.Params("hash")
	if hash == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "hash parameter required"})
	}

	history, err := s.buildHistory(c.Context(), hash)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "node not found"})
	}

	return c.JSON(history)
}

// buildHistory constructs a HistoryResponse for the given node hash.
func (s *Server) buildHistory(ctx context.Context, hash string) (*HistoryResponse, error) {
	ancestry, err := s.storer.Ancestry(ctx, hash)
	if err != nil {
		return nil, err
	}

	messages := make([]HistoryMessage, len(ancestry))
	for i, node := range ancestry {
		idx := len(ancestry) - 1 - i

		msg := HistoryMessage{
			Hash:       node.Hash,
			ParentHash: node.ParentHash,
		}

		if content, ok := node.Content.(map[string]any); ok {
			if role, ok := content["role"].(string); ok {
				msg.Role = role
			}
			// Content can now be []ContentBlock or string
			msg.Content = content["content"]
			if model, ok := content["model"].(string); ok {
				msg.Model = model
			}
			if provider, ok := content["provider"].(string); ok {
				msg.Provider = provider
			}
			// Copy additional metadata
			metadata := make(map[string]any)
			for k, v := range content {
				if k != "role" && k != "content" && k != "model" && k != "type" && k != "provider" {
					metadata[k] = v
				}
			}
			if len(metadata) > 0 {
				msg.Metadata = metadata
			}
		}

		messages[idx] = msg
	}

	return &HistoryResponse{
		Messages: messages,
		HeadHash: hash,
		Depth:    len(messages),
	}, nil
}
