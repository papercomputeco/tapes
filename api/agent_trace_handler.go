package api

import (
	"strconv"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/agenttrace"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// handleCreateAgentTrace handles POST /v1/agent-traces.
func (s *Server) handleCreateAgentTrace(c *fiber.Ctx) error {
	var trace agenttrace.AgentTrace
	if err := c.BodyParser(&trace); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "invalid request body",
		})
	}

	// Validate required fields
	if trace.Version == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "version is required",
		})
	}
	if trace.ID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "id is required",
		})
	}
	if trace.Timestamp == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "timestamp is required",
		})
	}
	if len(trace.Files) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "at least one file is required",
		})
	}

	for _, f := range trace.Files {
		if f.Path == "" {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
				Error: "file path is required for all files",
			})
		}
		for _, conv := range f.Conversations {
			for _, r := range conv.Ranges {
				if r.StartLine < 0 || r.EndLine < 0 {
					return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
						Error: "start_line and end_line must be non-negative",
					})
				}
			}
		}
	}

	created, err := s.agentTraceStore.CreateAgentTrace(c.Context(), &trace)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{
			Error: "failed to create agent trace",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(created)
}

// handleGetAgentTrace handles GET /v1/agent-traces/:id.
func (s *Server) handleGetAgentTrace(c *fiber.Ctx) error {
	id := c.Params("id")
	if id == "" {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: "id parameter is required",
		})
	}

	trace, err := s.agentTraceStore.GetAgentTrace(c.Context(), id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{
			Error: "agent trace not found",
		})
	}

	return c.JSON(trace)
}

// handleQueryAgentTraces handles GET /v1/agent-traces.
func (s *Server) handleQueryAgentTraces(c *fiber.Ctx) error {
	query := storage.AgentTraceQuery{
		FilePath: c.Query("file_path"),
		Revision: c.Query("revision"),
		ToolName: c.Query("tool_name"),
	}

	if limitStr := c.Query("limit"); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit < 0 {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
				Error: "limit must be a non-negative integer",
			})
		}
		query.Limit = limit
	}

	if offsetStr := c.Query("offset"); offsetStr != "" {
		offset, err := strconv.Atoi(offsetStr)
		if err != nil || offset < 0 {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
				Error: "offset must be a non-negative integer",
			})
		}
		query.Offset = offset
	}

	traces, err := s.agentTraceStore.QueryAgentTraces(c.Context(), query)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{
			Error: "failed to query agent traces",
		})
	}

	return c.JSON(traces)
}
