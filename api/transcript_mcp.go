package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// transcriptToolArgs is the core MCP `transcript` tool's input: the same
// parameters GET /v1/sessions/{id}/transcript takes.
type transcriptToolArgs struct {
	SessionID string `json:"session_id" jsonschema:"Session id (UUID)"`
	Role      string `json:"role,omitempty" jsonschema:"user, assistant, or all (default all); user keeps only rows with user text"`
	Q         string `json:"q,omitempty" jsonschema:"Keep only rows whose text contains this substring (case-insensitive)"`
	Subagents bool   `json:"subagents,omitempty" jsonschema:"Include subagent threads (default false: the root thread only)"`
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum rows in the page (default 200, max 1000)"`
	Cursor    string `json:"cursor,omitempty" jsonschema:"next_cursor from a previous call for the same session"`
}

// transcriptToolTimeout bounds one `transcript` tool call. The MCP route
// is exempt from the read deadline so cassette tool calls can run under
// their own 30-second limit; this core tool takes the same limit for
// itself rather than running unbounded.
const transcriptToolTimeout = 30 * time.Second

// registerTranscriptTool adds the core `transcript` read tool. It runs
// the same builder as the HTTP route, so the two return the same page for
// the same parameters.
func (s *Server) registerTranscriptTool(server *mcpsdk.Server) {
	readOnly := true
	mcpsdk.AddTool(server, &mcpsdk.Tool{
		Name:  "transcript",
		Title: "Session transcript",
		Description: "Read one captured session as a conversation: the user's messages and the " +
			"assistant's replies in order, with tool calls and tool results attached, harness " +
			"side-calls and injected context left out. Filter with role (user|assistant|all) and " +
			"q (case-insensitive substring); include subagent threads with subagents=true. Pages " +
			"follow next_cursor. Same result as GET /v1/sessions/{id}/transcript.",
		Annotations: &mcpsdk.ToolAnnotations{ReadOnlyHint: readOnly, IdempotentHint: true},
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, args transcriptToolArgs) (*mcpsdk.CallToolResult, any, error) {
		// The tool reads full span payloads, so it takes a payload slot like
		// the HTTP route does; a shed call is a tool error the client can
		// retry, not a queued read.
		if s.guards != nil {
			held, ok := s.guards.acquirePayload()
			if !ok {
				return nil, nil, fmt.Errorf("transcript: 503: %s; retry shortly", errTooManyPayloadReads)
			}
			defer held.giveBack()
		}
		ctx, cancel := context.WithTimeout(ctx, transcriptToolTimeout)
		defer cancel()
		resp, err := s.buildTranscript(ctx, transcriptQuery(args))
		if err != nil {
			if te, ok := errors.AsType[*transcriptError](err); ok {
				return nil, nil, fmt.Errorf("transcript: %d: %s", te.status, te.msg)
			}
			return nil, nil, fmt.Errorf("transcript: %w", err)
		}
		return nil, resp, nil
	})
}
