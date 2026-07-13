package api

//	@title			Tapes API
//	@description	HTTP API for inspecting, querying, and searching stored Tapes sessions.
//	@description
//	@description	The REST surface exposes health checks, session listing and retrieval, derived session summaries, aggregate stats, semantic search, and a streamable MCP endpoint.
//	@BasePath		/
//	@schemes		http https

// Re-exported Go types / structs for swagger to generate docs on.

var (
	_ = swaggerMCPRequest{}
	_ = swaggerMCPResponse{}
	_ = swaggerMCPError{}
	_ = swaggerMCPPostDoc
	_ = swaggerMCPGetDoc
	_ = swaggerMCPDeleteDoc
)

type swaggerMCPRequest struct {
	JSONRPC string         `json:"jsonrpc" example:"2.0"`
	ID      string         `json:"id,omitempty" example:"1"`
	Method  string         `json:"method" example:"tools/call"`
	Params  map[string]any `json:"params,omitempty"`
}

type swaggerMCPResponse struct {
	JSONRPC string           `json:"jsonrpc" example:"2.0"`
	ID      string           `json:"id,omitempty" example:"1"`
	Result  map[string]any   `json:"result,omitempty"`
	Error   *swaggerMCPError `json:"error,omitempty"`
}

type swaggerMCPError struct {
	Code    int    `json:"code" example:"-32600"`
	Message string `json:"message" example:"invalid request"`
}

// swaggerMCPPostDoc documents the streamable MCP POST transport.
//
//	@Summary		Invoke the streamable MCP endpoint
//	@ID			invokeMcp
//	@Description	Sends a JSON-RPC 2.0 request to the stateless Model Context Protocol endpoint mounted at /v1/mcp.
//	@Description
//	@Description	Typical calls include initialize, tools/list, and tools/call. When search is configured, the server exposes a search tool over this transport.
//	@Tags			mcp
//	@Accept			json
//	@Produce		json
//	@Param			request	body		swaggerMCPRequest	true	"JSON-RPC 2.0 request"
//	@Success		200		{object}	swaggerMCPResponse
//	@Failure		400		{object}	swaggerMCPResponse	"Invalid JSON-RPC request"
//	@Failure		500		{object}	swaggerMCPResponse	"Server-side MCP error"
//	@Router			/v1/mcp [post]
func swaggerMCPPostDoc() {}

// swaggerMCPGetDoc documents the streamable MCP GET transport.
//
//	@Summary		Open an MCP event stream
//	@ID			openMcpStream
//	@Description	Opens the streamable MCP endpoint for server-sent events. Stateless clients can use this to receive streamed MCP messages.
//	@Tags			mcp
//	@Produce		text/event-stream
//	@Success		200	{string}	string	"Server-sent event stream"
//	@Router			/v1/mcp [get]
func swaggerMCPGetDoc() {}

// swaggerMCPDeleteDoc documents the streamable MCP DELETE transport.
//
//	@Summary		Close an MCP session
//	@ID			closeMcpSession
//	@Description	Requests termination of a streamable MCP session when a client is using session-oriented transport semantics.
//	@Tags			mcp
//	@Produce		json
//	@Success		200	{object}	swaggerMCPResponse
//	@Failure		400	{object}	swaggerMCPResponse
//	@Router			/v1/mcp [delete]
func swaggerMCPDeleteDoc() {}
