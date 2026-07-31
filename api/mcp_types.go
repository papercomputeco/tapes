package api

// The MCP transport is implemented by modelcontextprotocol/go-sdk behind a
// net/http handler, so its request and response shapes never appear as Go types
// on this server. These declarations exist to give the published contract
// something to describe.
//
// They are JSON-RPC 2.0, deliberately loose: `params` and `result` are
// method-dependent, and pinning them here would describe one version of the MCP
// method set as though it were the protocol.

// MCPRequest is a JSON-RPC 2.0 request to the streamable MCP endpoint.
type MCPRequest struct {
	// JSONRPC is the protocol version, always "2.0".
	JSONRPC string `json:"jsonrpc" oas:"example=2.0"`

	// ID correlates a response with this request. Absent on notifications.
	ID string `json:"id,omitempty" oas:"example=1"`

	// Method is the MCP method being invoked, such as tools/call.
	Method string `json:"method" oas:"example=tools/call"`

	// Params are the method's arguments.
	Params map[string]any `json:"params,omitempty"`
}

// MCPResponse is a JSON-RPC 2.0 response from the streamable MCP endpoint.
//
// Exactly one of Result and Error is set, which is the JSON-RPC contract rather
// than anything this server adds.
type MCPResponse struct {
	// JSONRPC is the protocol version, always "2.0".
	JSONRPC string `json:"jsonrpc" oas:"example=2.0"`

	// ID is the id of the request this answers.
	ID string `json:"id,omitempty" oas:"example=1"`

	// Result is the method's return value on success.
	Result map[string]any `json:"result,omitempty"`

	// Error describes the failure when the call did not succeed.
	Error *MCPError `json:"error,omitempty"`
}

// MCPError is a JSON-RPC 2.0 error object.
type MCPError struct {
	// Code is the JSON-RPC error code.
	Code int `json:"code" oas:"example=-32600"`

	// Message is a short description of the failure.
	Message string `json:"message" oas:"example=invalid request"`
}
