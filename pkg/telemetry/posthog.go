package telemetry

import (
	"fmt"

	"github.com/posthog/posthog-go"

	"github.com/papercomputeco/tapes/pkg/utils"
)

const (
	// phProjectAPIKey is the PostHog write-only project API key.
	phProjectAPIKey = "phc_xCBFT1jetPLJIRGTqJ9Q0YuG5I1jhXtUkxYkNBEAXRY"

	// phEndpoint is the PostHog ingestion endpoint.
	phEndpoint = "https://us.i.posthog.com"
)

// Event name constants for all tracked telemetry events.
const (
	EventInstall        = "tapes_cli_installed"
	EventCommandRun     = "tapes_cli_command_run"
	EventInit           = "tapes_cli_init"
	EventSessionCreated = "tapes_cli_session_created"
	EventSearch         = "tapes_cli_search"
	EventServerStarted  = "tapes_cli_server_started"
	EventMCPTool        = "tapes_cli_mcp_tool"
	EventSyncPush       = "tapes_cli_sync_push"
	EventSyncPull       = "tapes_cli_sync_pull"
	EventError          = "tapes_cli_error"
)

// Client wraps the PostHog SDK client for capturing telemetry events.
// All capture methods are nil-safe: calling them on a nil *Client is a no-op.
type Client struct {
	ph         posthog.Client
	distinctID string
}

// NewClient creates a new telemetry Client that sends events to PostHog.
// The distinctID should be the persistent UUID from the telemetry Manager.
func NewClient(distinctID string) (*Client, error) {
	ph, err := posthog.NewWithConfig(
		phProjectAPIKey,
		posthog.Config{
			Endpoint: phEndpoint,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating posthog client: %w", err)
	}

	return &Client{
		ph:         ph,
		distinctID: distinctID,
	}, nil
}

// Close flushes any pending events and shuts down the PostHog client.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	return c.ph.Close()
}

// capture sends an event with the given name and properties merged with common properties.
func (c *Client) capture(event string, props map[string]interface{}) {
	if c == nil {
		return
	}

	merged := CommonProperties()
	merged["version"] = utils.Version
	merged["$lib"] = "tapes-cli"
	for k, v := range props {
		merged[k] = v
	}

	_ = c.ph.Enqueue(posthog.Capture{
		DistinctId: c.distinctID,
		Event:      event,
		Properties: merged,
	})
}

// CaptureInstall records a first-run install event.
func (c *Client) CaptureInstall() {
	c.capture(EventInstall, nil)
}

// CaptureCommandRun records a CLI command execution.
func (c *Client) CaptureCommandRun(command string) {
	c.capture(EventCommandRun, map[string]interface{}{
		"command": command,
	})
}

// CaptureInit records a tapes init event.
func (c *Client) CaptureInit(preset string) {
	c.capture(EventInit, map[string]interface{}{
		"preset": preset,
	})
}

// CaptureSessionCreated records a new recording session.
func (c *Client) CaptureSessionCreated(provider string) {
	c.capture(EventSessionCreated, map[string]interface{}{
		"provider": provider,
	})
}

// CaptureSearch records a search operation.
func (c *Client) CaptureSearch(resultCount int) {
	c.capture(EventSearch, map[string]interface{}{
		"result_count": resultCount,
	})
}

// CaptureServerStarted records a server startup event.
func (c *Client) CaptureServerStarted(mode string) {
	c.capture(EventServerStarted, map[string]interface{}{
		"mode": mode,
	})
}

// CaptureMCPTool records an MCP tool invocation.
func (c *Client) CaptureMCPTool(tool string) {
	c.capture(EventMCPTool, map[string]interface{}{
		"tool": tool,
	})
}

// CaptureSyncPush records a sync push event.
func (c *Client) CaptureSyncPush() {
	c.capture(EventSyncPush, nil)
}

// CaptureSyncPull records a sync pull event.
func (c *Client) CaptureSyncPull() {
	c.capture(EventSyncPull, nil)
}

// CaptureError records an error event.
func (c *Client) CaptureError(command, errType string) {
	c.capture(EventError, map[string]interface{}{
		"command":    command,
		"error_type": errType,
	})
}
