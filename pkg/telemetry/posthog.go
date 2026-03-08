package telemetry

import (
	"fmt"

	"github.com/posthog/posthog-go"

	"github.com/papercomputeco/tapes/pkg/utils"
)

var (
	// PostHogAPIKey is the PostHog write-only project API key.
	// Injected at build time via ldflags; defaults to empty (telemetry disabled).
	PostHogAPIKey = ""

	// PostHogEndpoint is the PostHog ingestion endpoint.
	// Injected at build time via ldflags; defaults to the US region.
	PostHogEndpoint = "https://us.i.posthog.com"
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
// Returns nil (not an error) when PostHogAPIKey is empty so callers can
// treat a nil *Client as "telemetry disabled" without extra checks.
func NewClient(distinctID string) (*Client, error) {
	if PostHogAPIKey == "" {
		return nil, nil
	}

	ph, err := posthog.NewWithConfig(
		PostHogAPIKey,
		posthog.Config{
			Endpoint: PostHogEndpoint,
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
func (c *Client) capture(event string, props posthog.Properties) {
	if c == nil {
		return
	}

	p := CommonProperties().
		Set("version", utils.Version).
		Set("$lib", "tapes-cli").
		Merge(props)

	_ = c.ph.Enqueue(posthog.Capture{
		DistinctId: c.distinctID,
		Event:      event,
		Properties: p,
	})
}

// CaptureInstall records a first-run install event.
func (c *Client) CaptureInstall() {
	c.capture(EventInstall, nil)
}

// CaptureCommandRun records a CLI command execution.
func (c *Client) CaptureCommandRun(command string) {
	c.capture(EventCommandRun, posthog.NewProperties().Set("command", command))
}

// CaptureInit records a tapes init event.
func (c *Client) CaptureInit(preset string) {
	c.capture(EventInit, posthog.NewProperties().Set("preset", preset))
}

// CaptureSessionCreated records a new recording session.
func (c *Client) CaptureSessionCreated(provider string) {
	c.capture(EventSessionCreated, posthog.NewProperties().Set("provider", provider))
}

// CaptureSearch records a search operation.
func (c *Client) CaptureSearch(resultCount int) {
	c.capture(EventSearch, posthog.NewProperties().Set("result_count", resultCount))
}

// CaptureServerStarted records a server startup event.
func (c *Client) CaptureServerStarted(mode string) {
	c.capture(EventServerStarted, posthog.NewProperties().Set("mode", mode))
}

// CaptureMCPTool records an MCP tool invocation.
func (c *Client) CaptureMCPTool(tool string) {
	c.capture(EventMCPTool, posthog.NewProperties().Set("tool", tool))
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
	c.capture(EventError, posthog.NewProperties().
		Set("command", command).
		Set("error_type", errType))
}
