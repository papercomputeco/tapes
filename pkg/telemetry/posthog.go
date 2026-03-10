package telemetry

import (
	"fmt"
	"log/slog"

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
	logger     *slog.Logger
}

// NewClient creates a new telemetry Client that sends events to PostHog.
// The distinctID should be the persistent UUID from the telemetry Manager.
// Returns nil (not an error) when PostHogAPIKey is empty so callers can
// treat a nil *Client as "telemetry disabled" without extra checks.
func NewClient(distinctID string, l *slog.Logger) (*Client, error) {
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
		logger:     l,
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
func (c *Client) capture(event string, props posthog.Properties) error {
	if c == nil {
		return nil
	}

	p := CommonProperties().
		Set("version", utils.Version).
		Set("$lib", "tapes-cli").
		Merge(props)

	return c.ph.Enqueue(posthog.Capture{
		DistinctId: c.distinctID,
		Event:      event,
		Properties: p,
	})
}

// CaptureInstall records a first-run install event.
func (c *Client) CaptureInstall() {
	if err := c.capture(EventInstall, nil); err != nil {
		c.logger.Debug("could not capture install telemetry", "error", err)
	}
}

// CaptureCommandRun records a CLI command execution.
func (c *Client) CaptureCommandRun(command string) {
	if err := c.capture(EventCommandRun, posthog.NewProperties().Set("command", command)); err != nil {
		c.logger.Debug("could not capture event run telemetry", "error", err)
	}
}

// CaptureInit records a tapes init event.
func (c *Client) CaptureInit(preset string) {
	if err := c.capture(EventInit, posthog.NewProperties().Set("preset", preset)); err != nil {
		c.logger.Debug("could not capture init telemetry", "error", err)
	}
}

// CaptureSessionCreated records a new recording session.
func (c *Client) CaptureSessionCreated(provider string) {
	if err := c.capture(EventSessionCreated, posthog.NewProperties().Set("provider", provider)); err != nil {
		c.logger.Debug("could not capture session creation telemetry", "error", err)
	}
}

// CaptureSearch records a search operation.
func (c *Client) CaptureSearch(resultCount int) {
	if err := c.capture(EventSearch, posthog.NewProperties().Set("result_count", resultCount)); err != nil {
		c.logger.Debug("could not capture search telemetry", "error", err)
	}
}

// CaptureServerStarted records a server startup event.
func (c *Client) CaptureServerStarted(mode string) {
	if err := c.capture(EventServerStarted, posthog.NewProperties().Set("mode", mode)); err != nil {
		c.logger.Debug("could not capture serve telemetry", "error", err)
	}
}

// CaptureMCPTool records an MCP tool invocation.
func (c *Client) CaptureMCPTool(tool string) {
	if err := c.capture(EventMCPTool, posthog.NewProperties().Set("tool", tool)); err != nil {
		c.logger.Debug("could not capture mcp telemetry", "error", err)
	}
}

// CaptureSyncPush records a sync push event.
func (c *Client) CaptureSyncPush() {
	if err := c.capture(EventSyncPush, nil); err != nil {
		c.logger.Debug("could not capture sync push telemetry", "error", err)
	}
}

// CaptureSyncPull records a sync pull event.
func (c *Client) CaptureSyncPull() {
	if err := c.capture(EventSyncPull, nil); err != nil {
		c.logger.Debug("could not capture sync pull telemetry", "error", err)
	}
}

// CaptureError records an error event.
func (c *Client) CaptureError(command, errType string) {
	if err := c.capture(EventError, posthog.NewProperties().Set("command", command).Set("error_type", errType)); err != nil {
		c.logger.Debug("could not capture error telemetry", "error", err)
	}
}
