// Package checkoutcmder provides the checkout subcommand for checking out
// a point in the conversation DAG.
package checkoutcmder

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/pkg/cliui"
	"github.com/papercomputeco/tapes/pkg/config"
	"github.com/papercomputeco/tapes/pkg/dotdir"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/utils"
)

type checkoutCommander struct {
	flags config.FlagSet

	hash      string
	apiTarget string
	debug     bool

	logger *slog.Logger
}

// sessionResponse mirrors the API's SessionResponse type for JSON deserialization.
type sessionResponse struct {
	Hash  string `json:"hash"`
	Depth int    `json:"depth"`
	Turns []turn `json:"turns"`
}

// turn mirrors the API's Turn type.
type turn struct {
	Hash       string             `json:"hash"`
	ParentHash *string            `json:"parent_hash,omitempty"`
	Role       string             `json:"role"`
	Content    []llm.ContentBlock `json:"content"`
	Model      string             `json:"model,omitempty"`
	Provider   string             `json:"provider,omitempty"`
	AgentName  string             `json:"agent_name,omitempty"`
	StopReason string             `json:"stop_reason,omitempty"`
	Usage      *llm.Usage         `json:"usage,omitempty"`
}

var checkoutFlags = config.FlagSet{
	config.FlagAPITarget: {Name: "api-target", Shorthand: "a", ViperKey: "client.api_target", Description: "Tapes API server URL"},
}

const checkoutLongDesc string = `Experimental: Checkout a point in the conversation for replay.

Fetches the conversation history up to the given hash from the API server
and saves the state as the starting point for a "tapes chat" session.

If no hash is provided, clears the checkout state so the next chat session
starts a new root conversation.

Examples:
  tapes checkout abc123def456   Checkout a specific conversation point
  tapes checkout                Clear checkout state, start fresh`

const checkoutShortDesc string = "Checkout a conversation point"

func NewCheckoutCmd() *cobra.Command {
	cmder := &checkoutCommander{
		flags: checkoutFlags,
	}

	cmd := &cobra.Command{
		Use:   "checkout [hash]",
		Short: checkoutShortDesc,
		Long:  checkoutLongDesc,
		Args:  cobra.MaximumNArgs(1),
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			configDir, _ := cmd.Flags().GetString("config-dir")
			v, err := config.InitViper(configDir)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			config.BindRegisteredFlags(v, cmd, cmder.flags, []string{
				config.FlagAPITarget,
			})

			cmder.apiTarget = v.GetString("client.api_target")
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				cmder.hash = args[0]
			}

			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			return cmder.run()
		},
	}

	config.AddStringFlag(cmd, cmder.flags, config.FlagAPITarget, &cmder.apiTarget)

	return cmd
}

func (c *checkoutCommander) run() error {
	dotdirManager := dotdir.NewManager()
	c.logger = logger.New(logger.WithDebug(c.debug), logger.WithPretty(true))

	// If no hash provided, clear checkout state
	if c.hash == "" {
		if err := dotdirManager.ClearCheckout(""); err != nil {
			return fmt.Errorf("clearing checkout: %w", err)
		}
		fmt.Printf("\n  %s Checkout cleared. Next chat will start a new conversation.\n\n", cliui.SuccessMark)
		return nil
	}

	c.logger.Debug("checking out conversation",
		"hash", c.hash,
		"api_target", c.apiTarget,
	)

	// Fetch the session from the API
	var session *sessionResponse
	if err := cliui.Step(os.Stdout, "Fetching session", func() error {
		var fetchErr error
		session, fetchErr = c.fetchSession(c.hash)
		return fetchErr
	}); err != nil {
		return err
	}

	// Convert API turns to checkout messages
	messages := make([]dotdir.CheckoutMessage, 0, len(session.Turns))
	for _, t := range session.Turns {
		text := extractText(t.Content)
		messages = append(messages, dotdir.CheckoutMessage{
			Role:    t.Role,
			Content: text,
		})
	}

	// Save the checkout state
	state := &dotdir.CheckoutState{
		Hash:     session.Hash,
		Messages: messages,
	}
	if err := dotdirManager.SaveCheckout(state, ""); err != nil {
		return fmt.Errorf("saving checkout: %w", err)
	}

	fmt.Printf("\n  %s Checked out %s %s\n\n",
		cliui.SuccessMark,
		cliui.HashStyle.Render(utils.Truncate(session.Hash, 16)),
		cliui.DimStyle.Render(fmt.Sprintf("(%d messages)", len(messages))),
	)

	for _, msg := range messages {
		preview := utils.Truncate(msg.Content, 60)
		fmt.Printf("  %s %s\n",
			cliui.RoleStyle.Render("["+msg.Role+"]"),
			cliui.PreviewStyle.Render(preview),
		)
	}

	fmt.Println()
	return nil
}

// fetchSession calls the API to get the session chain for a given hash.
func (c *checkoutCommander) fetchSession(hash string) (*sessionResponse, error) {
	url := fmt.Sprintf("%s/v1/sessions/%s", c.apiTarget, hash)

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting session from API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading API response: %w", err)
	}

	var session sessionResponse
	if err := json.Unmarshal(body, &session); err != nil {
		return nil, fmt.Errorf("parsing API response: %w", err)
	}

	return &session, nil
}

// extractText concatenates all text content blocks from a message.
func extractText(content []llm.ContentBlock) string {
	var b strings.Builder
	for _, block := range content {
		if block.Type == "text" {
			b.WriteString(block.Text)
		}
	}
	return b.String()
}
