// Package checkoutcmder provides the checkout subcommand for checking out
// a point in the conversation DAG.
package checkoutcmder

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/dotdir"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/utils"
)

type checkoutCommander struct {
	hash  string
	api   string
	debug bool

	logger *zap.Logger
}

// historyResponse mirrors the API's HistoryResponse type for JSON deserialization.
type historyResponse struct {
	Messages []historyMessage `json:"messages"`
	HeadHash string           `json:"head_hash"`
	Depth    int              `json:"depth"`
}

// historyMessage mirrors the API's HistoryMessage type.
type historyMessage struct {
	Hash       string             `json:"hash"`
	ParentHash *string            `json:"parent_hash,omitempty"`
	Role       string             `json:"role"`
	Content    []llm.ContentBlock `json:"content"`
	Model      string             `json:"model,omitempty"`
	Provider   string             `json:"provider,omitempty"`
	StopReason string             `json:"stop_reason,omitempty"`
	Usage      *llm.Usage         `json:"usage,omitempty"`
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
	cmder := &checkoutCommander{}

	cmd := &cobra.Command{
		Use:   "checkout [hash]",
		Short: checkoutShortDesc,
		Long:  checkoutLongDesc,
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				cmder.hash = args[0]
			}

			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %w", err)
			}

			cmder.api, err = cmd.Flags().GetString("api")
			if err != nil {
				return fmt.Errorf("could not get api flag: %w", err)
			}

			return cmder.run()
		},
	}

	cmd.PersistentFlags().StringVarP(&cmder.api, "api", "a", "http://localhost:8081", "Tapes API server address")

	return cmd
}

func (c *checkoutCommander) run() error {
	dotdirManager := dotdir.NewManager()
	c.logger = logger.NewLogger(c.debug)
	defer func() { _ = c.logger.Sync() }()

	// If no hash provided, clear checkout state
	if c.hash == "" {
		if err := dotdirManager.ClearCheckout(""); err != nil {
			return fmt.Errorf("clearing checkout: %w", err)
		}
		fmt.Println("Checkout cleared. Next chat will start a new conversation.")
		return nil
	}

	c.logger.Debug("checking out conversation",
		zap.String("hash", c.hash),
		zap.String("api", c.api),
	)

	// Fetch the conversation history from the API
	history, err := c.fetchHistory(c.hash)
	if err != nil {
		return fmt.Errorf("fetching history: %w", err)
	}

	// Convert API messages to checkout messages
	messages := make([]dotdir.CheckoutMessage, 0, len(history.Messages))
	for _, msg := range history.Messages {
		text := extractText(msg.Content)
		messages = append(messages, dotdir.CheckoutMessage{
			Role:    msg.Role,
			Content: text,
		})
	}

	// Save the checkout state
	state := &dotdir.CheckoutState{
		Hash:     history.HeadHash,
		Messages: messages,
	}
	if err := dotdirManager.SaveCheckout(state, ""); err != nil {
		return fmt.Errorf("saving checkout: %w", err)
	}

	fmt.Printf("Checked out %s (%d messages)\n", utils.Truncate(history.HeadHash, 16), len(messages))
	for _, msg := range messages {
		preview := utils.Truncate(msg.Content, 60)
		fmt.Printf("  [%s] %s\n", msg.Role, preview)
	}

	return nil
}

// fetchHistory calls the API to get the conversation history for a given hash.
func (c *checkoutCommander) fetchHistory(hash string) (*historyResponse, error) {
	url := fmt.Sprintf("%s/dag/history/%s", c.api, hash)

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting history from API: %w", err)
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

	var history historyResponse
	if err := json.Unmarshal(body, &history); err != nil {
		return nil, fmt.Errorf("parsing API response: %w", err)
	}

	return &history, nil
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
