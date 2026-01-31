// Package chatcmder provides the chat command for interactive LLM chat
// through the tapes proxy.
package chatcmder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/dotdir"
	"github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/utils"
)

type chatCommander struct {
	proxy string
	api   string
	model string
	debug bool

	logger *zap.Logger
}

// @jpmcb: TODO - we should adopt other providers with a -p --provider
// flag and utilize the native pkg/llm/provider/ packages
// vs. these hard coded Ollama request / responses.

// ollamaRequest is the Ollama-native request format.
// The chat command acts as a transparent Ollama client, sending requests
// through the tapes proxy.
type ollamaRequest struct {
	Model    string          `json:"model"`
	Messages []ollamaMessage `json:"messages"`
	Stream   bool            `json:"stream"`
}

// ollamaMessage is an Ollama-native message.
type ollamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ollamaStreamChunk represents a single streaming response chunk from Ollama.
type ollamaStreamChunk struct {
	Model     string        `json:"model"`
	CreatedAt time.Time     `json:"created_at"`
	Message   ollamaMessage `json:"message"`
	Done      bool          `json:"done"`
}

const chatLongDesc string = `Experimental: Start an interactive chat session through the tapes proxy.

The chat command sends messages to an LLM through the configured tapes proxy,
which transparently records the conversation in the Merkle DAG.
Supported providers: Ollama.

If a checkout state exists (from "tapes checkout"), the conversation
resumes from that point. Re-running "tapes chat" always starts from the
same checked-out hash - it does not advance the checkout state.

Use "tapes checkout <hash>" to checkout a specific conversation point,
or "tapes checkout" (no hash provided) to clear the checkout and start fresh.

Examples:
  tapes chat --model llama3.2
  tapes chat --model llama3.2 --proxy http://localhost:8080`

const chatShortDesc string = "Experimental: Interactive LLM chat through the tapes proxy"

func NewChatCmd() *cobra.Command {
	cmder := &chatCommander{}

	cmd := &cobra.Command{
		Use:   "chat",
		Short: chatShortDesc,
		Long:  chatLongDesc,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			cmder.debug, err = cmd.Flags().GetBool("debug")
			if err != nil {
				return fmt.Errorf("could not get debug flag: %v", err)
			}

			return cmder.run()
		},
	}

	cmd.PersistentFlags().StringVarP(&cmder.api, "api", "a", "http://localhost:8081", "Tapes API server address")

	cmd.Flags().StringVarP(&cmder.proxy, "proxy", "p", "http://localhost:8080", "Tapes proxy address")
	cmd.Flags().StringVarP(&cmder.model, "model", "m", "gemma3:latest", "Model name (e.g., gemma3:1b, ministral-3:latest)")

	return cmd
}

func (c *chatCommander) run() error {
	c.logger = logger.NewLogger(c.debug)
	defer c.logger.Sync()

	// Load checkout state
	dotdirManager := dotdir.NewManager()
	checkout, err := dotdirManager.LoadCheckoutState("")
	if err != nil {
		return fmt.Errorf("loading checkout state: %w", err)
	}

	// Build initial message history from checkout
	var messages []ollamaMessage
	if checkout != nil {
		fmt.Printf("Resuming from checkout %s (%d messages)\n",
			utils.Truncate(checkout.Hash, 16), len(checkout.Messages))
		for _, msg := range checkout.Messages {
			messages = append(messages, ollamaMessage{
				Role:    msg.Role,
				Content: msg.Content,
			})
		}
		fmt.Println()
	} else {
		fmt.Println("Starting new conversation (no checkout)")
		fmt.Println()
	}

	fmt.Println("Type your message and press Enter. Type /exit or Ctrl+D to quit.")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("you> ")
		if !scanner.Scan() {
			// EOF or error
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}
		if input == "/exit" {
			break
		}

		// Append user message
		messages = append(messages, ollamaMessage{
			Role:    "user",
			Content: input,
		})

		// Send to proxy and stream response
		assistantContent, err := c.sendAndStream(messages)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			// Remove the failed user message so we can retry
			messages = messages[:len(messages)-1]
			continue
		}

		// Append assistant response to history
		messages = append(messages, ollamaMessage{
			Role:    "assistant",
			Content: assistantContent,
		})

		fmt.Println()
		fmt.Println()
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading input: %w", err)
	}

	fmt.Println()
	return nil
}

// sendAndStream sends a chat request to the proxy and streams the response to stdout.
// Returns the full assistant response text.
func (c *chatCommander) sendAndStream(messages []ollamaMessage) (string, error) {
	reqBody := ollamaRequest{
		Model:    c.model,
		Messages: messages,
		Stream:   true,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	c.logger.Debug("sending chat request",
		zap.String("proxy", c.proxy),
		zap.String("model", c.model),
		zap.Int("message_count", len(messages)),
	)

	// POST to the proxy's Ollama-compatible chat endpoint
	url := c.proxy + "/api/chat"
	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		// LLM responses can be slow
		Timeout: 5 * time.Minute,
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("sending request to proxy: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("proxy returned status %d: %s", resp.StatusCode, string(respBody))
	}

	// Stream the response
	fmt.Print("assistant> ")

	var fullContent strings.Builder
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var chunk ollamaStreamChunk
		if err := json.Unmarshal(line, &chunk); err != nil {
			c.logger.Debug("failed to parse stream chunk",
				zap.Error(err),
				zap.String("line", string(line)),
			)
			continue
		}

		// Print the content token to stdout
		if chunk.Message.Content != "" {
			fmt.Print(chunk.Message.Content)
			fullContent.WriteString(chunk.Message.Content)
		}

		if chunk.Done {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return fullContent.String(), fmt.Errorf("reading stream: %w", err)
	}

	return fullContent.String(), nil
}
