package deck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

const (
	providerAnthropic = "anthropic"
	providerOpenAI    = "openai"

	modelClaudeSonnet = "claude-sonnet-4.5"
	modelClaudeHaiku  = "claude-haiku-4.5"
	modelGPT4o        = "gpt-4o"
	modelGPT4oMini    = "gpt-4o-mini"
)

type seedSession struct {
	Project  string
	Messages []seedMessage
}

type seedMessage struct {
	Role             string
	Model            string
	Provider         string
	Blocks           []llm.ContentBlock
	PromptTokens     int
	CompletionTokens int
	StopReason       string
	At               time.Time
	TotalDuration    time.Duration
	PromptDuration   time.Duration
}

type SeedDemoResponse struct {
	Sessions int `json:"sessions"`
	Messages int `json:"messages"`
}

func SeedDemoToDriver(ctx context.Context, driver storage.Driver) (int, int, error) {
	hasData, err := driverHasData(ctx, driver)
	if err != nil {
		return 0, 0, err
	}
	if hasData {
		return 0, 0, errors.New("database already has data (use --overwrite with a fresh local database)")
	}

	sessions := demoDeckSessions(time.Now())
	messageCount := 0
	for _, session := range sessions {
		if err := insertSessionToDriver(ctx, driver, session); err != nil {
			return 0, 0, err
		}
		messageCount += len(session.Messages)
	}

	return len(sessions), messageCount, nil
}

func SeedDemoViaAPI(ctx context.Context, apiTarget string, overwrite bool) (int, int, error) {
	payload, err := json.Marshal(map[string]bool{"overwrite": overwrite})
	if err != nil {
		return 0, 0, fmt.Errorf("marshal seed request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(apiTarget, "/")+"/v1/admin/seed/demo", bytes.NewReader(payload))
	if err != nil {
		return 0, 0, fmt.Errorf("create seed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("seed demo via api: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, fmt.Errorf("read seed response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("seed api returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result SeedDemoResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, 0, fmt.Errorf("decode seed response: %w", err)
	}

	return result.Sessions, result.Messages, nil
}

func driverHasData(ctx context.Context, driver storage.Driver) (bool, error) {
	nodes, err := driver.List(ctx)
	if err != nil {
		return false, fmt.Errorf("check database: %w", err)
	}

	return len(nodes) > 0, nil
}

func insertSessionToDriver(ctx context.Context, driver storage.Driver, session seedSession) error {
	var parent *merkle.Node
	for _, message := range session.Messages {
		bucket := merkle.Bucket{
			Type:     "message",
			Role:     message.Role,
			Content:  message.Blocks,
			Model:    message.Model,
			Provider: message.Provider,
		}

		usage := buildUsage(message)
		node := merkle.NewNode(bucket, parent, merkle.NodeOptions{
			StopReason: message.StopReason,
			Usage:      usage,
			Project:    session.Project,
		})
		node.CreatedAt = message.At

		if _, err := driver.Put(ctx, node); err != nil {
			return fmt.Errorf("create node: %w", err)
		}

		parent = node
	}

	return nil
}

func buildUsage(message seedMessage) *llm.Usage {
	if message.PromptTokens == 0 && message.CompletionTokens == 0 {
		return nil
	}

	totalTokens := message.PromptTokens + message.CompletionTokens
	usage := &llm.Usage{
		PromptTokens:     message.PromptTokens,
		CompletionTokens: message.CompletionTokens,
		TotalTokens:      totalTokens,
	}

	if message.TotalDuration > 0 {
		usage.TotalDurationNs = message.TotalDuration.Nanoseconds()
	}

	if message.PromptDuration > 0 {
		usage.PromptDurationNs = message.PromptDuration.Nanoseconds()
	}

	return usage
}

func demoDeckSessions(now time.Time) []seedSession {
	sessions := []seedSession{
		codeReviewSession(now.Add(-6 * time.Hour)),
		bugHuntSession(now.Add(-12 * time.Hour)),
		rateLimitSession(now.Add(-18 * time.Hour)),
		infraCheckSession(now.Add(-24 * time.Hour)),
		deployFailureSession(now.Add(-30 * time.Hour)),
		quickQuestionSession(now.Add(-36 * time.Hour)),
		abandonedResearchSession(now.Add(-42 * time.Hour)),
	}

	// Distribute sessions across projects
	projects := []string{"tapes", "tapes", "acme-api", "acme-api", "infra-ops", "tapes", "acme-api"}
	for i := range sessions {
		sessions[i].Project = projects[i]
	}

	return sessions
}

func codeReviewSession(base time.Time) seedSession {
	model := modelClaudeSonnet
	provider := providerAnthropic
	return seedSession{
		Messages: []seedMessage{
			userMessage(base, 0, model, provider, "Can you review PR #342? It adds JWT auth to the API.", 2400),
			assistantMessage(base, 8*time.Second, model, provider, "Reviewing the auth implementation now.", 3000, 1200, toolUseBlocks("Read", map[string]any{"path": "auth/jwt.go"})),
			assistantMessage(base, 30*time.Second, model, provider, "Found issues: hardcoded JWT secret and missing rate limiting.", 3800, 1800, toolUseBlocks("Grep", map[string]any{"query": "JWT_SECRET"})),
			userMessage(base, 2*time.Minute, model, provider, "Please fix those and add a rate limiter.", 1600),
			assistantMessage(base, 2*time.Minute+10*time.Second, model, provider, "Updated env config and added a rate limit middleware.", 3200, 1500, toolUseBlocks("Edit", map[string]any{"path": "middleware/rate_limit.go"})),
			assistantMessageWithStop(base, 6*time.Minute, model, provider, "Added refresh tokens and tests. All checks passing.", 2800, 1700, "stop", toolUseBlocks("Write", map[string]any{"path": "auth/refresh.go"})),
		},
	}
}

func bugHuntSession(base time.Time) seedSession {
	model := modelClaudeSonnet
	provider := providerAnthropic
	return seedSession{
		Messages: []seedMessage{
			userMessage(base, 0, model, provider, "Memory usage climbs until the worker crashes. Can you investigate?", 3200),
			assistantMessage(base, 12*time.Second, model, provider, "Investigating worker behavior and connection handling.", 3400, 1400, toolUseBlocks("Read", map[string]any{"path": "worker/processor.go"})),
			assistantMessage(base, 35*time.Second, model, provider, "Found a DB connection leak and a growing in-memory cache.", 4200, 1900, toolUseBlocks("Grep", map[string]any{"query": "Open("})),
			userMessage(base, 2*time.Minute, model, provider, "Please fix both issues.", 1500),
			assistantMessage(base, 2*time.Minute+20*time.Second, model, provider, "Added defer Close() for DB connections.", 3100, 1200, toolUseBlocks("Edit", map[string]any{"path": "worker/db.go"})),
			assistantMessage(base, 4*time.Minute, model, provider, "Implemented an LRU cache with TTL.", 3600, 1600, toolUseBlocks("Write", map[string]any{"path": "worker/cache.go"})),
			assistantMessage(base, 6*time.Minute, model, provider, "Added memory metrics and alerts.", 3300, 1500, toolUseBlocks("Write", map[string]any{"path": "worker/metrics.go"})),
			userMessage(base, 8*time.Minute, model, provider, "Nice. Add a summary for the PR.", 1200),
			assistantMessageWithStop(base, 9*time.Minute, model, provider, "Summary: fixed DB leak, bounded cache, added monitoring.", 2800, 1100, "stop", nil),
		},
	}
}

func rateLimitSession(base time.Time) seedSession {
	model := modelGPT4o
	provider := providerOpenAI
	return seedSession{
		Messages: []seedMessage{
			userMessage(base, 0, model, provider, "We need rate limiting across auth and read endpoints. Can you implement this?", 3600),
			assistantMessage(base, 10*time.Second, model, provider, "Reviewing the API routes and middleware stack.", 2800, 1200, toolUseBlocks("Read", map[string]any{"path": "api/routes.go"})),
			userMessage(base, 1*time.Minute, model, provider, "Auth should be strict. Reads can be higher throughput.", 1600),
			assistantMessage(base, 1*time.Minute+20*time.Second, model, provider, "Designing a per-route rate limiter with Redis backing.", 3100, 1400, toolUseBlocks("Write", map[string]any{"path": "middleware/ratelimit.go"})),
			assistantMessage(base, 3*time.Minute, model, provider, "Hooked middleware into auth and read routes.", 2900, 1300, toolUseBlocks("Edit", map[string]any{"path": "api/routes.go"})),
			assistantMessage(base, 4*time.Minute+30*time.Second, model, provider, "Added config options for per-route limits.", 2600, 1200, toolUseBlocks("Edit", map[string]any{"path": "config/config.go"})),
			assistantMessageWithStop(base, 6*time.Minute, model, provider, "Tests added and passing.", 2400, 1000, "stop", toolUseBlocks("Write", map[string]any{"path": "middleware/ratelimit_test.go"})),
		},
	}
}

func infraCheckSession(base time.Time) seedSession {
	model := modelClaudeHaiku
	provider := providerAnthropic
	return seedSession{
		Messages: []seedMessage{
			assistantMessage(base, 0, model, provider, "Starting nightly health check for the cluster.", 1200, 600, toolUseBlocks("Bash", map[string]any{"command": "kubectl get pods"})),
			assistantMessage(base, 25*time.Second, model, provider, "All pods healthy. CPU 34%, memory 2.1GB/4GB.", 1100, 500, toolUseBlocks("Bash", map[string]any{"command": "kubectl top pods"})),
			assistantMessage(base, 55*time.Second, model, provider, "Database connections stable, queue depth nominal.", 1000, 450, toolUseBlocks("Bash", map[string]any{"command": "check-db"})),
			assistantMessageWithStop(base, 90*time.Second, model, provider, "Health check complete. No critical issues.", 900, 400, "stop", nil),
		},
	}
}

func deployFailureSession(base time.Time) seedSession {
	model := modelGPT4o
	provider := providerOpenAI
	return seedSession{
		Messages: []seedMessage{
			userMessage(base, 0, model, provider, "Deploy failed in production. Can you check the logs?", 2600),
			assistantMessage(base, 12*time.Second, model, provider, "Checking deployment logs.", 2400, 900, toolUseBlocks("Bash", map[string]any{"command": "kubectl logs deploy/api"})),
			assistantMessageWithStop(base, 40*time.Second, model, provider, "Found a migration error. The deploy rolled back.", 2800, 800, "tool_use", toolResultBlocks("migration failed: missing column", true)),
		},
	}
}

func quickQuestionSession(base time.Time) seedSession {
	model := modelGPT4oMini
	provider := providerOpenAI
	return seedSession{
		Messages: []seedMessage{
			userMessage(base, 0, model, provider, "What is the syntax for Go generics?", 1200),
			assistantMessageWithStop(base, 6*time.Second, model, provider, "Use type parameters like func Max[T constraints.Ordered](a, b T) T.", 800, 1600, "stop", nil),
		},
	}
}

func abandonedResearchSession(base time.Time) seedSession {
	model := modelClaudeSonnet
	provider := providerAnthropic
	return seedSession{
		Messages: []seedMessage{
			userMessage(base, 0, model, provider, "Research pricing changes for the next quarter.", 1800),
			assistantMessage(base, 12*time.Second, model, provider, "Gathering vendor updates and competitor pricing.", 2200, 900, toolUseBlocks("Read", map[string]any{"path": "docs/pricing.md"})),
			userMessage(base, 5*time.Minute, model, provider, "Pause this for now. We'll revisit later.", 900),
		},
	}
}

func userMessage(base time.Time, offset time.Duration, model, provider, text string, promptTokens int) seedMessage {
	return seedMessage{
		Role:         "user",
		Model:        model,
		Provider:     provider,
		Blocks:       textBlocks(text),
		PromptTokens: promptTokens,
		At:           base.Add(offset),
	}
}

func assistantMessage(base time.Time, offset time.Duration, model, provider, text string, promptTokens, completionTokens int, extraBlocks []llm.ContentBlock) seedMessage {
	blocks := append(textBlocks(text), extraBlocks...)
	return seedMessage{
		Role:             "assistant",
		Model:            model,
		Provider:         provider,
		Blocks:           blocks,
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		At:               base.Add(offset),
		TotalDuration:    2 * time.Second,
		PromptDuration:   500 * time.Millisecond,
	}
}

func assistantMessageWithStop(base time.Time, offset time.Duration, model, provider, text string, promptTokens, completionTokens int, stopReason string, extraBlocks []llm.ContentBlock) seedMessage {
	msg := assistantMessage(base, offset, model, provider, text, promptTokens, completionTokens, extraBlocks)
	msg.StopReason = stopReason
	return msg
}

func textBlocks(text string) []llm.ContentBlock {
	return []llm.ContentBlock{{Type: "text", Text: text}}
}

func toolUseBlocks(name string, input map[string]any) []llm.ContentBlock {
	return []llm.ContentBlock{{
		Type:      "tool_use",
		ToolName:  name,
		ToolInput: input,
	}}
}

func toolResultBlocks(output string, isError bool) []llm.ContentBlock {
	return []llm.ContentBlock{{
		Type:       "tool_result",
		ToolOutput: output,
		IsError:    isError,
	}}
}
