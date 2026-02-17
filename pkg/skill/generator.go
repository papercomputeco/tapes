package skill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/deck"
)

const maxGenerateRetries = 3

// GenerateOptions controls filtering for skill generation.
type GenerateOptions struct {
	Since *time.Time // only include messages on or after this time
	Until *time.Time // only include messages on or before this time
}

// Generator extracts skills from session transcripts via an LLM.
type Generator struct {
	query   deck.Querier
	llmCall deck.LLMCallFunc
}

// NewGenerator creates a new skill Generator.
func NewGenerator(query deck.Querier, llmCall deck.LLMCallFunc) *Generator {
	return &Generator{
		query:   query,
		llmCall: llmCall,
	}
}

// Generate extracts a skill from one or more conversation hashes.
// Each hash is a leaf node in the Merkle DAG; its ancestry chain is loaded
// as the conversation transcript.
func (g *Generator) Generate(ctx context.Context, hashes []string, name, skillType string, opts *GenerateOptions) (*Skill, error) {
	if len(hashes) == 0 {
		return nil, errors.New("at least one hash is required")
	}

	if !ValidSkillType(skillType) {
		return nil, fmt.Errorf("invalid skill type %q; valid types: %s", skillType, strings.Join(SkillTypes, ", "))
	}

	var transcripts []string
	for _, hash := range hashes {
		detail, err := g.query.SessionDetail(ctx, hash)
		if err != nil {
			return nil, fmt.Errorf("load conversation %s: %w", hash, err)
		}

		messages := filterMessages(detail.Messages, opts)
		if len(messages) == 0 {
			return nil, fmt.Errorf("no messages in conversation %s after applying time filters", hash)
		}

		transcripts = append(transcripts, buildTranscript(messages))
	}

	combined := strings.Join(transcripts, "\n---\n")

	// Truncate large transcripts
	const maxChars = 30000
	if len(combined) > maxChars {
		combined = combined[:maxChars]
	}

	basePrompt := buildSkillPrompt(combined, name, skillType)

	var lastErr error
	for attempt := range maxGenerateRetries {
		prompt := basePrompt
		if attempt > 0 {
			prompt += "\n\nReturn ONLY valid JSON, no markdown."
			log.Printf("skill: retrying generation for %q (attempt %d/%d)", name, attempt+1, maxGenerateRetries)
		}

		response, err := g.llmCall(ctx, prompt)
		if err != nil {
			return nil, fmt.Errorf("llm call: %w", err)
		}

		skill, err := parseSkillResponse(response)
		if err != nil {
			lastErr = fmt.Errorf("parse response (attempt %d): %w", attempt+1, err)
			continue
		}

		// Override with caller-supplied values
		skill.Name = name
		skill.Type = skillType
		skill.Sessions = hashes
		skill.Version = "0.1.0"
		skill.CreatedAt = time.Now()

		return skill, nil
	}

	return nil, lastErr
}

func filterMessages(messages []deck.SessionMessage, opts *GenerateOptions) []deck.SessionMessage {
	if opts == nil {
		return messages
	}

	var filtered []deck.SessionMessage
	for _, msg := range messages {
		if opts.Since != nil && msg.Timestamp.Before(*opts.Since) {
			continue
		}
		if opts.Until != nil && msg.Timestamp.After(*opts.Until) {
			continue
		}
		filtered = append(filtered, msg)
	}
	return filtered
}

func buildTranscript(messages []deck.SessionMessage) string {
	var b strings.Builder
	for _, msg := range messages {
		fmt.Fprintf(&b, "[%s] %s\n", msg.Role, msg.Text)
	}
	return b.String()
}

func buildSkillPrompt(transcript, name, skillType string) string {
	return fmt.Sprintf(`Analyze the following LLM coding session transcript(s) and extract a reusable skill.

The skill should be named %q and categorized as %q.

Return ONLY valid JSON with these fields:

{
  "description": "A clear description with trigger phrases for when Claude should use this skill. Start with an action verb.",
  "tags": ["array", "of", "relevant", "tags"],
  "content": "Markdown body with step-by-step instructions in imperative form. Use ## headers and numbered steps."
}

Guidelines for extraction:
- Identify the reusable pattern/workflow from the session(s)
- Write a clear description with trigger phrases (e.g. "Use when debugging React hooks issues")
- Write step-by-step instructions in imperative form
- Focus on the generalizable technique, not session-specific details
- Include any important caveats or edge cases observed

Transcript(s):
%s`, name, skillType, transcript)
}

func parseSkillResponse(response string) (*Skill, error) {
	jsonStr := response
	if idx := strings.Index(response, "{"); idx >= 0 {
		endIdx := strings.LastIndex(response, "}")
		if endIdx > idx {
			jsonStr = response[idx : endIdx+1]
		}
	}

	var skill Skill
	if err := json.Unmarshal([]byte(jsonStr), &skill); err != nil {
		return nil, fmt.Errorf("unmarshal skill JSON: %w", err)
	}

	return &skill, nil
}
