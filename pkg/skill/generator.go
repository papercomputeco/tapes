package skill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const maxGenerateRetries = 3

// GenerateOptions controls filtering for skill generation.
type GenerateOptions struct {
	Since *time.Time // only include turns starting on or after this time
	Until *time.Time // only include turns starting on or before this time
}

// Generator extracts skills from session transcripts via an LLM.
//
// Transcripts are built from the span model: each user-visible turn
// contributes its prompt plus the conversation-spine ("main" call-kind,
// main-thread) llm span outputs, with tool usage summarized between
// responses. Offshoot calls (permission checks, title-gen, …) and
// injected context never reach the prompt — the extraction LLM sees the
// actual conversation, not the harness's shadow traffic.
type Generator struct {
	query   Querier
	llmCall LLMCallFunc
}

// NewGenerator creates a new skill Generator.
func NewGenerator(query Querier, llmCall LLMCallFunc) *Generator {
	return &Generator{
		query:   query,
		llmCall: llmCall,
	}
}

// Generate extracts a skill from one or more session IDs. Each ID is a
// product session (a /v1/sessions UUID); its derived turn/span
// projection is loaded as the conversation transcript.
func (g *Generator) Generate(ctx context.Context, sessionIDs []string, name, skillType string, opts *GenerateOptions) (*Skill, error) {
	if len(sessionIDs) == 0 {
		return nil, errors.New("at least one session ID is required")
	}

	if !ValidSkillType(skillType) {
		return nil, fmt.Errorf("invalid skill type %q; valid types: %s", skillType, strings.Join(SkillTypes, ", "))
	}

	var transcripts []string
	for _, sessionID := range sessionIDs {
		transcript, err := BuildSessionTranscript(ctx, g.query, sessionID, WithTimeFilter(opts))
		if err != nil {
			return nil, err
		}
		transcripts = append(transcripts, transcript)
	}

	// Truncate large transcripts at session boundary
	const maxChars = 30000
	var totalLen int
	for i, t := range transcripts {
		totalLen += len(t)
		if i > 0 {
			totalLen += 5 // len("\n---\n")
		}
		// Keep at least the first session: dropping to transcripts[:0] would
		// feed the LLM an empty prompt. A single oversized transcript is sent
		// whole (well within the model's context window).
		if i > 0 && totalLen > maxChars {
			transcripts = transcripts[:i]
			fmt.Fprintf(os.Stderr, "warning: transcript truncated to %d of %d session(s) to fit within %d char limit\n",
				len(transcripts), len(sessionIDs), maxChars)
			break
		}
	}

	combined := strings.Join(transcripts, "\n---\n")

	basePrompt := buildSkillPrompt(combined, name, skillType)

	var lastErr error
	for attempt := range maxGenerateRetries {
		prompt := basePrompt
		if attempt > 0 {
			prompt += "\n\nReturn ONLY valid JSON, no markdown."
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

		// Use the caller-supplied name when given; otherwise keep the LLM's
		// suggested name (the slug/identifier is derived from it downstream).
		if strings.TrimSpace(name) != "" {
			skill.Name = name
		}
		skill.Name = strings.TrimSpace(skill.Name)
		skill.Type = skillType
		skill.Sessions = sessionIDs
		skill.Version = "0.1.0"
		skill.CreatedAt = time.Now()

		return skill, nil
	}

	return nil, lastErr
}

func buildSkillPrompt(transcript, name, skillType string) string {
	// When the caller supplies a name we pin it; otherwise we ask the model to
	// suggest a short human-readable title (the slug is derived from it).
	nameInstruction := fmt.Sprintf("The skill should be named %q and categorized as %q.", name, skillType)
	nameField := ""
	if strings.TrimSpace(name) == "" {
		nameInstruction = fmt.Sprintf("Categorize the skill as %q and suggest a concise, descriptive name for it.", skillType)
		nameField = "  \"name\": \"a short human-readable skill title, e.g. Diagnose Flaky Tests\",\n"
	}
	return fmt.Sprintf(`Analyze the following LLM coding session transcript(s) and extract a reusable skill.

%s

Transcript format: [user] lines are the human's prompts, [assistant]
lines are the agent's responses, and [tools] lines summarize the tools
the agent invoked between responses.

Return ONLY valid JSON with these fields:

{
%s  "description": "A clear description with trigger phrases for when an agent should use this skill. Start with an action verb.",
  "tags": ["array", "of", "relevant", "tags"],
  "content": "Markdown body with step-by-step instructions in imperative form. Use ## headers and numbered steps."
}

Guidelines for extraction:
- Identify the reusable pattern/workflow from the session(s)
- Write a clear description with trigger phrases (e.g. "Use when debugging React hooks issues")
- Write step-by-step instructions in imperative form
- Focus on the generalizable technique, not session-specific details
- Use the [tools] lines to capture which tools the workflow relies on
- Include any important caveats or edge cases observed

Transcript(s):
%s`, nameInstruction, nameField, transcript)
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
