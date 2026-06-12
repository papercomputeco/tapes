package skill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/llm"
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
		turns, err := g.query.TraceSummaries(ctx, sessionID)
		if err != nil {
			return nil, fmt.Errorf("load session %s: %w", sessionID, err)
		}

		turns = filterTurns(turns, opts)
		if len(turns) == 0 {
			return nil, fmt.Errorf("no turns in session %s after applying time filters", sessionID)
		}

		transcript, err := g.buildTranscript(ctx, turns)
		if err != nil {
			return nil, fmt.Errorf("build transcript for session %s: %w", sessionID, err)
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
		if totalLen > maxChars {
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

		// Override with caller-supplied values
		skill.Name = name
		skill.Type = skillType
		skill.Sessions = sessionIDs
		skill.Version = "0.1.0"
		skill.CreatedAt = time.Now()

		return skill, nil
	}

	return nil, lastErr
}

// filterTurns drops synthetic turns (compaction seams, resume replays —
// no user intent to extract from) and applies the --since/--until
// window at turn grain.
func filterTurns(turns []TraceSummary, opts *GenerateOptions) []TraceSummary {
	var filtered []TraceSummary
	for _, turn := range turns {
		if turn.Synthetic != "" {
			continue
		}
		if opts != nil {
			if opts.Since != nil && turn.StartedAt.Before(*opts.Since) {
				continue
			}
			if opts.Until != nil && turn.StartedAt.After(*opts.Until) {
				continue
			}
		}
		filtered = append(filtered, turn)
	}
	return filtered
}

// buildTranscript renders the turn-grain transcript for one session.
// Per turn: the user prompt, then the main-thread conversation-spine
// llm responses in span order with tool usage summarized between them.
// When a turn's span detail is unavailable (or carries no spine text)
// the derive-time response preview stands in, so the transcript always
// has both halves of the exchange.
func (g *Generator) buildTranscript(ctx context.Context, turns []TraceSummary) (string, error) {
	var b strings.Builder
	for _, turn := range turns {
		if turn.UserPrompt != "" {
			fmt.Fprintf(&b, "[user] %s\n", turn.UserPrompt)
		}

		trace, err := g.query.Trace(ctx, turn.TraceID)
		if err != nil || trace == nil {
			if turn.ResponsePreview != "" {
				fmt.Fprintf(&b, "[assistant] %s\n", turn.ResponsePreview)
			}
			continue
		}

		if !writeSpineResponses(&b, trace.Spans) && turn.ResponsePreview != "" {
			fmt.Fprintf(&b, "[assistant] %s\n", turn.ResponsePreview)
		}
	}
	return b.String(), nil
}

// writeSpineResponses walks one turn's spans in presentation order,
// emitting an [assistant] line per conversation-spine llm span with
// text and a [tools] summary line for the tool calls in between.
// Offshoot and injected call kinds, and subagent threads, are skipped.
// Reports whether any assistant text was written.
func writeSpineResponses(b *strings.Builder, spans []Span) bool {
	wrote := false
	pendingTools := map[string]int{}
	var pendingOrder []string

	flushTools := func() {
		if len(pendingOrder) == 0 {
			return
		}
		parts := make([]string, 0, len(pendingOrder))
		for _, name := range pendingOrder {
			if count := pendingTools[name]; count > 1 {
				parts = append(parts, fmt.Sprintf("%s ×%d", name, count))
			} else {
				parts = append(parts, name)
			}
		}
		fmt.Fprintf(b, "[tools] %s\n", strings.Join(parts, ", "))
		pendingTools = map[string]int{}
		pendingOrder = nil
	}

	for _, sp := range spans {
		switch sp.Kind {
		case "tool":
			if sp.ThreadID != "" {
				continue
			}
			if _, seen := pendingTools[sp.Name]; !seen {
				pendingOrder = append(pendingOrder, sp.Name)
			}
			pendingTools[sp.Name]++
		case "llm":
			if sp.CallKind != "main" || sp.ThreadID != "" {
				continue
			}
			text := blocksText(sp.Output)
			if text == "" {
				continue
			}
			flushTools()
			fmt.Fprintf(b, "[assistant] %s\n", text)
			wrote = true
		}
	}
	flushTools()
	return wrote
}

// blocksText joins the visible text blocks of an llm span's output.
// Thinking blocks are intentionally excluded: they are model-internal
// and bloat the extraction prompt without adding workflow signal.
func blocksText(blocks []llm.ContentBlock) string {
	var texts []string
	for _, block := range blocks {
		if block.Text != "" {
			texts = append(texts, block.Text)
		}
	}
	return strings.Join(texts, "\n")
}

func buildSkillPrompt(transcript, name, skillType string) string {
	return fmt.Sprintf(`Analyze the following LLM coding session transcript(s) and extract a reusable skill.

The skill should be named %q and categorized as %q.

Transcript format: [user] lines are the human's prompts, [assistant]
lines are the agent's responses, and [tools] lines summarize the tools
the agent invoked between responses.

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
- Use the [tools] lines to capture which tools the workflow relies on
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
