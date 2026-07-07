// Package reflection extracts a human-facing session reflection from a session
// transcript via an LLM (PCC-241): a 2-3 sentence narrative of what the
// person/agent set out to do and how it went, plus an optional transferable
// observation — a reflection is a chance for an observation.
//
// It rides the skill pipeline's machinery wholesale: skill.Querier /
// skill.BuildSessionTranscript for the conversation-spine transcript (no
// shadow traffic) and skill.LLMCallFunc for the provider-agnostic inference
// call, so reflection generation needs no plumbing of its own.
package reflection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/papercomputeco/tapes/pkg/skill"
)

const maxGenerateRetries = 3

// maxTranscriptChars caps the transcript handed to the LLM. Unlike skill
// extraction (which truncates at session boundaries across many sessions), a
// reflection reads one session — and its opening (the goal) and ending (the
// outcome) matter most — so an oversized transcript keeps its head and tail
// and elides the middle.
const maxTranscriptChars = 30000

// Reflection is the LLM's extraction: the narrative plus an optional observation.
type Reflection struct {
	// Narrative is 2-3 sentences: what the person/agent set out to do, the
	// approach, and how it went (present tense while the session is live).
	Narrative string `json:"narrative"`
	// Observation is a single transferable insight (a gotcha, repeated
	// pattern, or workflow heuristic) worth remembering beyond this session.
	// Empty when the pass found nothing genuinely noteworthy.
	Observation string `json:"observation"`
}

// Generator extracts session reflections via an LLM, mirroring skill.Generator:
// the querier reads the span-model transcript surface and llmCall runs the
// inference.
type Generator struct {
	query   skill.Querier
	llmCall skill.LLMCallFunc
}

// NewGenerator creates a new reflection Generator.
func NewGenerator(query skill.Querier, llmCall skill.LLMCallFunc) *Generator {
	return &Generator{
		query:   query,
		llmCall: llmCall,
	}
}

// Generate builds the session's transcript and runs one LLM pass to extract
// its reflection. live selects the narrative's tense: present ("is working on…")
// for a session still running, past for a settled one.
func (g *Generator) Generate(ctx context.Context, sessionID string, live bool) (*Reflection, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, errors.New("session ID is required")
	}

	transcript, err := skill.BuildSessionTranscript(ctx, g.query, sessionID)
	if err != nil {
		return nil, err
	}
	transcript = elideMiddle(transcript, maxTranscriptChars)

	basePrompt := buildReflectionPrompt(transcript, live)

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

		reflection, err := parseReflectionResponse(response)
		if err != nil {
			lastErr = fmt.Errorf("parse response (attempt %d): %w", attempt+1, err)
			continue
		}
		return reflection, nil
	}

	return nil, lastErr
}

// elideMiddle caps s at max chars by keeping the head and tail and dropping
// the middle — for a reflection, the session's opening (goal) and ending (outcome)
// carry the signal.
func elideMiddle(s string, maxChars int) string {
	if len(s) <= maxChars {
		return s
	}
	const marker = "\n[... transcript elided ...]\n"
	if maxChars <= len(marker) {
		// Too small to fit even the marker — hard-truncate rather than
		// underflow the tail slice below.
		return s[:maxChars]
	}
	head := (maxChars - len(marker)) * 2 / 3
	tail := maxChars - len(marker) - head
	return s[:head] + marker + s[len(s)-tail:]
}

func buildReflectionPrompt(transcript string, live bool) string {
	tense := `The session has ENDED. Write the narrative in the past tense ("set out to…", "landed…", "failed because…").`
	if live {
		tense = `The session is STILL RUNNING. Write the narrative in the present tense ("is working on…", "is currently…").`
	}
	return fmt.Sprintf(`Analyze the following LLM coding session transcript and write a reflection.

%s

Transcript format: [user] lines are the AUTHOR's prompts (the human driving
the session), [assistant] lines are the agent's responses, and [tools] lines
summarize the tools the agent invoked between responses.

Return ONLY valid JSON with these fields:

{
  "narrative": "2-3 sentences about what the AUTHOR is trying to accomplish and where that effort stands. No preamble — start with the substance.",
  "observation": "OPTIONAL: one transferable insight from this session (a gotcha hit, a pattern repeated, a workflow heuristic) worth remembering beyond it. Empty string if nothing is genuinely noteworthy."
}

Guidelines:
- The narrative is about the AUTHOR, not the agent: it answers "what is this
  person trying to get done, and how is it going?" in the author's own terms
  (read their [user] prompts for intent). Do not narrate the agent's activity.
- Never open with "The agent". Open with the author's objective — e.g.
  "Setting up …", "Debugging …", "Trying to ship …" — or the author themselves.
- Mention the agent's work only where it explains progress, a blocker, or the
  outcome — never as a list of tools invoked or features exercised.
- The narrative is for a teammate skimming the session — say what actually
  happened, including failures.
- Do not restate raw metrics (turn counts, token counts, cost); the page
  already shows them.
- Only include an observation when it would help on a FUTURE session; do not
  manufacture one.

Transcript:
%s`, tense, transcript)
}

func parseReflectionResponse(response string) (*Reflection, error) {
	jsonStr := response
	if idx := strings.Index(response, "{"); idx >= 0 {
		endIdx := strings.LastIndex(response, "}")
		if endIdx > idx {
			jsonStr = response[idx : endIdx+1]
		}
	}

	var reflection Reflection
	if err := json.Unmarshal([]byte(jsonStr), &reflection); err != nil {
		return nil, fmt.Errorf("unmarshal reflection JSON: %w", err)
	}
	reflection.Narrative = strings.TrimSpace(reflection.Narrative)
	reflection.Observation = strings.TrimSpace(reflection.Observation)
	if reflection.Narrative == "" {
		return nil, errors.New("reflection narrative is empty")
	}
	return &reflection, nil
}
