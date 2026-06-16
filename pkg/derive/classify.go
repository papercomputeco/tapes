package derive

import (
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Node kinds — the design doc's §2g taxonomy. A "session" on the wire
// is many API calls of different kinds: the conversation spine plus
// shadow calls the harness fires on the user's behalf, plus injected
// context blocks. The set is OPEN: new kinds get cataloged here and a
// re-derive reclassifies all existing raw data.
const (
	// KindMain is the conversation spine: full tool set, streaming,
	// high max_tokens.
	KindMain = "main"

	// KindUnknown marks calls that match no cataloged tell. A non-zero
	// unknown count is either a genuinely new harness side-call to
	// catalog or a classifier regression — surfaced prominently, never
	// silently bucketed.
	KindUnknown = "unknown"

	// Shadow model calls — separate API requests that never appear in
	// the harness transcript.
	KindCheckStage1 = "offshoot:permission-check:stage1"
	KindCheckStage2 = "offshoot:permission-check:stage2"
	KindTitleGen    = "offshoot:title-gen"
	KindPlanNameGen = "offshoot:plan-name-gen"
	KindSuggestion  = "offshoot:suggestion"
	KindWebSummary  = "offshoot:web-summary"
	KindProbe       = "offshoot:probe"
	KindCompaction  = "offshoot:compaction"

	// Injected context — whole messages the harness prepends inside
	// otherwise-normal calls. They drift between turns (server lists
	// change, modes toggle), so they are kept OFF the main chain as
	// side-branch nodes (see TurnChain) and marked with these kinds.
	KindInjectedMCPInstructions = "injected:mcp-instructions"
	KindInjectedSkillsList      = "injected:skills-list"
	KindInjectedModeBanner      = "injected:mode-banner"
	// KindInjectedClaudeMD is the user-context blob the harness
	// prepends to its security-monitor checks (<user_claude_md>…).
	// Every check in a session shares it byte-for-byte, so left on the
	// chain it fuses all checks into one fan rooted at the blob.
	KindInjectedClaudeMD = "injected:claude-md"

	// KindInjectedSystemInsert marks mid-spine system-role inserts
	// (task reminders, CLAUDE.md re-injections, post-compaction
	// replays). Currently minted only by the span emit stage — the
	// node classifier still types these "main" because they ride
	// inside main calls; a dedicated classifier kind is a candidate
	// follow-up.
	KindInjectedSystemInsert = "injected:system-insert"
)

// ClassifyCall determines the kind of a captured API call from its
// parsed request and reduced response. Request-envelope parameters
// (system prompt, max_tokens, tool count, stream) are the definitive
// discriminators; content shape is the fallback. Tells are grounded in
// observed traffic — see the design doc §2g and the golden-session
// measurements.
func ClassifyCall(req *llm.ChatRequest, resp *llm.ChatResponse) string {
	if req == nil {
		return KindUnknown
	}

	system := strings.ToLower(req.System)
	toolCount := len(req.Tools)
	streaming := req.Stream != nil && *req.Stream

	// Security monitor: the canonical shadow call. System prompt is
	// the definitive tell; the stage is distinguished by the trailing
	// instruction (stage 1 is the block-biased fast path at
	// max_tokens≈64, stage 2 the reasoned reviewer with room to
	// think).
	if strings.Contains(system, "you are a security monitor") {
		if req.MaxTokens != nil && *req.MaxTokens <= 64 {
			return KindCheckStage1
		}
		if strings.Contains(strings.ToLower(lastText(req)), "err on the side of blocking") {
			return KindCheckStage1
		}
		return KindCheckStage2
	}

	// Connectivity probe: max_tokens=1, no tools, minimal body.
	if req.MaxTokens != nil && *req.MaxTokens == 1 && toolCount == 0 {
		return KindProbe
	}

	// Typeahead suggestion: parameters look exactly like a main call
	// (full tool set, streaming), so the [SUGGESTION MODE…] marker at
	// the START of the last message is the only discriminator. The
	// prefix match keeps a main turn that merely QUOTES the marker
	// (e.g. grepping harness internals) from misclassifying.
	if strings.HasPrefix(strings.TrimSpace(lastText(req)), "[SUGGESTION MODE") {
		return KindSuggestion
	}

	// Title / plan-name generation: tool-less calls whose system
	// prompt carries the JSON output contract.
	if toolCount == 0 {
		if strings.Contains(system, `{"title"`) {
			return KindTitleGen
		}
		if strings.Contains(system, "<conversation>") || strings.HasPrefix(strings.TrimSpace(firstText(req)), "<conversation>") {
			return KindPlanNameGen
		}
	}
	if r := strings.TrimSpace(responseText(resp)); strings.HasPrefix(r, `{"title"`) {
		return KindTitleGen
	} else if strings.HasPrefix(r, `{"name"`) {
		return KindPlanNameGen
	}

	// Web content summarization: the request opens with the fetched
	// page or the search instruction.
	first := strings.TrimSpace(firstText(req))
	if strings.HasPrefix(first, "Web page content:") || strings.HasPrefix(first, "Perform a web search") {
		return KindWebSummary
	}

	// Context compaction: the harness sends the full conversation plus
	// a summarize instruction as the final user message. The call SHAPE
	// is not a tell — newer harnesses (cc 2.1.x) send it streaming with
	// the full tool set, exactly like a main turn — only the instruction
	// text and the structured-summary response are.
	//
	// The request-side instruction is the strongest tell. The response
	// side is a fallback (some captures don't surface the instruction
	// message cleanly), but the bare "Primary Request and Intent"
	// substring is too loose: a normal turn that merely QUOTES the
	// header — e.g. a subagent that read this very file (#27) — would
	// trip it. So the response tell requires the FULL structured summary
	// (the header plus at least one more section), which prose quoting a
	// single header never carries.
	{
		lt := strings.ToLower(lastText(req))
		if strings.Contains(lt, "summary of the conversation so far") ||
			strings.Contains(lt, "context checkpoint compaction") ||
			strings.Contains(lt, "create a handoff summary for another llm") ||
			isCompactionSummary(responseText(resp)) {
			return KindCompaction
		}
	}

	// The conversation spine: streaming with the full tool set.
	if streaming && toolCount > 0 {
		return KindMain
	}

	return KindUnknown
}

// ClassifyInjected reports the injected-context kind for one request
// message, or "" when the message is ordinary conversation. Only whole
// messages that ARE an injected block qualify — a user turn that
// merely mentions skills is untouched. These messages drift between
// turns of the same conversation (an MCP server connects, a mode
// toggles), so TurnChain keeps them off the hashed spine.
func ClassifyInjected(msg llm.Message) string {
	if msg.Role != "user" && msg.Role != "system" {
		return ""
	}
	var text strings.Builder
	for _, b := range msg.Content {
		switch b.Type {
		case "text", "":
			text.WriteString(b.Text)
		default:
			// tool_use / tool_result / image blocks are never injected
			// context; a mixed message is conversation.
			return ""
		}
	}
	t := strings.TrimSpace(text.String())
	switch {
	case strings.HasPrefix(t, "# MCP Server Instructions"):
		return KindInjectedMCPInstructions
	case strings.HasPrefix(t, "The following skills are available"):
		return KindInjectedSkillsList
	case strings.HasPrefix(t, "Plan mode is active"),
		strings.HasPrefix(t, "Exited Plan Mode"),
		strings.HasPrefix(t, "## Exited Plan Mode"),
		strings.HasPrefix(t, "## Exit Plan Mode"),
		strings.HasPrefix(t, "## Plan Mode"),
		strings.HasPrefix(t, "[SYSTEM NOTIFICATION"):
		return KindInjectedModeBanner
	case strings.HasPrefix(t, "<user_claude_md>"):
		return KindInjectedClaudeMD
	}
	return ""
}

// compactionSummaryHeader is the lead section of the Claude Code
// compaction summary template; its presence is necessary but, on its
// own, not sufficient (prose can quote it — see #27).
const compactionSummaryHeader = "Primary Request and Intent"

// compactionSummarySections are the remaining canonical section
// headers of the compaction summary. A genuine summary carries several
// of them under the lead header; a turn that merely mentions the lead
// header in prose carries none.
var compactionSummarySections = []string{
	"Key Technical Concepts",
	"Files and Code Sections",
	"Pending Tasks",
	"Current Work",
	"Errors and fixes",
	"Problem Solving",
	"All user messages",
	"Optional Next Step",
}

// isCompactionSummary reports whether a response carries the real
// Claude Code compaction summary structure: the lead header plus at
// least one further canonical section. Quoting the lead header alone
// (as a subagent reading classify.go did) is not enough.
func isCompactionSummary(resp string) bool {
	if !strings.Contains(resp, compactionSummaryHeader) {
		return false
	}
	for _, h := range compactionSummarySections {
		if strings.Contains(resp, h) {
			return true
		}
	}
	return false
}

// firstText returns the text of the first request message.
func firstText(req *llm.ChatRequest) string {
	if req == nil || len(req.Messages) == 0 {
		return ""
	}
	return messageText(req.Messages[0])
}

// lastText returns the text of the last request message — for shadow
// calls this is the half that carries the distinctive instruction.
func lastText(req *llm.ChatRequest) string {
	if req == nil || len(req.Messages) == 0 {
		return ""
	}
	return messageText(req.Messages[len(req.Messages)-1])
}

func messageText(m llm.Message) string {
	var sb strings.Builder
	for _, b := range m.Content {
		if b.Text != "" {
			sb.WriteString(b.Text)
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func responseText(resp *llm.ChatResponse) string {
	if resp == nil {
		return ""
	}
	return messageText(resp.Message)
}
