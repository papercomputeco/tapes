package derive

import (
	"regexp"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// The security monitor's check request carries no tool_use_id — the
// judged action arrives as rendered text inside <transcript>…</transcript>.
// The join back to the concrete tool_use is therefore content-based and
// one-to-one: extract the judged action, group the stage-1/stage-2
// checks that share it, and consume the matching tool_use exactly once.
// The monitor judges SUBAGENT actions too, so the candidate pool is
// every tool_use across every captured call, not just the main thread.

// toolHeadPattern matches a rendered action line that begins with a
// tool name — the shape the harness uses for the action entry in the
// check transcript. Multi-line tool bodies (Bash scripts, Write
// payloads) continue after the header line.
var toolHeadPattern = regexp.MustCompile(`^(Bash|Read|Write|Edit|MultiEdit|NotebookEdit|Glob|Grep|WebSearch|WebFetch|Task|TaskCreate|TaskUpdate|TaskGet|Skill|CronCreate|CronList|CronDelete|Monitor|EnterPlanMode|ExitPlanMode|AskUserQuestion|PushNotification|mcp__\S+)\b`)

// toolUseRef is one candidate tool_use a verdict or summary can attach to.
type toolUseRef struct {
	id       string
	name     string
	rendered string // normalized name+input body for content matching
	webTool  bool   // WebFetch / WebSearch
	atTurn   int    // turn index of first capture
	consumed bool
}

// attachVerdicts links every permission-check call to the tool_use it
// judged by stamping ParentToolUseID on the check's nodes. Stage-1 and
// stage-2 checks for the same action share one attachment.
func attachVerdicts(turns []*attachTurn, candidates []*toolUseRef, report *RederiveReport) {
	type group struct {
		action string
		turns  []*attachTurn
	}
	groups := map[string]*group{}
	var order []*group
	for _, t := range turns {
		if t.kind != KindCheckStage1 && t.kind != KindCheckStage2 {
			continue
		}
		if t.judgedAction == "" {
			continue
		}
		g, ok := groups[t.judgedAction]
		if !ok {
			g = &group{action: t.judgedAction}
			groups[t.judgedAction] = g
			order = append(order, g)
		}
		g.turns = append(g.turns, t)
	}

	report.JudgedActions = len(order)
	for _, g := range order {
		ref := matchToolUse(candidates, g.action)
		if ref == nil {
			continue
		}
		ref.consumed = true
		report.AttachedVerdicts++
		for _, t := range g.turns {
			for _, dn := range t.nodes {
				// Stamp only this check's own nodes — a deduped node
				// first captured by another call keeps its own edge.
				if dn.Node.Kind == t.kind {
					dn.Node.ParentToolUseID = ref.id
				}
			}
		}
	}
}

// attachWebSummaries links web-summary calls to the most recent
// preceding unconsumed WebFetch/WebSearch tool_use. The summary request
// carries no id and no echo of the tool input, so recency is the
// strongest available signal; with N concurrent fetches this can
// misattribute within the burst, which is acceptable for a foldable
// annotation.
func attachWebSummaries(turns []*attachTurn, candidates []*toolUseRef, report *RederiveReport) {
	for _, t := range turns {
		if t.kind != KindWebSummary {
			continue
		}
		var best *toolUseRef
		for _, u := range candidates {
			if !u.webTool || u.consumed || u.atTurn > t.index {
				continue
			}
			if best == nil || u.atTurn >= best.atTurn {
				best = u
			}
		}
		if best == nil {
			continue
		}
		best.consumed = true
		report.WebSummaryAttached++
		for _, dn := range t.nodes {
			if dn.Node.Kind == t.kind {
				dn.Node.ParentToolUseID = best.id
			}
		}
	}
}

// judgedAction extracts the rendered action a check call is judging:
// the last transcript entry. In the observed capture shape the check's
// final message carries the transcript as individual text blocks —
// `<transcript>`, one block per entry, `</transcript>`, then the stage
// instruction — so the action is the block immediately before the
// closing tag. A single-blob transcript falls back to line scanning
// with the tool-header heuristic.
func judgedAction(req *llm.ChatRequest) string {
	if req == nil || len(req.Messages) == 0 {
		return ""
	}
	blocks := req.Messages[len(req.Messages)-1].Content

	// Block-structured transcript: action = block before </transcript>.
	for i := len(blocks) - 1; i > 0; i-- {
		if strings.TrimSpace(blocks[i].Text) == "</transcript>" {
			return normalizeAction(blocks[i-1].Text)
		}
	}

	// Fallback: single-blob transcript. Take the last line inside
	// <transcript> that begins with a tool header (multi-line actions
	// otherwise yield a content fragment, not the header).
	var body string
	for _, b := range blocks {
		if i := strings.Index(b.Text, "<transcript>"); i >= 0 {
			body = b.Text[i+len("<transcript>"):]
			if j := strings.Index(body, "</transcript>"); j >= 0 {
				body = body[:j]
			}
		}
	}
	lines := strings.Split(body, "\n")
	for i := len(lines) - 1; i >= 0 && i >= len(lines)-10; i-- {
		line := strings.TrimSpace(lines[i])
		if toolHeadPattern.MatchString(line) {
			return normalizeAction(line)
		}
	}
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); line != "" {
			return normalizeAction(line)
		}
	}
	return ""
}

// matchToolUse finds the first unconsumed candidate whose rendered form
// matches the judged action: MCP tools by full tool-name prefix,
// everything else by body-substring overlap in either direction.
func matchToolUse(candidates []*toolUseRef, action string) *toolUseRef {
	actionNorm := normalizeAction(action)
	actionBody := stripToolHead(actionNorm)
	isMCP := strings.HasPrefix(actionNorm, "mcp__")

	for _, ref := range candidates {
		if ref.consumed {
			continue
		}
		if isMCP {
			refName := strings.ToLower(ref.name)
			if strings.HasPrefix(refName, "mcp__") && strings.HasPrefix(actionNorm, refName) {
				return ref
			}
			continue
		}
		refBody := stripToolHead(ref.rendered)
		if len(actionBody) < 6 || len(refBody) < 6 {
			continue
		}
		probe := actionBody
		if len(probe) > 22 {
			probe = probe[:22]
		}
		refProbe := refBody
		if len(refProbe) > 22 {
			refProbe = refProbe[:22]
		}
		if strings.Contains(refBody, probe) || strings.Contains(actionBody, refProbe) {
			return ref
		}
	}
	return nil
}

// renderToolUse approximates the harness's rendered action entry for a
// tool_use block: the tool name plus the most distinctive input value.
func renderToolUse(name string, input map[string]any) string {
	detail := ""
	for _, key := range []string{"command", "query", "file_path", "path", "pattern", "url", "prompt", "skill", "message", "subject"} {
		if v, ok := input[key].(string); ok && v != "" {
			detail = v
			break
		}
	}
	return normalizeAction(name + " " + detail)
}

// normalizeAction lower-cases and collapses an action rendering so the
// check transcript's form and the tool_use's form compare cleanly.
func normalizeAction(s string) string {
	s = strings.ToLower(s)
	s = strings.TrimPrefix(strings.TrimSpace(s), "$ ")
	s = strings.NewReplacer("$", "", `"`, "").Replace(s)
	return strings.Join(strings.Fields(s), " ")
}

// toolHeads is the lowercase form of toolHeadPattern's alternatives,
// for matching against already-normalized action text.
var toolHeads = map[string]struct{}{
	"bash": {}, "read": {}, "write": {}, "edit": {}, "multiedit": {},
	"notebookedit": {}, "glob": {}, "grep": {}, "websearch": {}, "webfetch": {},
	"task": {}, "taskcreate": {}, "taskupdate": {}, "taskget": {}, "skill": {},
	"croncreate": {}, "cronlist": {}, "crondelete": {}, "monitor": {},
	"enterplanmode": {}, "exitplanmode": {}, "askuserquestion": {},
	"pushnotification": {},
}

// stripToolHead drops the leading tool-name token so substring matching
// compares command bodies, not headers. Expects normalized (lowercase)
// input.
func stripToolHead(s string) string {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return s
	}
	head := fields[0]
	if _, ok := toolHeads[head]; ok || strings.HasPrefix(head, "mcp__") {
		return strings.Join(fields[1:], " ")
	}
	return s
}
