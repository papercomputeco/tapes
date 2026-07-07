package sessions

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

// Outcome kinds. An open set on the wire — consumers render unknown
// kinds as text — but constants here so matchers and tests share one
// spelling.
const (
	OutcomeKindPullRequest = "pull_request"
	OutcomeKindRepo        = "repo"
	OutcomeKindIssue       = "issue"
	OutcomeKindLinearIssue = "linear_issue"
)

// DetectedBy values: which matcher family surfaced the outcome.
const (
	OutcomeDetectedByGhCli = "gh_cli"
	OutcomeDetectedByMCP   = "mcp"
)

// Outcome is one artifact a session produced (a pull request, a repo,
// an issue), detected from the session's tool calls at derive time. URL
// is the outcome's identity — the fold dedupes on it. TraceID/SpanID
// point back at the detecting tool span (the simulacrum memory stream's
// source-pointer convention), and DetectedAt carries that span's start
// time — never the wall clock — so a re-derive reproduces the fold
// byte-for-byte.
type Outcome struct {
	Kind       string    `json:"kind"`
	URL        string    `json:"url"`
	Title      string    `json:"title,omitempty"`
	Repo       string    `json:"repo,omitempty"`
	TraceID    string    `json:"trace_id,omitempty"`
	SpanID     string    `json:"span_id,omitempty"`
	DetectedBy string    `json:"detected_by,omitempty"`
	DetectedAt time.Time `json:"detected_at,omitzero"`
}

// URL shapes for the artifacts the gh matchers extract. Ordered most
// specific first where one output could match several (a PR URL also
// matches the bare repo shape).
var (
	githubPullURL  = regexp.MustCompile(`https://github\.com/([\w.-]+/[\w.-]+)/pull/\d+`)
	githubIssueURL = regexp.MustCompile(`https://github\.com/([\w.-]+/[\w.-]+)/issues/\d+`)
	githubRepoURL  = regexp.MustCompile(`https://github\.com/([\w.-]+/[\w.-]+)`)
	linearIssueURL = regexp.MustCompile(`https://linear\.app/([\w-]+)/issue/[\w-]+(?:/[\w-]+)?`)
)

// ghOutcomeMatcher pairs a gh CLI invocation with the URL shape its
// stdout prints on success. The command substring gates the match; the
// URL in the tool output is the outcome's identity — a create whose
// output carries no URL (errored, interrupted, dry-run) yields nothing.
type ghOutcomeMatcher struct {
	command string
	kind    string
	url     *regexp.Regexp
}

// ghOutcomeMatchers is the Bash/gh matcher family. Adding an outcome
// kind detected from a shell command is one row here.
var ghOutcomeMatchers = []ghOutcomeMatcher{
	{command: "gh pr create", kind: OutcomeKindPullRequest, url: githubPullURL},
	{command: "gh repo create", kind: OutcomeKindRepo, url: githubRepoURL},
	{command: "gh issue create", kind: OutcomeKindIssue, url: githubIssueURL},
}

// linearIssueToolNames gates the MCP matcher family: Linear MCP tool
// invocations that create (or update) an issue. Matched as lowercase
// substrings of the tool name so harness-specific prefixes
// (mcp__linear-server__save_issue, mcp__claude_ai_Linear__create_issue)
// all hit.
var linearIssueToolNames = []string{"save_issue", "create_issue"}

// DetectToolOutcomes inspects one completed tool call — its name, its
// tool_use input, and its tool_result output text — and returns the
// outcomes it produced. Pure; the derive fold stamps trace/span
// provenance and timestamps onto the results. Conservative by design:
// no URL in the output means no outcome, so failed creates never count.
func DetectToolOutcomes(toolName string, input map[string]any, output string) []Outcome {
	if output == "" {
		return nil
	}
	if toolName == "Bash" {
		return detectGhOutcomes(input, output)
	}
	if isLinearIssueTool(toolName) {
		return detectLinearIssueOutcome(output)
	}
	return nil
}

// detectGhOutcomes runs the Bash/gh matcher family over one shell tool
// call. Each matcher fires at most once per call: a `gh pr create`
// prints exactly one PR URL, and dedup-by-URL upstream makes repeats
// harmless anyway.
func detectGhOutcomes(input map[string]any, output string) []Outcome {
	cmd, _ := input["command"].(string)
	if cmd == "" {
		return nil
	}
	lower := strings.ToLower(cmd)
	var outcomes []Outcome
	for _, m := range ghOutcomeMatchers {
		if !strings.Contains(lower, m.command) {
			continue
		}
		match := m.url.FindStringSubmatch(output)
		if match == nil {
			continue
		}
		outcomes = append(outcomes, Outcome{
			Kind:       m.kind,
			URL:        match[0],
			Repo:       match[1],
			DetectedBy: OutcomeDetectedByGhCli,
		})
	}
	return outcomes
}

func isLinearIssueTool(toolName string) bool {
	lower := strings.ToLower(toolName)
	if !strings.Contains(lower, "linear") {
		return false
	}
	for _, name := range linearIssueToolNames {
		if strings.Contains(lower, name) {
			return true
		}
	}
	return false
}

// linearIssuePayload is the slice of a Linear MCP issue-create result
// the matcher reads. The result is JSON text; unknown fields are
// ignored, and a non-JSON result falls back to a bare URL scan.
type linearIssuePayload struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}

// detectLinearIssueOutcome extracts the created issue's canonical URL
// (and title when the payload parses) from a Linear MCP tool result.
func detectLinearIssueOutcome(output string) []Outcome {
	outcome := Outcome{
		Kind:       OutcomeKindLinearIssue,
		DetectedBy: OutcomeDetectedByMCP,
	}
	var payload linearIssuePayload
	if err := json.Unmarshal([]byte(output), &payload); err == nil && payload.URL != "" {
		if linearIssueURL.MatchString(payload.URL) {
			outcome.URL = payload.URL
			outcome.Title = payload.Title
		}
	}
	if outcome.URL == "" {
		outcome.URL = linearIssueURL.FindString(output)
	}
	if outcome.URL == "" {
		return nil
	}
	if match := linearIssueURL.FindStringSubmatch(outcome.URL); match != nil {
		// The workspace slug is the closest analogue to owner/name.
		outcome.Repo = match[1]
	}
	return []Outcome{outcome}
}

// DedupeOutcomes drops repeat detections of the same artifact, keyed by
// URL, keeping the first occurrence (capture order — the detection
// closest to the creating call). Re-running a create command or
// re-printing a URL never double-counts an outcome.
func DedupeOutcomes(outcomes []Outcome) []Outcome {
	if len(outcomes) < 2 {
		return outcomes
	}
	seen := make(map[string]struct{}, len(outcomes))
	deduped := make([]Outcome, 0, len(outcomes))
	for _, o := range outcomes {
		if _, dup := seen[o.URL]; dup {
			continue
		}
		seen[o.URL] = struct{}{}
		deduped = append(deduped, o)
	}
	return deduped
}
