package derive

import (
	"regexp"
	"strings"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// Verdict is the security monitor's disposition on a permission-check
// span, extracted at derive time from the check response. It is a typed
// derived field persisted on the span (spans.verdict), not read-time
// text parsing.
type Verdict struct {
	Disposition string `json:"disposition"` // ALLOW | BLOCK
	Stage       int    `json:"stage"`
	Reasoned    bool   `json:"reasoned"`
}

// blockVerdictPattern matches the security monitor's <block>yes/no
// decision in the check response.
var blockVerdictPattern = regexp.MustCompile(`(?i)<block>\s*(yes|no)`)

// ClassifyVerdict extracts the security-monitor disposition from a
// permission-check span's output blocks. It returns nil for any span
// that is not a permission check, or for a check that carries no
// <block> marker. This is the (wire, claude-code) adapter for verdicts,
// mirroring ClassifyCall: harness-specific tells stay here, the wire
// type is harness-neutral.
func ClassifyVerdict(callKind string, blocks []llm.ContentBlock) *Verdict {
	if !strings.HasPrefix(callKind, "offshoot:permission-check") {
		return nil
	}

	var text strings.Builder
	for _, b := range blocks {
		if b.Text != "" {
			text.WriteString(b.Text)
		}
	}
	m := blockVerdictPattern.FindStringSubmatch(text.String())
	if m == nil {
		return nil
	}

	v := &Verdict{Disposition: "ALLOW", Stage: 1}
	if strings.EqualFold(m[1], "yes") {
		v.Disposition = "BLOCK"
	}
	if strings.HasSuffix(callKind, "stage2") {
		v.Stage = 2
	}
	if strings.Contains(text.String(), "<thinking>") {
		v.Reasoned = true
	}
	return v
}
