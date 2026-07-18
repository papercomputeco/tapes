package derive

import (
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

// SessionStatus is the deriver's chain-aware session outcome plus the
// signals that produced it. It was computed at ingest from the re-sent
// conversation nodes; the deriver now folds it from the delta-span
// projection so status is a pure, re-derivable output (Goals 1 & 5), and
// ingest touches session identity only.
type SessionStatus struct {
	DerivedStatus   string
	HasGitActivity  bool
	ToolResultCount int
	ToolErrorCount  int
}

// FoldSessionStatus reproduces the ingest-time status computation from a
// session's spans: git activity and tool result/error tallies over the
// session's tool spans (all threads, matching the ingest count over the
// re-sent conversation), and sessions.DetermineStatus over the session's
// terminal main-spine response.
//
// A tool span carries its tool_result once its Output is filled, and its
// Status is "error" for an error result — so counting spans is the
// delta-view equivalent of the ingest count over tool_result blocks,
// without the re-sent-history double-count the node path had to dedup.
func FoldSessionStatus(tools []*Span, terminal *Span) SessionStatus {
	st := SessionStatus{}
	for _, ts := range tools {
		if len(ts.Output) > 0 {
			st.ToolResultCount++
		}
		if ts.Status == "error" {
			st.ToolErrorCount++
		}
		if !st.HasGitActivity && sessions.BlocksHaveGitActivity(ts.Input) {
			st.HasGitActivity = true
		}
	}

	// The terminal span is the closing main-spine llm response — always an
	// assistant turn by construction; DetermineStatus reads only role,
	// stop_reason, and content, so a throwaway node (never hashed) suffices.
	var leaf *merkle.Node
	if terminal != nil {
		leaf = &merkle.Node{
			StopReason: terminal.StopReason,
			Bucket:     merkle.Bucket{Role: "assistant", Content: terminal.Output},
		}
	}
	st.DerivedStatus = sessions.DetermineStatus(leaf, st.HasGitActivity, st.ToolResultCount, st.ToolErrorCount)
	return st
}
