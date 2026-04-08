package sessions

import (
	"strings"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

// DetermineStatus classifies a session based on its terminal (leaf) node and
// a pair of flags derived from scanning the full ancestry.
//
// Precedence:
//  1. Any tool_result error in the chain → StatusFailed
//  2. Any git commit/push in the chain → StatusCompleted (strong done signal)
//  3. Non-assistant leaf → StatusAbandoned
//  4. Assistant leaf with a known-terminal stop_reason → StatusCompleted
//  5. Assistant leaf with a known-failing stop_reason → StatusFailed
//  6. Empty or otherwise unrecognised stop_reason → StatusUnknown
func DetermineStatus(leaf *merkle.Node, hasToolError, hasGitActivity bool) string {
	if leaf == nil {
		return StatusUnknown
	}
	if hasToolError {
		return StatusFailed
	}
	if hasGitActivity {
		return StatusCompleted
	}

	role := strings.ToLower(leaf.Bucket.Role)
	if role != roleAssistant {
		return StatusAbandoned
	}

	reason := strings.ToLower(strings.TrimSpace(leaf.StopReason))
	switch reason {
	case "stop", "end_turn", "end-turn", "eos":
		return StatusCompleted
	case "length", "max_tokens", "content_filter", "tool_use", "tool_use_response":
		return StatusFailed
	case "":
		return StatusUnknown
	default:
		if strings.Contains(reason, "error") {
			return StatusFailed
		}
	}
	return StatusUnknown
}
