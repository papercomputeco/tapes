package deck

import (
	"strings"

	"github.com/papercomputeco/tapes/pkg/storage/ent"
)

func determineStatus(leaf *ent.Node, hasToolError, hasGitActivity bool) string {
	if hasToolError {
		return StatusFailed
	}

	// Git commits/pushes are a strong signal the session achieved its goal,
	// regardless of who sent the last message or the stop reason.
	if hasGitActivity {
		return StatusCompleted
	}

	role := strings.ToLower(leaf.Role)
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
