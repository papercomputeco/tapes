package sessions

import (
	"strings"

	"github.com/papercomputeco/tapes/pkg/merkle"
)

// DetermineStatus classifies a session from its terminal (leaf) node, a
// git-activity flag, and the session's tool_result success/failure counts.
//
// A single mid-conversation tool error is normal agentic behaviour (a failed
// grep, a missing file) that the model routinely recovers from, so it no
// longer marks the whole session failed. "Failed" now means the session
// either ended broken or was dominated by errors.
//
// Precedence:
//  1. Unrecovered terminal error → StatusFailed. The session ended on an error
//     the model never came back from: a non-assistant leaf carrying a
//     tool_result error (no assistant turn followed it), or an assistant leaf
//     with a known-failing stop_reason (length / max_tokens / content_filter /
//     *error*). An assistant turn after an error means the model saw it and
//     responded — recovered, not failed.
//  2. Git commit/push anywhere → StatusCompleted. Shipped work and ended clean
//     (rule 1 already ruled out a broken ending); outranks the error-rate
//     check so a noisy-but-recovered session that shipped reads as completed.
//  3. tool_error_count / tool_result_count > 1/2 → StatusFailed. The session
//     limped along mostly erroring.
//  4. Assistant leaf with a known-terminal stop_reason → StatusCompleted.
//     `tool_use` / `tool_use_response` count as terminal (designed terminus of
//     a subagent dispatch / parallel-tool side-conversation — see PCC-560).
//  5. Non-assistant leaf (no terminal error) → StatusAbandoned.
//  6. Empty or otherwise unrecognised stop_reason → StatusUnknown.
func DetermineStatus(leaf *merkle.Node, hasGitActivity bool, toolResultCount, toolErrorCount int) string {
	if leaf == nil {
		return StatusUnknown
	}

	role := strings.ToLower(leaf.Bucket.Role)
	reason := strings.ToLower(strings.TrimSpace(leaf.StopReason))
	failingStop := reason == "length" || reason == "max_tokens" || reason == "content_filter" || strings.Contains(reason, "error")

	// 1. Unrecovered terminal error — ended broken.
	if role != roleAssistant && BlocksHaveToolError(leaf.Bucket.Content) {
		return StatusFailed
	}
	if role == roleAssistant && failingStop {
		return StatusFailed
	}

	// 2. Shipped work and ended clean.
	if hasGitActivity {
		return StatusCompleted
	}

	// 3. Mostly errors across the session.
	if toolResultCount > 0 && toolErrorCount*2 > toolResultCount {
		return StatusFailed
	}

	// 4. Assistant leaf with a terminal stop_reason.
	if role == roleAssistant {
		switch reason {
		case "stop", "end_turn", "end-turn", "eos", "tool_use", "tool_use_response":
			return StatusCompleted
		default:
			return StatusUnknown
		}
	}

	// 5. Non-assistant leaf, no terminal error → abandoned.
	return StatusAbandoned
}
