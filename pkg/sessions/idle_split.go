package sessions

import (
	"time"
)

// DefaultIdleGap is the idle gap after which a synthetic (root-derived)
// session is split. Thirty minutes is long enough that a person pausing
// mid-task stays in one session and short enough that a long-running bot
// replying to one ever-growing conversation gets a session per burst of
// activity rather than one session forever.
const DefaultIdleGap = 30 * time.Minute

// syntheticSplitIDLayout renders a split's start instant into its id:
// UTC, second resolution, no separators that would need escaping in a
// URL or a LIKE pattern.
const syntheticSplitIDLayout = "20060102T150405Z"

// SyntheticSplit is one idle-split segment of a synthetic conversation: the
// harness_session_id its turns are keyed by and the capture-time range of
// the turns assigned to it so far.
type SyntheticSplit struct {
	HarnessSessionID string
	FirstCapturedAt  time.Time
	LastCapturedAt   time.Time
}

// SyntheticSplitID is the harness_session_id of a split of conversation
// that opens at start: the conversation key (the root-hash prefix
// SyntheticHarnessSessionID derives), a dash, and the start instant in UTC
// at second resolution. Deterministic in its inputs, so re-assigning the
// same turn produces the same id.
//
// The first split of a conversation is keyed by the bare conversation key,
// not by this form, so a conversation that never idles out keeps the id it
// always had.
func SyntheticSplitID(conversation string, start time.Time) string {
	return conversation + "-" + start.UTC().Format(syntheticSplitIDLayout)
}

// AssignSyntheticSplit picks the split a synthetic turn captured at `at`
// belongs to, given the conversation's existing splits. It returns the
// split the turn joins, with its range widened to cover `at`, and whether
// that split is new.
//
// The rule compares capture timestamps, never arrival order:
//
//  1. A split whose range already contains `at` takes the turn (a retry,
//     or a backfilled turn that falls inside a known burst).
//  2. Otherwise the nearest split that ENDS at most gap before `at` takes
//     it — the conversation continued within the idle gap.
//  3. Otherwise the nearest split that STARTS at most gap after `at` takes
//     it — a late-arriving turn from just before a known burst.
//  4. Otherwise the turn opens a new split. The conversation's first split
//     is keyed by the bare conversation key; every later one by
//     SyntheticSplitID(conversation, at).
//
// Predecessor before successor makes the one order-dependent case
// deterministic: a late turn that falls within gap of two splits joins the
// earlier one. The two splits are not merged, because their turns are
// already stamped; in arrival order they would have been one split.
//
// gap <= 0 disables splitting: every turn joins the conversation key.
// splits must be every split the conversation has (rule 4 needs to know
// whether any exists); their order does not matter.
func AssignSyntheticSplit(conversation string, splits []SyntheticSplit, at time.Time, gap time.Duration) (SyntheticSplit, bool) {
	if gap <= 0 {
		for _, s := range splits {
			if s.HarnessSessionID == conversation {
				return widen(s, at), false
			}
		}
		return SyntheticSplit{HarnessSessionID: conversation, FirstCapturedAt: at, LastCapturedAt: at}, true
	}

	var pred, succ *SyntheticSplit
	for i := range splits {
		s := &splits[i]
		switch {
		case !at.Before(s.FirstCapturedAt) && !at.After(s.LastCapturedAt):
			return widen(*s, at), false
		case s.LastCapturedAt.Before(at) && at.Sub(s.LastCapturedAt) <= gap:
			if pred == nil || s.LastCapturedAt.After(pred.LastCapturedAt) {
				pred = s
			}
		case s.FirstCapturedAt.After(at) && s.FirstCapturedAt.Sub(at) <= gap:
			if succ == nil || s.FirstCapturedAt.Before(succ.FirstCapturedAt) {
				succ = s
			}
		}
	}
	if pred != nil {
		return widen(*pred, at), false
	}
	if succ != nil {
		return widen(*succ, at), false
	}

	id := conversation
	if len(splits) > 0 {
		id = SyntheticSplitID(conversation, at)
	}
	return SyntheticSplit{HarnessSessionID: id, FirstCapturedAt: at, LastCapturedAt: at}, true
}

func widen(s SyntheticSplit, at time.Time) SyntheticSplit {
	if at.Before(s.FirstCapturedAt) {
		s.FirstCapturedAt = at
	}
	if at.After(s.LastCapturedAt) {
		s.LastCapturedAt = at
	}
	return s
}
