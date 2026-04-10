package deck

import (
	"sort"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

// preFilterCandidatesByTime reduces the candidate set using time-based filters
// before the O(N log N) grouping step. This is the hot path when switching
// between 24h and 30d periods — avoiding a full sort of all candidates.
func preFilterCandidatesByTime(candidates []sessionCandidate, filters Filters) []sessionCandidate {
	var cutoff time.Time
	hasCutoff := false

	if filters.Since > 0 {
		cutoff = time.Now().Add(-filters.Since)
		hasCutoff = true
	}
	if filters.From != nil && (!hasCutoff || filters.From.After(cutoff)) {
		cutoff = *filters.From
		hasCutoff = true
	}

	if !hasCutoff && filters.To == nil {
		return candidates
	}

	filtered := make([]sessionCandidate, 0, len(candidates))
	for _, c := range candidates {
		if hasCutoff && c.summary.EndTime.Before(cutoff) {
			continue
		}
		if filters.To != nil && c.summary.StartTime.After(*filters.To) {
			continue
		}
		filtered = append(filtered, c)
	}
	return filtered
}

func matchesFilters(summary SessionSummary, filters Filters) bool {
	if filters.Model != "" {
		if sessions.NormalizeModel(summary.Model) != sessions.NormalizeModel(filters.Model) {
			return false
		}
	}
	if filters.Status != "" && summary.Status != filters.Status {
		return false
	}
	if filters.Project != "" && summary.Project != filters.Project {
		return false
	}
	if filters.From != nil && summary.EndTime.Before(*filters.From) {
		return false
	}
	if filters.To != nil && summary.StartTime.After(*filters.To) {
		return false
	}
	if filters.Since > 0 {
		cutoff := time.Now().Add(-filters.Since)
		if summary.EndTime.Before(cutoff) {
			return false
		}
	}
	return true
}

// SortSessions sorts session summaries in place by the given key and direction.
func SortSessions(sessions []SessionSummary, sortKey, sortDir string) {
	ascending := strings.EqualFold(sortDir, "asc")
	switch sortKey {
	case "date":
		sort.Slice(sessions, func(i, j int) bool {
			if ascending {
				return sessions[i].StartTime.Before(sessions[j].StartTime)
			}
			return sessions[i].StartTime.After(sessions[j].StartTime)
		})
	case "tokens":
		sort.Slice(sessions, func(i, j int) bool {
			left := sessions[i].InputTokens + sessions[i].OutputTokens
			right := sessions[j].InputTokens + sessions[j].OutputTokens
			if ascending {
				return left < right
			}
			return left > right
		})
	case "duration":
		sort.Slice(sessions, func(i, j int) bool {
			if ascending {
				return sessions[i].Duration < sessions[j].Duration
			}
			return sessions[i].Duration > sessions[j].Duration
		})
	default:
		sort.Slice(sessions, func(i, j int) bool {
			if ascending {
				return sessions[i].TotalCost < sessions[j].TotalCost
			}
			return sessions[i].TotalCost > sessions[j].TotalCost
		})
	}
}
