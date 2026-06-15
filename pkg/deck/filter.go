package deck

import (
	"sort"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

// matchesFilters evaluates the deck's client-side filter set against one
// session row. Time bounds are also pushed down to /v1/sessions; rechecking
// them here keeps a cached page honest after the window changes. Model,
// status, and project only exist client-side: the sessions list endpoint
// does not filter on them.
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
