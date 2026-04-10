package deck

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/papercomputeco/tapes/pkg/sessions"
)

const (
	groupIDPrefix = "group:"
	groupWindow   = time.Hour
)

type sessionGroup struct {
	summary      SessionSummary
	modelCosts   map[string]ModelCost
	statusCounts map[string]int
	members      []sessionCandidate
}

func groupSessionCandidates(candidates []sessionCandidate) []*sessionGroup {
	// Sort a copy to avoid mutating the cached input slice.
	sorted := make([]sessionCandidate, len(candidates))
	copy(sorted, candidates)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].summary.StartTime.Equal(sorted[j].summary.StartTime) {
			return sorted[i].summary.EndTime.Before(sorted[j].summary.EndTime)
		}
		return sorted[i].summary.StartTime.Before(sorted[j].summary.StartTime)
	})
	candidates = sorted

	groups := []*sessionGroup{}
	byKey := map[string]*sessionGroup{}

	for _, candidate := range candidates {
		key := sessionGroupKey(candidate.summary)
		group := byKey[key]

		if group == nil || candidate.summary.StartTime.Sub(group.summary.EndTime) > groupWindow {
			groupID := makeGroupID(key, candidate.summary.StartTime)
			group = &sessionGroup{
				summary: SessionSummary{
					ID:           groupID,
					Label:        candidate.summary.Label,
					Model:        candidate.summary.Model,
					Project:      candidate.summary.Project,
					AgentName:    candidate.summary.AgentName,
					Status:       candidate.summary.Status,
					StartTime:    candidate.summary.StartTime,
					EndTime:      candidate.summary.EndTime,
					Duration:     candidate.summary.Duration,
					InputTokens:  candidate.summary.InputTokens,
					OutputTokens: candidate.summary.OutputTokens,
					InputCost:    candidate.summary.InputCost,
					OutputCost:   candidate.summary.OutputCost,
					TotalCost:    candidate.summary.TotalCost,
					ToolCalls:    candidate.summary.ToolCalls,
					MessageCount: candidate.summary.MessageCount,
					SessionCount: 1,
				},
				modelCosts:   sessions.CopyModelCosts(candidate.modelCosts),
				statusCounts: map[string]int{candidate.summary.Status: 1},
				members:      []sessionCandidate{candidate},
			}
			groups = append(groups, group)
			byKey[key] = group
			continue
		}

		group.members = append(group.members, candidate)
		group.summary.EndTime = maxTime(group.summary.EndTime, candidate.summary.EndTime)
		group.summary.Duration = max(group.summary.EndTime.Sub(group.summary.StartTime), 0)
		group.summary.InputTokens += candidate.summary.InputTokens
		group.summary.OutputTokens += candidate.summary.OutputTokens
		group.summary.InputCost += candidate.summary.InputCost
		group.summary.OutputCost += candidate.summary.OutputCost
		group.summary.TotalCost += candidate.summary.TotalCost
		group.summary.ToolCalls += candidate.summary.ToolCalls
		group.summary.MessageCount += candidate.summary.MessageCount
		group.summary.SessionCount++
		group.statusCounts[candidate.summary.Status]++
		sessions.MergeModelCosts(group.modelCosts, candidate.modelCosts)
	}

	for _, group := range groups {
		group.summary.Status = summarizeGroupStatus(group.statusCounts)
		group.summary.Model = sessions.DominantModel(group.modelCosts)
		if group.summary.Model == "" {
			group.summary.Model = firstNonEmptyModel(group.members)
		}
	}

	return groups
}

func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func summarizeGroupStatus(counts map[string]int) string {
	if counts[StatusFailed] > 0 {
		return StatusFailed
	}
	if counts[StatusAbandoned] > 0 {
		return StatusAbandoned
	}
	if counts[StatusCompleted] > 0 {
		return StatusCompleted
	}
	return StatusUnknown
}

func sessionGroupKey(summary SessionSummary) string {
	label := normalizeSessionLabel(summary.Label)
	if label == "" {
		label = summary.ID
	}
	agent := strings.ToLower(strings.TrimSpace(summary.AgentName))
	project := strings.ToLower(strings.TrimSpace(summary.Project))
	return strings.Join([]string{label, agent, project}, "|")
}

func normalizeSessionLabel(label string) string {
	parts := strings.Fields(strings.ToLower(strings.TrimSpace(label)))
	return strings.Join(parts, " ")
}

func makeGroupID(key string, start time.Time) string {
	sum := sha256.Sum256([]byte(key))
	return groupIDPrefix + hex.EncodeToString(sum[:]) + ":" + strconv.FormatInt(start.Unix(), 10)
}

func groupIDKeyHash(summary SessionSummary) string {
	key := sessionGroupKey(summary)
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func parseGroupID(sessionID string) (string, int64, bool) {
	if !isGroupID(sessionID) {
		return "", 0, false
	}
	trimmed := strings.TrimPrefix(sessionID, groupIDPrefix)
	parts := strings.SplitN(trimmed, ":", 2)
	if len(parts) != 2 {
		return "", 0, false
	}
	startUnix, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return parts[0], startUnix, true
}

func findGroupByID(groups []*sessionGroup, sessionID string) *sessionGroup {
	for _, group := range groups {
		if group.summary.ID == sessionID {
			return group
		}
	}

	hash, startUnix, ok := parseGroupID(sessionID)
	if !ok {
		return nil
	}

	var best *sessionGroup
	var bestDelta int64
	for _, group := range groups {
		if groupIDKeyHash(group.summary) != hash {
			continue
		}
		delta := group.summary.StartTime.Unix() - startUnix
		if delta < 0 {
			delta = -delta
		}
		if best == nil || delta < bestDelta {
			best = group
			bestDelta = delta
		}
	}

	return best
}

func isGroupID(sessionID string) bool {
	return strings.HasPrefix(sessionID, groupIDPrefix)
}

func firstNonEmptyModel(members []sessionCandidate) string {
	for _, member := range members {
		if member.summary.Model != "" {
			return member.summary.Model
		}
	}
	return ""
}
