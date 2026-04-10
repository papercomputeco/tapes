package deck

import "time"

const (
	messageGroupWindow  = 5 * time.Second
	maxGroupedTextChars = 4000
)

// buildGroupedMessages collapses adjacent same-role messages within
// messageGroupWindow into SessionMessageGroup entries. The output is
// chronologically ordered and consumed by the deck TUI's transcript view.
func buildGroupedMessages(messages []SessionMessage) []SessionMessageGroup {
	if len(messages) == 0 {
		return nil
	}

	groups := make([]SessionMessageGroup, 0, len(messages))
	current := SessionMessageGroup{
		Role:         messages[0].Role,
		StartTime:    messages[0].Timestamp,
		EndTime:      messages[0].Timestamp,
		InputTokens:  messages[0].InputTokens,
		OutputTokens: messages[0].OutputTokens,
		TotalTokens:  messages[0].TotalTokens,
		InputCost:    messages[0].InputCost,
		OutputCost:   messages[0].OutputCost,
		TotalCost:    messages[0].TotalCost,
		ToolCalls:    uniqueToolCalls(messages[0].ToolCalls),
		Text:         truncateGroupedText(messages[0].Text),
		Count:        1,
		StartIndex:   0,
		EndIndex:     1,
	}

	for i := 1; i < len(messages); i++ {
		msg := messages[i]
		gap := msg.Timestamp.Sub(current.EndTime)
		if msg.Role == current.Role && gap <= messageGroupWindow {
			current.EndTime = msg.Timestamp
			current.InputTokens += msg.InputTokens
			current.OutputTokens += msg.OutputTokens
			current.TotalTokens += msg.TotalTokens
			current.InputCost += msg.InputCost
			current.OutputCost += msg.OutputCost
			current.TotalCost += msg.TotalCost
			current.ToolCalls = mergeToolCalls(current.ToolCalls, msg.ToolCalls)
			current.Text = appendGroupedText(current.Text, msg.Text)
			current.Count++
			current.EndIndex = i + 1
			continue
		}

		groups = append(groups, current)
		current = SessionMessageGroup{
			Role:         msg.Role,
			StartTime:    msg.Timestamp,
			EndTime:      msg.Timestamp,
			InputTokens:  msg.InputTokens,
			OutputTokens: msg.OutputTokens,
			TotalTokens:  msg.TotalTokens,
			InputCost:    msg.InputCost,
			OutputCost:   msg.OutputCost,
			TotalCost:    msg.TotalCost,
			ToolCalls:    uniqueToolCalls(msg.ToolCalls),
			Text:         truncateGroupedText(msg.Text),
			Count:        1,
			StartIndex:   i,
			EndIndex:     i + 1,
		}
	}

	groups = append(groups, current)
	for i := 1; i < len(groups); i++ {
		groups[i].Delta = groups[i].StartTime.Sub(groups[i-1].EndTime)
	}

	return groups
}

func uniqueToolCalls(calls []string) []string {
	if len(calls) == 0 {
		return nil
	}
	seen := map[string]bool{}
	unique := make([]string, 0, len(calls))
	for _, call := range calls {
		if call == "" || seen[call] {
			continue
		}
		seen[call] = true
		unique = append(unique, call)
	}
	return unique
}

func mergeToolCalls(existing, next []string) []string {
	if len(next) == 0 {
		return existing
	}
	if len(existing) == 0 {
		return uniqueToolCalls(next)
	}
	seen := map[string]bool{}
	for _, call := range existing {
		seen[call] = true
	}
	for _, call := range next {
		if call == "" || seen[call] {
			continue
		}
		seen[call] = true
		existing = append(existing, call)
	}
	return existing
}

func truncateGroupedText(text string) string {
	if text == "" {
		return ""
	}
	if len(text) <= maxGroupedTextChars {
		return text
	}
	return text[:maxGroupedTextChars] + "..."
}

func appendGroupedText(current, next string) string {
	if next == "" {
		return current
	}
	if current == "" {
		return truncateGroupedText(next)
	}
	if len(current) >= maxGroupedTextChars {
		return current
	}
	remaining := maxGroupedTextChars - len(current)
	separator := "\n\n"
	if remaining <= len(separator) {
		return current
	}
	remaining -= len(separator)
	if remaining <= 0 {
		return current
	}
	if len(next) > remaining {
		next = next[:remaining] + "..."
	}
	return current + separator + next
}
