package deckcmder

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/papercomputeco/tapes/pkg/deck"
)

func sortedModelCosts(costs map[string]deck.ModelCost) []deck.ModelCost {
	items := make([]deck.ModelCost, 0, len(costs))
	for _, cost := range costs {
		items = append(items, cost)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].TotalCost != items[j].TotalCost {
			return items[i].TotalCost > items[j].TotalCost
		}
		return items[i].Model < items[j].Model
	})

	return items
}

func clamp(value, upper int) int {
	if value < 0 {
		return 0
	}
	if value > upper {
		return upper
	}
	return value
}

func periodToDuration(p timePeriod) time.Duration {
	switch p {
	case period24h:
		return 24 * time.Hour
	case period30d:
		return 30 * 24 * time.Hour
	default:
		return 30 * 24 * time.Hour
	}
}

func periodToLabel(p timePeriod) string {
	switch p {
	case period24h:
		return "24h"
	case period30d:
		return "30d"
	default:
		return "30d"
	}
}

func changeArrow(change float64) string {
	if change > 0 {
		return "↑"
	}
	if change < 0 {
		return "↓"
	}
	return "→"
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func costGradientIndex(cost float64, sessions []deck.SessionSummary) int {
	if len(sessions) == 0 {
		return 0
	}
	minCost := cost
	maxCost := cost
	for _, s := range sessions {
		if s.TotalCost < minCost {
			minCost = s.TotalCost
		}
		if s.TotalCost > maxCost {
			maxCost = s.TotalCost
		}
	}
	if maxCost <= minCost {
		return len(costOrangeGradient) / 2
	}
	ratio := (cost - minCost) / (maxCost - minCost)
	index := int(ratio * float64(len(costOrangeGradient)-1))
	return clamp(index, len(costOrangeGradient)-1)
}

func formatCostIndicator(cost float64, allSessions []deck.SessionSummary) string {
	if len(allSessions) == 0 {
		return deckMutedStyle.Render("$")
	}

	// Map to $ symbols (1-5)
	index := costGradientIndex(cost, allSessions)
	symbols := min(max(index+1, 1), 5)

	// Create indicator with color
	indicator := strings.Repeat("$", symbols)
	colorIndex := min(max(index, 0), len(costOrangeGradient)-1)
	style := lipgloss.NewStyle().Foreground(lipgloss.Color(costOrangeGradient[colorIndex]))
	return style.Render(indicator)
}

func formatCostWithScale(cost float64, allSessions []deck.SessionSummary) string {
	if len(allSessions) == 0 {
		return formatCost(cost)
	}
	index := costGradientIndex(cost, allSessions)
	colorIndex := min(max(index, 0), len(costOrangeGradient)-1)
	style := lipgloss.NewStyle().Foreground(lipgloss.Color(costOrangeGradient[colorIndex]))
	return style.Render(formatCost(cost))
}

// renderCostWeightedBarbell creates a mini visualization showing token distribution and cost
// Format: ●──◍ where circle size = tokens, color = cost
func renderCostWeightedBarbell(inputTokens, outputTokens int64, inputCost, outputCost float64, allSessions []deck.SessionSummary) string {
	if len(allSessions) == 0 {
		return "●──●"
	}

	// Find token ranges across all sessions for scaling
	var maxInputTokens, maxOutputTokens int64
	var minCost, maxCost float64 = 999999, 0
	for _, s := range allSessions {
		if s.InputTokens > maxInputTokens {
			maxInputTokens = s.InputTokens
		}
		if s.OutputTokens > maxOutputTokens {
			maxOutputTokens = s.OutputTokens
		}
		if s.TotalCost > maxCost {
			maxCost = s.TotalCost
		}
		if s.TotalCost < minCost {
			minCost = s.TotalCost
		}
	}

	// Determine circle sizes based on token count (relative to max)
	inputSize := getCircleSize(inputTokens, maxInputTokens)
	outputSize := getCircleSize(outputTokens, maxOutputTokens)

	// Determine colors based on cost (using orange gradient)
	totalCost := inputCost + outputCost
	var costRatio float64
	if maxCost > minCost {
		costRatio = (totalCost - minCost) / (maxCost - minCost)
	} else {
		costRatio = 0.5
	}
	colorIndex := int(costRatio * float64(len(costOrangeGradient)-1))
	if colorIndex >= len(costOrangeGradient) {
		colorIndex = len(costOrangeGradient) - 1
	}
	if colorIndex < 0 {
		colorIndex = 0
	}

	orangeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color(costOrangeGradient[colorIndex]))

	// Build the barbell: input circle + connector + output circle
	connector := getConnector(inputSize, outputSize)
	barbell := inputSize + connector + outputSize

	return orangeStyle.Render(barbell)
}

// getCircleSize returns a Unicode circle character based on relative token size
func getCircleSize(tokens, maxTokens int64) string {
	if maxTokens == 0 {
		return "●"
	}

	ratio := float64(tokens) / float64(maxTokens)

	switch {
	case ratio < 0.3:
		return "·" // Small dot (U+00B7)
	case ratio < 0.6:
		return "●" // Medium circle (U+25CF)
	default:
		return circleLarge // Large circle (U+2B24)
	}
}

// getConnector returns a connector line based on the size difference
func getConnector(inputSize, outputSize string) string {
	// Determine line style based on asymmetry
	switch {
	case inputSize == "·" && outputSize == circleLarge:
		return "──—" // Input small, output large
	case inputSize == circleLarge && outputSize == "·":
		return "—──" // Input large, output small
	case inputSize == outputSize:
		return "──" // Balanced
	default:
		return "─—" // Slight asymmetry
	}
}

func colorizeModel(model string) string {
	modelLower := strings.ToLower(model)

	// Check for Claude models
	for tier, colorValue := range claudeColors {
		if strings.Contains(modelLower, tier) {
			return lipgloss.NewStyle().Foreground(lipgloss.Color(colorValue)).Render(model)
		}
	}

	// Check for OpenAI models
	for modelName, colorValue := range openaiColors {
		if strings.Contains(modelLower, modelName) || strings.Contains(modelLower, strings.ReplaceAll(modelName, "-", "")) {
			return lipgloss.NewStyle().Foreground(lipgloss.Color(colorValue)).Render(model)
		}
	}

	// Check for Google models
	for modelName, colorValue := range googleColors {
		if strings.Contains(modelLower, modelName) || strings.Contains(modelLower, strings.ReplaceAll(modelName, "-", "")) {
			return lipgloss.NewStyle().Foreground(lipgloss.Color(colorValue)).Render(model)
		}
	}

	// Default color for unknown models
	return deckMutedStyle.Render(model)
}

func formatStatusWithCircle(status string) (string, string) {
	var circle string
	text := status

	switch status {
	case deck.StatusCompleted:
		circle = deckStatusOKStyle.Render("●")
		text = lipgloss.NewStyle().Foreground(colorForeground).Render(text)
	case deck.StatusFailed:
		circle = deckStatusFailStyle.Render("●")
		text = lipgloss.NewStyle().Foreground(colorForeground).Render(text)
	case deck.StatusAbandoned:
		circle = deckStatusWarnStyle.Render("●")
		text = lipgloss.NewStyle().Foreground(colorForeground).Render(text)
	default:
		circle = deckMutedStyle.Render("○")
		text = deckMutedStyle.Render(text)
	}

	return circle, text
}

func formatCost(value float64) string {
	return fmt.Sprintf("$%.2f", value)
}

func formatTokens(value int64) string {
	if value >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(value)/1_000_000.0)
	}
	if value >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(value)/1_000.0)
	}
	return strconv.FormatInt(value, 10)
}

func formatDuration(value time.Duration) string {
	if value <= 0 {
		return "0s"
	}

	minutes := int(value.Minutes())
	hours := minutes / 60
	minutes %= 60
	if hours > 0 {
		return fmt.Sprintf("%d%s%02d%s", hours, deckDimStyle.Render("h"), minutes, deckDimStyle.Render("m"))
	}
	if minutes > 0 {
		return fmt.Sprintf("%d%s", minutes, deckDimStyle.Render("m"))
	}
	return "0" + deckDimStyle.Render("m")
}

func formatDurationMinutes(value time.Duration) string {
	if value <= 0 {
		return "0m"
	}
	minutes := int(value.Minutes())
	if minutes < 1 {
		return "<1m"
	}
	return fmt.Sprintf("%dm", minutes)
}

func formatPercent(value float64) string {
	return fmt.Sprintf("%.0f%%", value*100)
}

func truncateText(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	if limit <= 3 {
		return value[:limit]
	}
	return value[:limit-3] + "..."
}

func renderBar(value, ceiling float64, width int) string {
	if ceiling <= 0 {
		return strings.Repeat("░", width)
	}
	ratio := value / ceiling
	filled := min(max(int(ratio*float64(width)), 0), width)
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func renderHeaderLine(width int, left, right string) string {
	lineWidth := width
	if lineWidth <= 0 {
		lineWidth = 80
	}
	leftWidth := lipgloss.Width(left)
	rightWidth := lipgloss.Width(right)
	if leftWidth+rightWidth+1 >= lineWidth {
		return strings.TrimSpace(left + " " + right)
	}
	spacing := lineWidth - leftWidth - rightWidth
	return left + strings.Repeat(" ", spacing) + right
}

func renderRule(width int) string {
	lineWidth := width
	if lineWidth <= 0 {
		lineWidth = 80
	}
	return deckDividerStyle.Render(strings.Repeat("─", lineWidth))
}

func addHorizontalPadding(line string) string {
	padding := strings.Repeat(" ", horizontalPadding)
	return padding + line
}

func addPadding(content string) string {
	if content == "" {
		return content
	}
	lines := strings.Split(content, "\n")
	paddedLines := make([]string, 0, len(lines)+2*verticalPadding)

	// Add top padding
	for range verticalPadding {
		paddedLines = append(paddedLines, "")
	}

	// Add horizontal padding to each line
	for _, line := range lines {
		paddedLines = append(paddedLines, addHorizontalPadding(line))
	}

	// Add bottom padding
	for range verticalPadding {
		paddedLines = append(paddedLines, "")
	}

	return strings.Join(paddedLines, "\n")
}

// detailLabel returns the first real user prompt for the detail view
// breadcrumb. Falls back to the summary label when no messages are available.
func (m deckModel) detailLabel() string {
	if m.detail == nil || len(m.detail.Messages) == 0 {
		return m.detail.Summary.Label
	}

	for _, msg := range m.detail.Messages {
		if msg.Role != roleUser {
			continue
		}
		text := stripSystemContent(msg.Text)
		line := firstNonEmptyLine(text)
		if line != "" {
			return line
		}
	}

	return m.detail.Summary.Label
}

func firstNonEmptyLine(text string) string {
	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if isSystemLine(line) {
			continue
		}
		return line
	}
	return ""
}

func stripSystemContent(text string) string {
	for _, tag := range []string{"system-reminder", "local-command"} {
		text = deck.StripTaggedSection(text, tag)
	}
	return strings.TrimSpace(text)
}

func isSystemLine(line string) bool {
	if strings.HasPrefix(line, "<") && strings.HasSuffix(line, ">") {
		return true
	}
	return strings.HasPrefix(strings.ToLower(line), "command:")
}

func (m deckModel) applyBackground(content string) string {
	content = deckBackgroundStyle.Render(content)
	contentWidth := lipgloss.Width(content)
	contentHeight := strings.Count(content, "\n") + 1
	width := max(m.width+2*horizontalPadding, contentWidth)
	height := max(m.height+2*verticalPadding, contentHeight)
	if width <= 0 || height <= 0 {
		return content
	}
	return lipgloss.Place(
		width,
		height,
		lipgloss.Left,
		lipgloss.Top,
		content,
		lipgloss.WithWhitespaceStyle(lipgloss.NewStyle().Background(colorBaseBg)),
	)
}

func renderCassetteTape() []string {
	// Static cassette tape graphic
	return []string{
		deckMutedStyle.Render(" ╭─╮╭─╮ "),
		deckMutedStyle.Render(" │●││●│ "),
		deckMutedStyle.Render(" ╰─╯╰─╯ "),
	}
}

func fitCell(value string, width int) string {
	if width <= 0 {
		return value
	}
	if lipgloss.Width(value) > width {
		return truncateText(value, width)
	}
	return value + strings.Repeat(" ", width-lipgloss.Width(value))
}

func avgTokenCount(total int64, count int) int64 {
	if count <= 0 {
		return 0
	}
	return total / int64(count)
}

func statusStyleFor(status string) lipgloss.Style {
	switch status {
	case deck.StatusCompleted:
		return deckStatusOKStyle
	case deck.StatusFailed:
		return deckStatusFailStyle
	case deck.StatusAbandoned:
		return deckStatusWarnStyle
	default:
		return deckMutedStyle
	}
}

func splitPercent(inputCost, outputCost float64) (float64, float64) {
	total := inputCost + outputCost
	if total <= 0 {
		return 0, 0
	}
	return (inputCost / total) * 100, (outputCost / total) * 100
}

func renderTokenSplitBar(inputPercent float64, width int) string {
	if width <= 0 {
		width = 24
	}
	filled := min(max(int((inputPercent/100)*float64(width)), 0), width)
	inputStyle := lipgloss.NewStyle().Foreground(colorGreen)
	outputStyle := lipgloss.NewStyle().Foreground(colorMagenta)

	bar := inputStyle.Render(strings.Repeat("░", filled)) +
		outputStyle.Render(strings.Repeat("░", width-filled))
	return bar
}

type deckOverviewStats struct {
	TotalSessions  int
	TotalCost      float64
	InputTokens    int64
	OutputTokens   int64
	TotalDuration  time.Duration
	TotalToolCalls int
	SuccessRate    float64
	Completed      int
	Failed         int
	Abandoned      int
	CostByModel    map[string]deck.ModelCost
}

func summarizeSessions(sessions []deck.SessionSummary) deckOverviewStats {
	stats := deckOverviewStats{
		TotalSessions: len(sessions),
		CostByModel:   map[string]deck.ModelCost{},
	}
	for _, session := range sessions {
		stats.TotalCost += session.TotalCost
		stats.InputTokens += session.InputTokens
		stats.OutputTokens += session.OutputTokens
		stats.TotalDuration += session.Duration
		stats.TotalToolCalls += session.ToolCalls
		switch session.Status {
		case deck.StatusCompleted:
			stats.Completed++
		case deck.StatusFailed:
			stats.Failed++
		case deck.StatusAbandoned:
			stats.Abandoned++
		}

		modelCost := stats.CostByModel[session.Model]
		modelCost.Model = session.Model
		modelCost.InputTokens += session.InputTokens
		modelCost.OutputTokens += session.OutputTokens
		modelCost.InputCost += session.InputCost
		modelCost.OutputCost += session.OutputCost
		modelCost.TotalCost += session.TotalCost
		modelCost.SessionCount++
		stats.CostByModel[session.Model] = modelCost
	}
	if stats.TotalSessions > 0 {
		stats.SuccessRate = float64(stats.Completed) / float64(stats.TotalSessions)
	}
	return stats
}

func (m deckModel) selectedSessions() []deck.SessionSummary {
	if m.overview == nil || len(m.overview.Sessions) == 0 {
		return nil
	}

	// Always return all sessions for metrics/chrome calculations.
	// Search filtering only applies to the session list view.
	return m.overview.Sessions
}

func (m deckModel) headerSessionCount(lastWindow string, selected, total int, filtered bool) string {
	if filtered {
		return fmt.Sprintf("last %s · %d/%d sessions", lastWindow, selected, total)
	}
	return fmt.Sprintf("last %s · %d sessions", lastWindow, total)
}

func (m deckModel) sortedMessages() []deck.SessionMessage {
	if m.detail == nil || len(m.detail.Messages) == 0 {
		return nil
	}

	sortKey := messageSortOrder[m.messageSort%len(messageSortOrder)]
	cacheKey := fmt.Sprintf("%s:%s", sortKey, m.detail.Summary.ID)
	cache := m.sortedCache
	if cache != nil &&
		cache.key == cacheKey &&
		cache.id == m.detail.Summary.ID &&
		cache.count == len(m.detail.Messages) &&
		len(cache.messages) > 0 {
		return cache.messages
	}

	if sortKey == "time" {
		if cache != nil {
			cache.messages = m.detail.Messages
			cache.key = cacheKey
			cache.id = m.detail.Summary.ID
			cache.count = len(m.detail.Messages)
		}
		return m.detail.Messages
	}

	messages := make([]deck.SessionMessage, len(m.detail.Messages))
	copy(messages, m.detail.Messages)
	sort.SliceStable(messages, func(i, j int) bool {
		switch sortKey {
		case "tokens":
			if messages[i].TotalTokens == messages[j].TotalTokens {
				return messages[i].Timestamp.Before(messages[j].Timestamp)
			}
			return messages[i].TotalTokens > messages[j].TotalTokens
		case sortKeyCost:
			if messages[i].TotalCost == messages[j].TotalCost {
				return messages[i].Timestamp.Before(messages[j].Timestamp)
			}
			return messages[i].TotalCost > messages[j].TotalCost
		case "delta":
			if messages[i].Delta == messages[j].Delta {
				return messages[i].Timestamp.Before(messages[j].Timestamp)
			}
			return messages[i].Delta > messages[j].Delta
		default:
			return messages[i].Timestamp.Before(messages[j].Timestamp)
		}
	})

	if cache != nil {
		cache.messages = messages
		cache.key = cacheKey
		cache.id = m.detail.Summary.ID
		cache.count = len(m.detail.Messages)
	}
	return messages
}

func (m deckModel) resetSortedCache() {
	if m.sortedCache == nil {
		return
	}
	m.sortedCache.messages = nil
	m.sortedCache.key = ""
	m.sortedCache.id = ""
	m.sortedCache.count = 0
}

func (m deckModel) sortedGroupedMessages() []deck.SessionMessageGroup {
	if m.detail == nil {
		return nil
	}
	groups := m.timelineGroups()
	if len(groups) == 0 {
		return nil
	}

	sortKey := messageSortOrder[m.messageSort%len(messageSortOrder)]
	cacheKey := fmt.Sprintf("%s:%s", sortKey, m.detail.Summary.ID)
	cache := m.sortedGroupCache
	if cache != nil &&
		cache.key == cacheKey &&
		cache.id == m.detail.Summary.ID &&
		cache.count == len(groups) &&
		len(cache.groups) > 0 {
		return cache.groups
	}

	if sortKey == "time" {
		if cache != nil {
			cache.groups = groups
			cache.key = cacheKey
			cache.id = m.detail.Summary.ID
			cache.count = len(groups)
		}
		return groups
	}

	sorted := make([]deck.SessionMessageGroup, len(groups))
	copy(sorted, groups)
	sort.SliceStable(sorted, func(i, j int) bool {
		switch sortKey {
		case "tokens":
			if sorted[i].TotalTokens == sorted[j].TotalTokens {
				return sorted[i].StartTime.Before(sorted[j].StartTime)
			}
			return sorted[i].TotalTokens > sorted[j].TotalTokens
		case sortKeyCost:
			if sorted[i].TotalCost == sorted[j].TotalCost {
				return sorted[i].StartTime.Before(sorted[j].StartTime)
			}
			return sorted[i].TotalCost > sorted[j].TotalCost
		case "delta":
			if sorted[i].Delta == sorted[j].Delta {
				return sorted[i].StartTime.Before(sorted[j].StartTime)
			}
			return sorted[i].Delta > sorted[j].Delta
		default:
			return sorted[i].StartTime.Before(sorted[j].StartTime)
		}
	})

	if cache != nil {
		cache.groups = sorted
		cache.key = cacheKey
		cache.id = m.detail.Summary.ID
		cache.count = len(groups)
	}
	return sorted
}

func (m deckModel) resetSortedGroupCache() {
	if m.sortedGroupCache == nil {
		return
	}
	m.sortedGroupCache.groups = nil
	m.sortedGroupCache.key = ""
	m.sortedGroupCache.id = ""
	m.sortedGroupCache.count = 0
}

func (m deckModel) useGroupedConversations() bool {
	return m.detail != nil && len(m.detail.GroupedMessages) > 0
}

func (m deckModel) currentConversationLength() int {
	if m.useGroupedConversations() {
		return len(m.sortedGroupedMessages())
	}
	return len(m.sortedMessages())
}

func (m deckModel) timelineGroups() []deck.SessionMessageGroup {
	if m.detail == nil {
		return nil
	}
	if len(m.detail.GroupedMessages) > 0 {
		return m.detail.GroupedMessages
	}
	return messageGroupsFromMessages(m.detail.Messages)
}

func (m deckModel) selectedGroup() *deck.SessionMessageGroup {
	if !m.useGroupedConversations() {
		return nil
	}
	groups := m.sortedGroupedMessages()
	if len(groups) == 0 {
		return nil
	}
	index := clamp(m.messageCursor, len(groups)-1)
	return &groups[index]
}

func isSelectedGroup(selected *deck.SessionMessageGroup, candidate *deck.SessionMessageGroup) bool {
	if selected == nil || candidate == nil {
		return false
	}
	return selected.StartIndex == candidate.StartIndex && selected.EndIndex == candidate.EndIndex
}

func findSelectedGroupIndex(groups []deck.SessionMessageGroup, selected *deck.SessionMessageGroup) int {
	if selected == nil {
		return 0
	}
	for i := range groups {
		if isSelectedGroup(selected, &groups[i]) {
			return i
		}
	}
	return 0
}

func messageGroupsFromMessages(messages []deck.SessionMessage) []deck.SessionMessageGroup {
	if len(messages) == 0 {
		return nil
	}
	groups := make([]deck.SessionMessageGroup, 0, len(messages))
	for i, msg := range messages {
		groups = append(groups, deck.SessionMessageGroup{
			Role:         msg.Role,
			StartTime:    msg.Timestamp,
			EndTime:      msg.Timestamp,
			Delta:        msg.Delta,
			InputTokens:  msg.InputTokens,
			OutputTokens: msg.OutputTokens,
			TotalTokens:  msg.TotalTokens,
			InputCost:    msg.InputCost,
			OutputCost:   msg.OutputCost,
			TotalCost:    msg.TotalCost,
			ToolCalls:    msg.ToolCalls,
			Text:         msg.Text,
			Count:        1,
			StartIndex:   i,
			EndIndex:     i + 1,
		})
	}
	return groups
}

func padLines(lines []string, width, height int) []string {
	if height <= 0 {
		return []string{}
	}
	if width <= 0 {
		width = 1
	}
	result := make([]string, 0, height)
	for _, line := range lines {
		result = append(result, padRight(line, width))
		if len(result) >= height {
			return result[:height]
		}
	}
	for len(result) < height {
		result = append(result, strings.Repeat(" ", width))
	}
	return result
}

func padRight(value string, width int) string {
	visualWidth := lipgloss.Width(value)
	if visualWidth >= width {
		return value
	}
	return value + strings.Repeat(" ", width-visualWidth)
}

func padRightWithColor(coloredValue string, width int) string {
	// Use lipgloss.Width to get the visual width (without ANSI codes)
	visualWidth := lipgloss.Width(coloredValue)
	if visualWidth >= width {
		return coloredValue
	}
	return coloredValue + strings.Repeat(" ", width-visualWidth)
}

func joinColumns(left, right []string, gap int) []string {
	maxLines := max(len(right), len(left))
	lines := make([]string, 0, maxLines)
	gapSpace := strings.Repeat(" ", gap)
	for i := range maxLines {
		leftLine := ""
		if i < len(left) {
			leftLine = left[i]
		}
		rightLine := ""
		if i < len(right) {
			rightLine = right[i]
		}
		lines = append(lines, leftLine+gapSpace+rightLine)
	}
	return lines
}

func stableVisibleRange(total, cursor, size, offset int) (start, end, newOffset int) {
	if total <= 0 || size <= 0 {
		return 0, 0, 0
	}
	if total <= size {
		return 0, total, 0
	}
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= total {
		cursor = total - 1
	}

	// Keep current offset unless cursor is outside the visible window
	if cursor < offset {
		offset = cursor
	} else if cursor >= offset+size {
		offset = cursor - size + 1
	}

	// Clamp offset to valid range
	maxOffset := total - size
	if offset > maxOffset {
		offset = maxOffset
	}
	if offset < 0 {
		offset = 0
	}

	return offset, offset + size, offset
}

func sessionListVisibleRows(totalSessions, availableHeight int) int {
	if availableHeight <= 0 {
		return 1
	}
	visible := max(availableHeight-sessionListChromeLines, 1)
	if totalSessions > visible {
		visible = max(availableHeight-sessionListChromeLines-sessionListPositionLines, 1)
	}
	return visible
}

func safeDivide(value, divisor float64) float64 {
	if divisor == 0 {
		return 0
	}
	return value / divisor
}

func formatTokensDetail(value int64) string {
	if value < 10_000 {
		return formatInt(value) + " tok"
	}
	return formatTokens(value) + " tok"
}

func formatInt(value int64) string {
	str := strconv.FormatInt(value, 10)
	if len(str) <= 3 {
		return str
	}
	var parts []string
	for len(str) > 3 {
		parts = append([]string{str[len(str)-3:]}, parts...)
		str = str[:len(str)-3]
	}
	if str != "" {
		parts = append([]string{str}, parts...)
	}
	return strings.Join(parts, ",")
}

func toolUsageIcon(toolCalls []string) string {
	if len(toolCalls) == 0 {
		return ""
	}
	if hasToolVerb(toolCalls, []string{"write", "create", "update", "delete", "patch", "put", "post"}) {
		return "✎"
	}
	if hasToolVerb(toolCalls, []string{"read", "get", "list", "search", "fetch"}) {
		return "⚯"
	}
	return "⚙"
}

func hasToolVerb(toolCalls []string, verbs []string) bool {
	for _, call := range toolCalls {
		lower := strings.ToLower(call)
		for _, verb := range verbs {
			if strings.Contains(lower, verb) {
				return true
			}
		}
	}
	return false
}

func fitCellRight(value string, width int) string {
	if width <= 0 {
		return value
	}
	if lipgloss.Width(value) >= width {
		return value
	}
	return strings.Repeat(" ", width-lipgloss.Width(value)) + value
}

func wrapText(text string, width int) []string {
	if width <= 0 {
		return []string{text}
	}
	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{""}
	}
	lines := []string{}
	current := ""
	for _, word := range words {
		if current == "" {
			current = word
			continue
		}
		if lipgloss.Width(current)+1+lipgloss.Width(word) <= width {
			current = current + " " + word
			continue
		}
		lines = append(lines, current)
		current = word
	}
	if current != "" {
		lines = append(lines, current)
	}
	return lines
}

// truncateString truncates a string to fit within the specified width,
// accounting for ANSI styling codes using lipgloss.Width
func truncateString(text string, width int) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(text) <= width {
		return text
	}

	// Iterate through runes and build result until we hit width limit
	result := ""
	for _, r := range text {
		test := result + string(r)
		if lipgloss.Width(test) > width {
			break
		}
		result = test
	}
	return result
}
