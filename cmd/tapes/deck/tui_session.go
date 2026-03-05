package deckcmder

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/papercomputeco/tapes/pkg/deck"
)

func (m deckModel) viewSession() string {
	if m.detail == nil {
		return deckMutedStyle.Render("no session selected")
	}

	// Breadcrumb navigation: tapes > [project >] session-name
	statusStyle := statusStyleFor(m.detail.Summary.Status)
	statusDot := statusStyle.Render("●")
	breadcrumb := deckAccentStyle.Render("tapes")
	if m.detail.Summary.Project != "" {
		breadcrumb += deckMutedStyle.Render(" > ") + deckMutedStyle.Render(m.detail.Summary.Project)
	}
	// Cap the title so it doesn't overflow the header line
	titleMaxWidth := max(m.width*3/4, 40)
	title := m.detailLabel()
	if lipgloss.Width(title) > titleMaxWidth {
		title = truncateText(title, titleMaxWidth)
	}
	breadcrumb += deckMutedStyle.Render(" > ") + deckTitleStyle.Render(title)
	statusRight := statusDot + " " + deckMutedStyle.Render(m.detail.Summary.Status)
	header := renderHeaderLine(m.width, breadcrumb, statusRight)

	sessionID := m.detail.Summary.ID
	if len(sessionID) > 7 {
		sessionID = sessionID[:7]
	}
	idText := deckMutedStyle.Render(sessionID)
	if len(m.detail.SubSessions) > 1 {
		idText = deckMutedStyle.Render(fmt.Sprintf("%s · %d sessions", sessionID, len(m.detail.SubSessions)))
	}
	idLine := renderHeaderLine(m.width, "", idText)

	lines := make([]string, 0, 30)
	lines = append(lines, header, idLine, renderRule(m.width), "")

	// 1. METRICS SECTION
	lines = append(lines, m.renderSessionMetrics()...)
	lines = append(lines, "", renderRule(m.width), "")

	// 2. CONVERSATION TIMELINE (waveform visualization)
	lines = append(lines, m.renderConversationTimeline()...)
	lines = append(lines, "", renderRule(m.width), "")

	// 3 & 4. CONVERSATION TABLE + MESSAGE DETAIL (side by side)
	footer := m.viewSessionFooter()
	above := strings.Join(lines, "\n")
	chromeLines := countWrappedLines(above, m.width) + countWrappedLines(footer, m.width) + 1
	screenHeight := m.height
	if screenHeight <= 0 {
		screenHeight = 40
	}
	remaining := max(screenHeight-chromeLines, 10)

	gap := 3
	// Table takes ~45%, detail pane takes ~55% (detail is ~20% wider than table)
	tableWidth := max((m.width-gap)*5/11, 40)
	detailWidth := m.width - gap - tableWidth
	if detailWidth < 25 {
		detailWidth = 25
		tableWidth = m.width - gap - detailWidth
	}

	tableBlock := m.renderConversationTable(tableWidth, remaining)
	detailBlock := m.renderMessageDetailPane(detailWidth, remaining)
	tableLines := joinColumns(tableBlock, detailBlock, gap)

	return above + "\n" + strings.Join(tableLines, "\n") + "\n\n" + footer
}

func (m deckModel) renderSessionMetrics() []string {
	// Calculate averages from overview for comparisons
	var avgCost, avgDuration, avgTokens, avgToolCalls float64
	if m.overview != nil && len(m.overview.Sessions) > 0 {
		var totalCost, totalDuration float64
		var totalTokens, totalToolCalls int64
		for _, s := range m.overview.Sessions {
			totalCost += s.TotalCost
			totalDuration += float64(s.Duration)
			totalTokens += s.InputTokens + s.OutputTokens
			totalToolCalls += int64(s.ToolCalls)
		}
		count := float64(len(m.overview.Sessions))
		avgCost = totalCost / count
		avgDuration = totalDuration / count
		avgTokens = float64(totalTokens) / count
		avgToolCalls = float64(totalToolCalls) / count
	}

	// Calculate this session's values
	thisCost := m.detail.Summary.TotalCost
	thisDuration := float64(m.detail.Summary.Duration)
	thisTokens := float64(m.detail.Summary.InputTokens + m.detail.Summary.OutputTokens)
	thisToolCalls := float64(m.detail.Summary.ToolCalls)
	toolsPerTurn := thisToolCalls / float64(max(1, m.detail.Summary.MessageCount))

	// Prepare metric data (matching overview page style)
	type metricData struct {
		label      string
		value      string
		change     string
		changeIcon string
		isPositive bool
		secondary  string
	}

	metrics := []metricData{
		{
			label:     "TOTAL COST",
			value:     formatCost(thisCost),
			secondary: formatCost(avgCost) + " avg",
		},
		{
			label:     "TOKENS USED",
			value:     fmt.Sprintf("%s in / %s out", formatTokens(m.detail.Summary.InputTokens), formatTokens(m.detail.Summary.OutputTokens)),
			secondary: formatTokens(int64(avgTokens)) + " avg",
		},
		{
			label:     "AGENT TIME",
			value:     formatDuration(m.detail.Summary.Duration),
			secondary: formatDuration(time.Duration(avgDuration)) + " avg",
		},
		{
			label:     "TOOL CALLS",
			value:     strconv.Itoa(m.detail.Summary.ToolCalls),
			secondary: fmt.Sprintf("%.1f tools/turn", toolsPerTurn),
		},
	}

	// Add comparison data if we have overview stats
	if m.overview != nil && len(m.overview.Sessions) > 0 {
		// Cost comparison
		if avgCost > 0 {
			change := ((thisCost - avgCost) / avgCost) * 100
			metrics[0].change = fmt.Sprintf("%.1f%% vs avg", abs(change))
			metrics[0].changeIcon = changeArrow(change)
			metrics[0].isPositive = change < 0 // Lower cost is better
		}

		// Tokens comparison
		if avgTokens > 0 {
			change := ((thisTokens - avgTokens) / avgTokens) * 100
			metrics[1].change = fmt.Sprintf("%.1f%% vs avg", abs(change))
			metrics[1].changeIcon = changeArrow(change)
			metrics[1].isPositive = change > 0 // More tokens = more work
		}

		// Duration comparison
		if avgDuration > 0 {
			change := ((thisDuration - avgDuration) / avgDuration) * 100
			metrics[2].change = fmt.Sprintf("%.1f%% vs avg", abs(change))
			metrics[2].changeIcon = changeArrow(change)
			metrics[2].isPositive = change > 0 // More time = more work
		}

		// Tool calls comparison
		if avgToolCalls > 0 {
			change := ((thisToolCalls - avgToolCalls) / avgToolCalls) * 100
			metrics[3].change = fmt.Sprintf("%.1f%% vs avg", abs(change))
			metrics[3].changeIcon = changeArrow(change)
			metrics[3].isPositive = change > 0 // More tools = more work
		}
	}

	// Render metrics in grid layout (matching overview page)
	cols := len(metrics)
	lineWidth := m.width
	if lineWidth <= 0 {
		lineWidth = 80
	}

	spaceWidth := (cols - 1) * 3
	colWidth := max((lineWidth-spaceWidth)/cols, 16)

	labelStyle := lipgloss.NewStyle().Foreground(colorLabel).Bold(true)
	highlightValueStyle := lipgloss.NewStyle().Foreground(colorForeground).Bold(true)
	lightGrayStyle := lipgloss.NewStyle().Foreground(colorBrightBlack)
	dimSeparator := deckDimStyle.Render(" │ ")

	blockHeight := 4
	lines := make([]string, 0, blockHeight)
	metricBlocks := make([][]string, 0, cols)

	totalTokens := m.detail.Summary.InputTokens + m.detail.Summary.OutputTokens
	tokenInPercent, tokenOutPercent := splitPercent(float64(m.detail.Summary.InputTokens), float64(m.detail.Summary.OutputTokens))

	for _, metric := range metrics {
		block := make([]string, 0, blockHeight)

		// Line 1: label
		block = append(block, labelStyle.Render(fitCell(metric.label, colWidth)))

		// Tokens metric gets a custom layout to match the design.
		if metric.label == "TOKENS USED" {
			// Line 2: total + change
			totalStr := highlightValueStyle.Render(formatTokens(totalTokens) + " total")
			changeStr := ""
			if metric.change != "" {
				arrowStyle := deckStatusFailStyle
				if metric.isPositive {
					arrowStyle = deckStatusOKStyle
				}
				changeStr = arrowStyle.Render(metric.changeIcon) + " " + lightGrayStyle.Render(metric.change)
			}
			line2 := totalStr
			if changeStr != "" {
				line2 = totalStr + "  " + changeStr
			}
			block = append(block, fitCell(line2, colWidth))

			// Line 3: input/output breakdown
			leftWidth := max(colWidth/2-1, 8)
			rightWidth := max(colWidth-leftWidth-1, 8)
			left := fmt.Sprintf("%s in  %2.0f%%", formatTokens(m.detail.Summary.InputTokens), tokenInPercent)
			right := fmt.Sprintf("%s out %2.0f%%", formatTokens(m.detail.Summary.OutputTokens), tokenOutPercent)
			line3 := fitCell(left, leftWidth) + " " + fitCellRight(right, rightWidth)
			block = append(block, fitCell(line3, colWidth))

			// Line 4: split bar
			barWidth := max(colWidth, 12)
			block = append(block, fitCell(renderTokenSplitBar(tokenInPercent, barWidth), colWidth))
		} else {
			// Line 2: value
			block = append(block, highlightValueStyle.Render(fitCell(metric.value, colWidth)))

			// Line 3: comparison
			if metric.change != "" {
				arrowStyle := deckStatusFailStyle
				if metric.isPositive {
					arrowStyle = deckStatusOKStyle
				}
				comp := arrowStyle.Render(metric.changeIcon) + " " + lightGrayStyle.Render(metric.change)
				block = append(block, fitCell(comp, colWidth))
			} else {
				block = append(block, deckMutedStyle.Render(fitCell("—", colWidth)))
			}

			// Line 4: secondary
			block = append(block, deckMutedStyle.Render(fitCell(metric.secondary, colWidth)))
		}

		metricBlocks = append(metricBlocks, block)
	}

	for line := range blockHeight {
		row := make([]string, 0, cols*2)
		for i, block := range metricBlocks {
			row = append(row, block[line])
			if i < cols-1 {
				row = append(row, dimSeparator)
			}
		}
		lines = append(lines, strings.Join(row, ""))
	}

	return lines
}

func (m deckModel) viewFooter() string {
	helpText := "j down • k up • enter drill • h back • s sort • f status • / search • p period • r replay • a analytics • q quit"
	return deckMutedStyle.Render(helpText)
}

func (m deckModel) viewSessionFooter() string {
	return deckMutedStyle.Render(m.help.View(m.keys))
}

func (m deckModel) renderConversationTimeline() []string {
	lines := []string{}

	if m.detail == nil || len(m.detail.Messages) == 0 {
		lines = append(lines, deckSectionStyle.Render("timeline"))
		lines = append(lines, deckMutedStyle.Render("no messages"))
		return lines
	}

	groups := m.timelineGroups()
	if len(groups) == 0 {
		lines = append(lines, deckSectionStyle.Render("timeline"))
		lines = append(lines, deckMutedStyle.Render("no messages"))
		return lines
	}

	// Header with sort info
	sortLabel := messageSortOrder[m.messageSort%len(messageSortOrder)]
	headerLeft := deckSectionStyle.Render("conversation timeline") + "  " +
		deckMutedStyle.Render(fmt.Sprintf("(sort: %s, %d groups, %d messages)", sortLabel, len(groups), len(m.detail.Messages)))
	lines = append(lines, headerLeft)

	// Build waveform visualization
	selected := m.selectedGroup()
	waveformLines := m.buildWaveform(groups, selected)
	lines = append(lines, waveformLines...)

	return lines
}

type waveformPoint struct {
	tokens    int64
	role      string
	hasTools  bool
	toolCalls []string
	start     int
	end       int
	isCurrent bool
}

func sampleWaveformPoints(groups []deck.SessionMessageGroup, maxBars int, selected *deck.SessionMessageGroup) []waveformPoint {
	if len(groups) == 0 {
		return nil
	}
	if maxBars <= 0 {
		maxBars = 1
	}
	if len(groups) <= maxBars {
		points := make([]waveformPoint, 0, len(groups))
		for _, group := range groups {
			points = append(points, waveformPoint{
				tokens:    group.TotalTokens,
				role:      group.Role,
				hasTools:  len(group.ToolCalls) > 0,
				toolCalls: group.ToolCalls,
				start:     group.StartIndex,
				end:       group.EndIndex,
				isCurrent: isSelectedGroup(selected, &group),
			})
		}
		return points
	}

	windowSize := min(len(groups), maxBars*waveformWindowMultiplier)
	if windowSize <= 0 {
		windowSize = len(groups)
	}
	groupSize := windowSize / maxBars
	if windowSize%maxBars != 0 {
		groupSize++
	}
	if groupSize <= 0 {
		groupSize = 1
	}
	selectedIndex := findSelectedGroupIndex(groups, selected)
	startIndex := clamp(selectedIndex-windowSize/2, max(0, len(groups)-windowSize))
	endIndex := min(startIndex+windowSize, len(groups))
	points := make([]waveformPoint, 0, maxBars)
	for start := startIndex; start < endIndex; start += groupSize {
		end := min(start+groupSize, endIndex)
		var maxTokens int64
		userCount := 0
		asstCount := 0
		hasTools := false
		var mergedTools []string
		isCurrent := false
		startMessage := groups[start].StartIndex
		endMessage := groups[end-1].EndIndex
		for i := start; i < end; i++ {
			group := groups[i]
			if group.TotalTokens > maxTokens {
				maxTokens = group.TotalTokens
			}
			if group.Role == roleUser {
				userCount++
			} else {
				asstCount++
			}
			if len(group.ToolCalls) > 0 {
				hasTools = true
				mergedTools = append(mergedTools, group.ToolCalls...)
			}
			if isSelectedGroup(selected, &group) {
				isCurrent = true
			}
		}
		role := roleAssistant
		if userCount > asstCount {
			role = roleUser
		}
		points = append(points, waveformPoint{
			tokens:    maxTokens,
			role:      role,
			hasTools:  hasTools,
			toolCalls: mergedTools,
			start:     startMessage,
			end:       endMessage,
			isCurrent: isCurrent,
		})
	}

	return points
}

func (m deckModel) buildWaveform(groups []deck.SessionMessageGroup, selected *deck.SessionMessageGroup) []string {
	// Calculate available width for waveform
	availWidth := m.width
	if availWidth <= 0 {
		availWidth = 80
	}

	// Reserve space for labels and padding
	axisWidth := 8

	maxBarHeight := 5
	gap := " "
	barWidth := max(1, (availWidth-axisWidth)/2)
	points := sampleWaveformPoints(groups, barWidth, selected)

	// Find max tokens for scaling
	var maxTokens int64
	for _, point := range points {
		if point.tokens > maxTokens {
			maxTokens = point.tokens
		}
	}
	if maxTokens == 0 {
		maxTokens = 1
	}

	// Waveform bars (multi-line) to represent token volume
	lines := make([]string, 0, maxBarHeight+3)
	barLines := make([]string, maxBarHeight)
	toolMarkers := []string{}
	msgNumbers := []string{}

	for i, point := range points {
		// Calculate bar height (1-maxBarHeight)
		ratio := float64(point.tokens) / float64(maxTokens)
		barHeight := int(ratio * float64(maxBarHeight))
		barHeight = min(max(barHeight, 1), maxBarHeight)

		// Choose bar style based on role
		var barStyle lipgloss.Style

		if point.role == roleUser {
			barStyle = deckRoleUserStyle // Cyan for user
		} else {
			barStyle = deckRoleAsstStyle // Orange for assistant
		}

		isCurrent := point.isCurrent
		// Highlight current message with a subtle background
		if isCurrent {
			barStyle = barStyle.Bold(true).Background(colorHighlightBg)
		}

		// Build stacked bar from top to bottom
		for row := range maxBarHeight {
			empty := row < maxBarHeight-barHeight
			char := " "
			if !empty {
				char = "█"
			}
			switch {
			case isCurrent:
				barLines[row] += barStyle.Render(char) + gap
			case !empty:
				barLines[row] += barStyle.Render(char) + gap
			default:
				barLines[row] += char + gap
			}
		}

		// Tool marker
		switch {
		case point.hasTools:
			icon := toolUsageIcon(point.toolCalls)
			toolMarkers = append(toolMarkers, lipgloss.NewStyle().Foreground(colorYellow).Render(icon)+gap)
		default:
			toolMarkers = append(toolMarkers, " "+gap)
		}

		// Message number (show every 5 or at current position)
		switch {
		case i%5 == 0 || isCurrent:
			msgNumbers = append(msgNumbers, deckMutedStyle.Render(strconv.Itoa(point.start+1))+gap)
		default:
			msgNumbers = append(msgNumbers, " "+gap)
		}
	}

	// Build the waveform display
	axisStyle := lipgloss.NewStyle().Foreground(colorBrightBlack)
	legendLines := []string{
		deckRoleUserStyle.Render("▇") + " user",
		deckRoleAsstStyle.Render("▇") + " assistant",
		lipgloss.NewStyle().Foreground(colorYellow).Render("⚙") + " tools",
		deckHighlightStyle.Render("▇") + " current",
	}
	for len(legendLines) < maxBarHeight {
		legendLines = append(legendLines, "")
	}
	barLineWidth := lipgloss.Width(barLines[0])
	for i := range maxBarHeight {
		label := ""
		switch i {
		case 0:
			label = labelTokens
		case 1:
			label = formatTokens(maxTokens)
		case maxBarHeight / 2:
			label = formatTokens(maxTokens / 2)
		case maxBarHeight - 1:
			label = "0"
		}
		axis := axisStyle.Render(padRight(label, axisWidth))
		legendLine := legendLines[i]
		legendWidth := lipgloss.Width(legendLine)
		legendPad := max(1, availWidth-axisWidth-barLineWidth-legendWidth)
		lines = append(lines, axis+barLines[i]+strings.Repeat(" ", legendPad)+legendLine)
	}
	toolLine := strings.Repeat(" ", axisWidth) + strings.Join(toolMarkers, "")
	numberLine := strings.Repeat(" ", axisWidth) + strings.Join(msgNumbers, "")

	lines = append(lines, "")
	lines = append(lines, toolLine)
	lines = append(lines, numberLine)

	// // Add full-width x-axis line (light gray)
	// xAxisStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#3A3A3A"))
	// axisLineWidth := max(availWidth-axisWidth, 0)
	// xAxis := xAxisStyle.Render(strings.Repeat("─", axisLineWidth))
	// lines = append(lines, strings.Repeat(" ", axisWidth)+xAxis)

	return lines
}

func (m deckModel) renderConversationTable(width, height int) []string {
	lines := []string{}

	header := deckSectionStyle.Render("conversation") + "  " +
		deckAccentStyle.Render("[s]") + deckMutedStyle.Render(" sort  ") +
		deckAccentStyle.Render("[↑↓]") + deckMutedStyle.Render(" navigate")
	if m.useGroupedConversations() {
		header += deckMutedStyle.Render("  (grouped)")
	}
	lines = append(lines, header)

	if m.detail == nil || len(m.detail.Messages) == 0 {
		lines = append(lines, deckMutedStyle.Render("no messages"))
		return padLines(lines, width, height)
	}

	if height < 3 {
		height = 3
	}

	if m.useGroupedConversations() {
		groups := m.sortedGroupedMessages()
		if len(groups) == 0 {
			lines = append(lines, deckMutedStyle.Render("no messages"))
			return padLines(lines, width, height)
		}

		// Column widths for conversation table
		const colNum = 4
		const colRole = 9
		const colTime = 9
		const colTokens = 9
		const colCost = 8
		const colDelta = 8

		// Table header
		headerRow := fitCell("#", colNum) +
			fitCell("role", colRole) +
			fitCell("time", colTime) +
			fitCellRight("tokens", colTokens) + " " +
			fitCellRight(sortKeyCost, colCost) + " " +
			fitCellRight("delta", colDelta)
		lines = append(lines, deckMutedStyle.Render(headerRow))

		maxVisible := max(height-2, 5)
		start, end, _ := stableVisibleRange(len(groups), m.messageCursor, maxVisible, max(m.messageCursor-(maxVisible/2), 0))

		for i := start; i < end; i++ {
			group := groups[i]
			cursor := "  "
			if i == m.messageCursor {
				cursor = "> "
			}

			roleText := roleUser
			roleStyle := deckRoleUserStyle
			if group.Role == roleAssistant {
				roleText = "asst"
				roleStyle = deckRoleAsstStyle
			}
			if group.Count > 1 {
				roleText = fmt.Sprintf("%s x%d", roleText, group.Count)
			}

			toolIndicator := ""
			if len(group.ToolCalls) > 0 {
				icon := toolUsageIcon(group.ToolCalls)
				toolIndicator = " " + lipgloss.NewStyle().Foreground(colorYellow).Render(icon)
			}

			msgNum := strconv.Itoa(group.StartIndex + 1)
			timeStr := group.StartTime.Format("15:04:05")
			tokensStr := formatTokensCompact(group.TotalTokens)
			costStr := formatCost(group.TotalCost)
			deltaStr := ""
			if group.Delta > 0 {
				deltaStr = formatDuration(group.Delta)
			}

			row := cursor +
				fitCell(msgNum, colNum) +
				padRightWithColor(roleStyle.Render(fitCell(roleText, colRole)), colRole) +
				fitCell(timeStr, colTime) +
				fitCellRight(tokensStr, colTokens) + " " +
				fitCellRight(costStr, colCost) + " " +
				fitCellRight(deltaStr, colDelta) +
				toolIndicator

			if i == m.messageCursor {
				row = deckHighlightStyle.Render(row)
			}

			lines = append(lines, row)
		}

		if len(groups) > maxVisible {
			position := fmt.Sprintf("showing %d-%d of %d", start+1, end, len(groups))
			lines = append(lines, "", deckMutedStyle.Render(position))
		}

		return padLines(lines, width, height)
	}

	messages := m.sortedMessages()
	if len(messages) == 0 {
		lines = append(lines, deckMutedStyle.Render("no messages"))
		return padLines(lines, width, height)
	}

	// Column widths for conversation table
	const colNum = 4
	const colRole = 9
	const colTime = 9
	const colTokens = 9
	const colCost = 8
	const colDelta = 8

	// Table header
	headerRow := fitCell("#", colNum) +
		fitCell("role", colRole) +
		fitCell("time", colTime) +
		fitCellRight("tokens", colTokens) + " " +
		fitCellRight(sortKeyCost, colCost) + " " +
		fitCellRight("delta", colDelta)
	lines = append(lines, deckMutedStyle.Render(headerRow))

	// Calculate visible range (show current message and surrounding context)
	maxVisible := max(height-2, 5) // Reserve space for header
	start, end, _ := stableVisibleRange(len(messages), m.messageCursor, maxVisible, max(m.messageCursor-(maxVisible/2), 0))

	// Render message rows
	for i := start; i < end; i++ {
		msg := messages[i]

		cursor := "  "
		if i == m.messageCursor {
			cursor = "> "
		}

		// Format role
		roleText := roleUser
		roleStyle := deckRoleUserStyle
		if msg.Role == roleAssistant {
			roleText = "asst"
			roleStyle = deckRoleAsstStyle
		}

		// Tool indicator
		toolIndicator := ""
		if len(msg.ToolCalls) > 0 {
			icon := toolUsageIcon(msg.ToolCalls)
			toolIndicator = " " + lipgloss.NewStyle().Foreground(colorYellow).Render(icon)
		}

		// Format row
		msgNum := strconv.Itoa(i + 1)
		timeStr := msg.Timestamp.Format("15:04:05")
		tokensStr := formatTokensCompact(msg.TotalTokens)
		costStr := formatCost(msg.TotalCost)
		deltaStr := ""
		if msg.Delta > 0 {
			deltaStr = formatDuration(msg.Delta)
		}

		row := cursor +
			fitCell(msgNum, colNum) +
			padRightWithColor(roleStyle.Render(fitCell(roleText, colRole)), colRole) +
			fitCell(timeStr, colTime) +
			fitCellRight(tokensStr, colTokens) + " " +
			fitCellRight(costStr, colCost) + " " +
			fitCellRight(deltaStr, colDelta) +
			toolIndicator

		if i == m.messageCursor {
			row = deckHighlightStyle.Render(row)
		}

		lines = append(lines, row)
	}

	// Show position indicator if not all messages are visible
	if len(messages) > maxVisible {
		position := fmt.Sprintf("showing %d-%d of %d", start+1, end, len(messages))
		lines = append(lines, "", deckMutedStyle.Render(position))
	}

	return padLines(lines, width, height)
}

func (m deckModel) renderMessageDetailPane(width, height int) []string {
	lines := []string{}

	// Ensure minimum width for box
	boxWidth := max(width-2, 20) // Account for borders

	// Box title with overlapping header
	title := " message detail "
	titleLen := len(title)
	leftDash := max(0, (boxWidth-titleLen)/2)
	rightDash := max(0, boxWidth-titleLen-leftDash)
	topBorder := deckDimStyle.Render("┌"+strings.Repeat("─", leftDash)) +
		deckMutedStyle.Render(title) +
		deckDimStyle.Render(strings.Repeat("─", rightDash)+"┐")
	lines = append(lines, topBorder)

	if m.detail == nil || len(m.detail.Messages) == 0 {
		emptyLine := deckDimStyle.Render("│") +
			padRight(" no message", boxWidth) +
			deckDimStyle.Render("│")
		lines = append(lines, emptyLine)
		bottomBorder := deckDimStyle.Render("└" + strings.Repeat("─", boxWidth) + "┘")
		lines = append(lines, bottomBorder)
		return padLines(lines, width, height)
	}

	if height < 3 {
		height = 3
	}

	messages := m.sortedMessages()
	if len(messages) == 0 {
		emptyLine := deckDimStyle.Render("│") +
			padRight(" no message", boxWidth) +
			deckDimStyle.Render("│")
		lines = append(lines, emptyLine)
		bottomBorder := deckDimStyle.Render("└" + strings.Repeat("─", boxWidth) + "┘")
		lines = append(lines, bottomBorder)
		return padLines(lines, width, height)
	}

	if m.useGroupedConversations() {
		groups := m.sortedGroupedMessages()
		if len(groups) == 0 {
			return padLines(lines, width, height)
		}
		group := groups[clamp(m.messageCursor, len(groups)-1)]
		contentLines := []string{}

		roleLabel := "User"
		roleStyle := deckRoleUserStyle
		if group.Role == roleAssistant {
			roleLabel = "Assistant"
			roleStyle = deckRoleAsstStyle
		}
		contentLines = append(contentLines, deckMutedStyle.Render("Role: ")+roleStyle.Render(roleLabel))

		timeInfo := "Time: " + group.StartTime.Format("15:04:05")
		if group.EndTime.After(group.StartTime) {
			timeInfo += " - " + group.EndTime.Format("15:04:05")
		}
		if group.Delta > 0 {
			timeInfo += fmt.Sprintf("  (+%s)", formatDuration(group.Delta))
		}
		contentLines = append(contentLines, timeInfo)
		if group.Count > 1 {
			contentLines = append(contentLines, deckMutedStyle.Render(fmt.Sprintf("Messages: %d", group.Count)))
		}
		contentLines = append(contentLines, "")

		contentLines = append(contentLines, deckMutedStyle.Render("Tokens: ")+fmt.Sprintf(
			"In %s  Out %s  Total %s",
			formatTokensDetail(group.InputTokens),
			formatTokensDetail(group.OutputTokens),
			formatTokensDetail(group.TotalTokens),
		))
		contentLines = append(contentLines, deckMutedStyle.Render("Cost:   ")+fmt.Sprintf(
			"In %s  Out %s  Total %s",
			formatCost(group.InputCost),
			formatCost(group.OutputCost),
			deckAccentStyle.Render(formatCost(group.TotalCost)),
		))
		contentLines = append(contentLines, "")

		if len(group.ToolCalls) > 0 {
			contentLines = append(contentLines, deckMutedStyle.Render("Tools:"))
			toolsList := strings.Join(group.ToolCalls, ", ")
			wrappedTools := wrapText(toolsList, max(20, boxWidth-4))
			for _, line := range wrappedTools {
				contentLines = append(contentLines, "  "+lipgloss.NewStyle().Foreground(colorYellow).Render(line))
			}
			contentLines = append(contentLines, "")
		}

		text := strings.TrimSpace(group.Text)
		if text != "" {
			contentLines = append(contentLines, deckMutedStyle.Render("Message:"))
			wrappedText := wrapText(text, max(20, boxWidth-4))
			maxPreview := max(height-len(contentLines)-4, 3)
			for i, line := range wrappedText {
				if i >= maxPreview {
					contentLines = append(contentLines, deckMutedStyle.Render("  ..."))
					break
				}
				contentLines = append(contentLines, "  "+line)
			}
		}

		maxContentWidth := boxWidth - 2
		maxContentLines := max(height-2, 0)
		if maxContentLines > 0 && len(contentLines) > maxContentLines {
			contentLines = append(contentLines[:maxContentLines-1], deckMutedStyle.Render("  ..."))
		}
		for _, contentLine := range contentLines {
			visualWidth := lipgloss.Width(contentLine)
			if visualWidth > maxContentWidth {
				contentLine = truncateString(contentLine, maxContentWidth-3) + "..."
				visualWidth = lipgloss.Width(contentLine)
			}
			padding := ""
			if visualWidth < maxContentWidth {
				padding = strings.Repeat(" ", maxContentWidth-visualWidth)
			}
			boxedLine := deckDimStyle.Render("│") + " " + contentLine + padding + " " + deckDimStyle.Render("│")
			lines = append(lines, boxedLine)
		}

		for len(lines) < height-1 {
			emptyLine := deckDimStyle.Render("│") + strings.Repeat(" ", boxWidth) + deckDimStyle.Render("│")
			lines = append(lines, emptyLine)
		}

		bottomBorder := deckDimStyle.Render("└" + strings.Repeat("─", boxWidth) + "┘")
		lines = append(lines, bottomBorder)
		return lines
	}

	msg := messages[m.messageCursor]
	contentLines := []string{}

	// Role
	roleLabel := "User"
	roleStyle := deckRoleUserStyle
	if msg.Role == roleAssistant {
		roleLabel = "Assistant"
		roleStyle = deckRoleAsstStyle
	}
	contentLines = append(contentLines, deckMutedStyle.Render("Role: ")+roleStyle.Render(roleLabel))

	// Time and delta
	timeInfo := "Time: " + msg.Timestamp.Format("15:04:05")
	if msg.Delta > 0 {
		timeInfo += fmt.Sprintf("  (+%s)", formatDuration(msg.Delta))
	}
	contentLines = append(contentLines, timeInfo)
	contentLines = append(contentLines, "")

	// Token + cost breakdown (inline to save vertical space)
	contentLines = append(contentLines, deckMutedStyle.Render("Tokens: ")+
		fmt.Sprintf("In %s  Out %s  Total %s", formatTokensDetail(msg.InputTokens), formatTokensDetail(msg.OutputTokens), formatTokensDetail(msg.TotalTokens)))
	contentLines = append(contentLines, deckMutedStyle.Render("Cost:   ")+
		fmt.Sprintf("In %s  Out %s  Total %s", formatCost(msg.InputCost), formatCost(msg.OutputCost), deckAccentStyle.Render(formatCost(msg.TotalCost))))
	contentLines = append(contentLines, "")

	// Tools
	if len(msg.ToolCalls) > 0 {
		contentLines = append(contentLines, deckMutedStyle.Render("Tools:"))
		toolsList := strings.Join(msg.ToolCalls, ", ")
		wrappedTools := wrapText(toolsList, max(20, boxWidth-4))
		for _, line := range wrappedTools {
			contentLines = append(contentLines, "  "+lipgloss.NewStyle().Foreground(colorYellow).Render(line))
		}
		contentLines = append(contentLines, "")
	}

	// Message preview
	text := strings.TrimSpace(msg.Text)
	if text != "" {
		contentLines = append(contentLines, deckMutedStyle.Render("Message:"))
		wrappedText := wrapText(text, max(20, boxWidth-4))
		// Show first few lines of message
		maxPreview := max(height-len(contentLines)-4, 3)
		for i, line := range wrappedText {
			if i >= maxPreview {
				contentLines = append(contentLines, deckMutedStyle.Render("  ..."))
				break
			}
			contentLines = append(contentLines, "  "+line)
		}
	}

	// Wrap each content line in box borders
	maxContentWidth := boxWidth - 2 // Account for 1 space padding on each side
	maxContentLines := max(height-2, 0)
	if maxContentLines > 0 && len(contentLines) > maxContentLines {
		contentLines = append(contentLines[:maxContentLines-1], deckMutedStyle.Render("  ..."))
	}
	for _, contentLine := range contentLines {
		visualWidth := lipgloss.Width(contentLine)

		// Truncate if too long
		if visualWidth > maxContentWidth {
			// Simple truncation - just cut to fit and add ellipsis
			contentLine = truncateString(contentLine, maxContentWidth-3) + "..."
			visualWidth = lipgloss.Width(contentLine)
		}

		padding := ""
		if visualWidth < maxContentWidth {
			padding = strings.Repeat(" ", maxContentWidth-visualWidth)
		}
		boxedLine := deckDimStyle.Render("│") + " " + contentLine + padding + " " + deckDimStyle.Render("│")
		lines = append(lines, boxedLine)
	}

	// Fill remaining space with empty boxed lines
	for len(lines) < height-1 {
		emptyLine := deckDimStyle.Render("│") + strings.Repeat(" ", boxWidth) + deckDimStyle.Render("│")
		lines = append(lines, emptyLine)
	}

	// Bottom border
	bottomBorder := deckDimStyle.Render("└" + strings.Repeat("─", boxWidth) + "┘")
	lines = append(lines, bottomBorder)

	return lines
}

func formatTokensCompact(tokens int64) string {
	if tokens >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(tokens)/1_000.0)
	}
	return strconv.FormatInt(tokens, 10)
}
