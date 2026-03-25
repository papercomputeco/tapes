package deckcmder

import (
	"fmt"
	"image/color"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/papercomputeco/tapes/pkg/deck"
)

func countWrappedLines(s string, width int) int {
	if s == "" {
		return 0
	}
	if width <= 0 {
		width = 80
	}
	lines := strings.Split(s, "\n")
	count := 0
	for _, line := range lines {
		lineWidth := lipgloss.Width(line)
		if lineWidth == 0 {
			count++
			continue
		}
		rows := lineWidth / width
		if lineWidth%width != 0 {
			rows++
		}
		count += max(rows, 1)
	}
	return count
}

// overviewChrome renders all overview sections except the session list and
// returns the joined string plus the total line count (including blank
// separator lines and footer).
func (m deckModel) overviewChrome() (above string, footer string) {
	allSessions := m.overview.Sessions

	headerLeft := deckTitleStyle.Render("tapes deck")
	cassetteLines := renderCassetteTape()

	lines := make([]string, 0, 10)

	if m.metricsReady && m.overviewStats != nil {
		stats := *m.overviewStats

		lastWindow := formatDuration(stats.TotalDuration)
		filteredCount := len(allSessions)
		if term := strings.TrimSpace(m.searchInput.Value()); term != "" {
			filteredCount = len(m.filteredSessions())
		}
		isFiltered := filteredCount != len(allSessions)
		sessionCount := deckMutedStyle.Render(m.headerSessionCount(lastWindow, filteredCount, len(allSessions), isFiltered))

		header1 := renderHeaderLine(m.width, headerLeft, cassetteLines[0])
		header2 := renderHeaderLine(m.width, "", cassetteLines[1])
		header3 := renderHeaderLine(m.width, sessionCount, cassetteLines[2])

		metrics := m.viewMetrics(stats)
		costByModel := m.viewCostByModel(stats)

		lines = append(lines, header1, header2, header3, renderRule(m.width), "")
		lines = append(lines, metrics)
		lines = append(lines, "", costByModel, "")
	} else {
		sessionCount := deckMutedStyle.Render(fmt.Sprintf("%d sessions", len(allSessions)))

		header1 := renderHeaderLine(m.width, headerLeft, cassetteLines[0])
		header2 := renderHeaderLine(m.width, "", cassetteLines[1])
		header3 := renderHeaderLine(m.width, sessionCount, cassetteLines[2])

		loadingLine := m.spinner.View() + " loading metrics..."

		lines = append(lines, header1, header2, header3, renderRule(m.width), "")
		lines = append(lines, deckMutedStyle.Render(loadingLine), "")
	}

	return strings.Join(lines, "\n"), m.viewFooter()
}

// sessionListHeight returns the number of rows available for the session list
// in the current terminal, based on the actual rendered chrome height.
func (m deckModel) sessionListHeight() int {
	if m.overview == nil {
		return max(m.height-31, 5) // fallback
	}
	above, footer := m.overviewChrome()
	// +1 for the blank line between session list and footer
	// +2*verticalPadding for the outer padding added by addPadding
	chromeLines := countWrappedLines(above, m.width) + countWrappedLines(footer, m.width) + 1 + 2*verticalPadding
	return max(m.height-chromeLines, 5)
}

func (m deckModel) viewOverview() string {
	if m.overview == nil {
		loading := m.spinner.View() + " loading sessions..."
		return deckMutedStyle.Render(loading)
	}

	above, footer := m.overviewChrome()
	chromeLines := countWrappedLines(above, m.width) + countWrappedLines(footer, m.width) + 1 + 2*verticalPadding
	availableHeight := max(m.height-chromeLines, 5)

	return above + m.viewSessionList(availableHeight) + "\n\n" + footer
}

func (m deckModel) viewMetrics(stats deckOverviewStats) string {
	// Period selector header with box background for active
	periodLabel := periodToLabel(m.timePeriod)
	periods := []string{"24h", "30d"}
	periodParts := []string{}
	for _, p := range periods {
		if p == periodLabel {
			// Active period with filled background
			periodParts = append(periodParts, deckHighlightStyle.Render(" "+p+" "))
		} else {
			// Inactive period
			periodParts = append(periodParts, deckMutedStyle.Render(p))
		}
	}
	periodSelector := strings.Join(periodParts, "  ") + "  " + deckDimStyle.Render("(p to change)")

	lines := []string{periodSelector, ""}

	// Calculate metrics
	avgCost := safeDivide(stats.TotalCost, float64(max(1, stats.TotalSessions)))
	avgTime := time.Duration(int64(stats.TotalDuration) / int64(max(1, stats.TotalSessions)))
	avgTools := stats.TotalToolCalls / max(1, stats.TotalSessions)

	// Prepare metric data with comparisons
	type metricData struct {
		label      string
		value      string
		change     string
		changeIcon string
		isPositive bool
	}

	metrics := []metricData{
		{
			label: "TOTAL SPEND",
			value: formatCost(stats.TotalCost),
		},
		{
			label: "TOKENS USED",
			value: fmt.Sprintf("%s in / %s out", formatTokens(stats.InputTokens), formatTokens(stats.OutputTokens)),
		},
		{
			label: "AGENT TIME",
			value: formatDuration(stats.TotalDuration),
		},
		{
			label: "TOOL CALLS",
			value: strconv.Itoa(stats.TotalToolCalls),
		},
		{
			label: "SUCCESS RATE",
			value: formatPercent(stats.SuccessRate),
		},
	}

	// Add comparison data if available
	if m.overview != nil && m.overview.PreviousPeriod != nil {
		prev := m.overview.PreviousPeriod

		// Cost comparison
		if prev.TotalCost > 0 {
			change := ((stats.TotalCost - prev.TotalCost) / prev.TotalCost) * 100
			metrics[0].change = fmt.Sprintf("%.1f%%", abs(change))
			metrics[0].changeIcon = changeArrow(change)
			metrics[0].isPositive = change < 0 // Lower cost is better
		}

		// Tokens comparison
		prevTokens := prev.TotalTokens
		currTokens := stats.InputTokens + stats.OutputTokens
		if prevTokens > 0 {
			change := ((float64(currTokens) - float64(prevTokens)) / float64(prevTokens)) * 100
			metrics[1].change = fmt.Sprintf("%.1f%%", abs(change))
			metrics[1].changeIcon = changeArrow(change)
			metrics[1].isPositive = change > 0 // More tokens means more usage
		}

		// Duration comparison
		if prev.TotalDuration > 0 {
			change := ((float64(stats.TotalDuration) - float64(prev.TotalDuration)) / float64(prev.TotalDuration)) * 100
			metrics[2].change = fmt.Sprintf("%.1f%%", abs(change))
			metrics[2].changeIcon = changeArrow(change)
			metrics[2].isPositive = change > 0 // More time means more work
		}

		// Tool calls comparison
		if prev.TotalToolCalls > 0 {
			change := ((float64(stats.TotalToolCalls) - float64(prev.TotalToolCalls)) / float64(prev.TotalToolCalls)) * 100
			metrics[3].change = fmt.Sprintf("%.1f%%", abs(change))
			metrics[3].changeIcon = changeArrow(change)
			metrics[3].isPositive = change > 0
		}

		// Success rate comparison
		if prev.SuccessRate > 0 {
			change := ((stats.SuccessRate - prev.SuccessRate) / prev.SuccessRate) * 100
			metrics[4].change = fmt.Sprintf("%.1f%%", abs(change))
			metrics[4].changeIcon = changeArrow(change)
			metrics[4].isPositive = change > 0 // Higher success is better
		}
	}

	// Render metrics in a grid
	cols := len(metrics)
	if cols == 0 {
		return strings.Join(lines, "\n")
	}

	lineWidth := m.width
	if lineWidth <= 0 {
		lineWidth = 80
	}

	spaceWidth := (cols - 1) * 3
	colWidth := max((lineWidth-spaceWidth)/cols, 16)

	// Label style with more contrast and bold value style
	labelStyle := lipgloss.NewStyle().Foreground(colorLabel).Bold(true)
	highlightValueStyle := lipgloss.NewStyle().Foreground(colorForeground).Bold(true)
	dimSeparator := deckDimStyle.Render(" │ ")

	// Render labels with separators
	labels := make([]string, 0, cols)
	for i, metric := range metrics {
		labels = append(labels, labelStyle.Render(fitCell(metric.label, colWidth)))
		if i < cols-1 {
			labels = append(labels, dimSeparator)
		}
	}
	lines = append(lines, strings.Join(labels, ""))

	// Render values with separators
	values := make([]string, 0, cols)
	for i, metric := range metrics {
		values = append(values, highlightValueStyle.Render(fitCell(metric.value, colWidth)))
		if i < cols-1 {
			values = append(values, dimSeparator)
		}
	}
	lines = append(lines, strings.Join(values, ""))

	// Render comparisons with color only on arrow
	if m.overview != nil && m.overview.PreviousPeriod != nil {
		lightGrayStyle := lipgloss.NewStyle().Foreground(colorBrightBlack)
		comparisons := make([]string, 0, cols)
		for i, metric := range metrics {
			if metric.change != "" {
				var arrowStyle lipgloss.Style
				if metric.isPositive {
					arrowStyle = deckStatusOKStyle
				} else {
					arrowStyle = deckStatusFailStyle
				}
				// Color only the arrow, rest is light gray
				comp := arrowStyle.Render(metric.changeIcon) + " " + lightGrayStyle.Render(metric.change+" vs prev")
				comparisons = append(comparisons, fitCell(comp, colWidth))
			} else {
				comparisons = append(comparisons, deckMutedStyle.Render(fitCell("—", colWidth)))
			}
			if i < cols-1 {
				comparisons = append(comparisons, dimSeparator)
			}
		}
		lines = append(lines, strings.Join(comparisons, ""))
	}

	// Add average row (no blank line before, pull closer)
	avgValues := []string{
		formatCost(avgCost) + " avg",
		fmt.Sprintf("%s / %s avg", formatTokens(avgTokenCount(stats.InputTokens, stats.TotalSessions)), formatTokens(avgTokenCount(stats.OutputTokens, stats.TotalSessions))),
		formatDuration(avgTime) + " avg",
		fmt.Sprintf("%d avg", avgTools),
		fmt.Sprintf("%d/%d complete", stats.Completed, stats.TotalSessions),
	}
	avgLine := make([]string, 0)
	for i, val := range avgValues {
		avgLine = append(avgLine, deckMutedStyle.Render(fitCell(val, colWidth)))
		if i < cols-1 {
			avgLine = append(avgLine, dimSeparator)
		}
	}
	lines = append(lines, deckMutedStyle.Render(strings.Join(avgLine, "")))

	return strings.Join(lines, "\n")
}

func (m deckModel) viewCostByModel(stats deckOverviewStats) string {
	if len(stats.CostByModel) == 0 {
		return deckMutedStyle.Render("cost by model: no data")
	}

	// Calculate chart dimensions dynamically based on available width
	gap := 4
	availableWidth := m.width

	// Ensure minimum total width
	minTotalWidth := 100
	if availableWidth < minTotalWidth {
		availableWidth = minTotalWidth
	}

	// Split available width between charts (40% cost, 60% status)
	costChartWidth := (availableWidth - gap) * 2 / 5
	statusChartWidth := availableWidth - gap - costChartWidth

	// Cost by model chart
	costLines := m.renderCostByModelChart(stats, costChartWidth)

	// Status chart
	statusLines := m.renderStatusPieChart(stats, statusChartWidth)

	// Combine side by side with gap
	combined := joinColumns(costLines, statusLines, gap)

	return strings.Join(combined, "\n")
}

func (m deckModel) renderCostByModelChart(stats deckOverviewStats, width int) []string {
	// Ensure minimum width
	minWidth := 40
	if width < minWidth {
		width = minWidth
	}

	maxCost := 0.0
	for _, cost := range stats.CostByModel {
		if cost.TotalCost > maxCost {
			maxCost = cost.TotalCost
		}
	}

	// Create box with overlapping title
	title := " cost by model "
	titleLen := len(title)
	leftDash := max(0, (width-titleLen)/2)
	rightDash := max(0, width-titleLen-leftDash)
	topBorder := deckDimStyle.Render("┌"+strings.Repeat("─", leftDash)) + deckMutedStyle.Render(title) + deckDimStyle.Render(strings.Repeat("─", rightDash)+"┐")
	costs := sortedModelCosts(stats.CostByModel)
	if len(costs) > maxCostByModelEntries {
		costs = costs[:maxCostByModelEntries]
	}
	lines := make([]string, 0, len(costs)+2)
	lines = append(lines, topBorder)

	barWidth := 15 // Bar width for cost visualization

	for _, cost := range costs {
		bar := renderBar(cost.TotalCost, maxCost, barWidth)
		// Use model color for the bar
		modelColor := getModelColor(cost.Model)
		coloredBar := lipgloss.NewStyle().Foreground(modelColor).Render(bar)

		line := fmt.Sprintf(" %-17s %s %s %d", cost.Model, coloredBar, formatCost(cost.TotalCost), cost.SessionCount)
		// Calculate padding to fill the width
		contentWidth := lipgloss.Width(line)
		paddingNeeded := width - contentWidth
		paddingNeeded = max(paddingNeeded, 0)
		paddedLine := line + strings.Repeat(" ", paddingNeeded)
		lines = append(lines, deckDimStyle.Render("│")+paddedLine+deckDimStyle.Render("│"))
	}

	// Add empty line for spacing
	lines = append(lines, deckDimStyle.Render("│"+strings.Repeat(" ", width)+"│"))

	bottomBorder := deckDimStyle.Render("└" + strings.Repeat("─", width) + "┘")
	lines = append(lines, bottomBorder)

	return lines
}

func (m deckModel) renderStatusPieChart(stats deckOverviewStats, width int) []string {
	// Ensure minimum width
	minWidth := 55
	if width < minWidth {
		width = minWidth
	}

	// Calculate percentages
	completedPct := float64(stats.Completed) / float64(stats.TotalSessions) * 100
	failed := countByStatusInStats(stats, deck.StatusFailed)
	abandoned := countByStatusInStats(stats, deck.StatusAbandoned)
	failedPct := float64(failed) / float64(stats.TotalSessions) * 100
	abandonedPct := float64(abandoned) / float64(stats.TotalSessions) * 100

	// Mock efficiency data
	efficiency := struct {
		perSession float64
		perMinute  float64
		tokPerMin  int
	}{
		perSession: 0.038,
		perMinute:  0.001,
		tokPerMin:  34,
	}

	// Create box
	title := " session status "
	titleLen := len(title)
	leftDash := max(0, (width-titleLen)/2)
	rightDash := max(0, width-titleLen-leftDash)
	topBorder := deckDimStyle.Render("┌"+strings.Repeat("─", leftDash)) + deckMutedStyle.Render(title) + deckDimStyle.Render(strings.Repeat("─", rightDash)+"┐")
	lines := make([]string, 0, 7)
	lines = append(lines, topBorder)

	// Horizontal bar visualization
	barWidth := width - 2 // Account for 1 space padding on each side
	completedWidth := int(float64(barWidth) * completedPct / 100)
	failedWidth := int(float64(barWidth) * failedPct / 100)
	abandonedWidth := barWidth - completedWidth - failedWidth

	bar := deckStatusOKStyle.Render(strings.Repeat("█", completedWidth)) +
		deckStatusFailStyle.Render(strings.Repeat("█", failedWidth)) +
		deckStatusWarnStyle.Render(strings.Repeat("█", abandonedWidth))

	lines = append(lines, deckDimStyle.Render("│")+" "+bar+" "+deckDimStyle.Render("│"))
	lines = append(lines, deckDimStyle.Render("│"+strings.Repeat(" ", width)+"│"))

	// Status breakdown - all on one line horizontally
	legendLine := fmt.Sprintf(" %s completed %2.0f%% (%d)  %s failed %2.0f%% (%d)  %s abandoned %2.0f%% (%d)",
		deckStatusOKStyle.Render("●"), completedPct, stats.Completed,
		deckStatusFailStyle.Render("●"), failedPct, failed,
		deckStatusWarnStyle.Render("●"), abandonedPct, abandoned)

	lines = append(lines,
		deckDimStyle.Render("│")+legendLine+strings.Repeat(" ", max(0, width-lipgloss.Width(legendLine)))+deckDimStyle.Render("│"),
		deckDimStyle.Render("│"+strings.Repeat(" ", width)+"│"),
	)

	// Efficiency metrics - simplified to fit
	efficiencyLine := fmt.Sprintf(" %s %s/sess  %d tok/m",
		deckMutedStyle.Render("eff:"),
		formatCost(efficiency.perSession),
		efficiency.tokPerMin)

	lines = append(lines, deckDimStyle.Render("│")+efficiencyLine+strings.Repeat(" ", max(0, width-lipgloss.Width(efficiencyLine)))+deckDimStyle.Render("│"))

	bottomBorder := deckDimStyle.Render("└" + strings.Repeat("─", width) + "┘")
	lines = append(lines, bottomBorder)

	return lines
}

func getModelColor(model string) color.Color {
	modelLower := strings.ToLower(model)

	// Check for Claude models
	for tier, colorValue := range claudeColors {
		if strings.Contains(modelLower, tier) {
			return lipgloss.Color(colorValue)
		}
	}

	// Check for OpenAI models
	for modelName, colorValue := range openaiColors {
		if strings.Contains(modelLower, modelName) || strings.Contains(modelLower, strings.ReplaceAll(modelName, "-", "")) {
			return lipgloss.Color(colorValue)
		}
	}

	// Check for Google models
	for modelName, colorValue := range googleColors {
		if strings.Contains(modelLower, modelName) || strings.Contains(modelLower, strings.ReplaceAll(modelName, "-", "")) {
			return lipgloss.Color(colorValue)
		}
	}

	return colorRed // Default to primary accent
}

func countByStatusInStats(stats deckOverviewStats, status string) int {
	switch status {
	case deck.StatusFailed:
		return stats.Failed
	case deck.StatusAbandoned:
		return stats.Abandoned
	default:
		return 0
	}
}

func (m deckModel) viewSessionList(availableHeight int) string {
	sessions := m.filteredSessions()

	status := m.filters.Status
	if status == "" {
		status = "all"
	}
	sortDir := m.filters.SortDir
	if sortDir == "" {
		sortDir = sortDirDesc
	}

	// Action buttons
	sortBtn := deckAccentStyle.Render("[s]") + " sort"
	filterBtn := deckAccentStyle.Render("[f]") + " filter"
	searchBtn := deckAccentStyle.Render("[/]") + " search"
	actions := "  " + sortBtn + "  " + filterBtn + "  " + searchBtn

	// Build session header with search indicator
	sessionHeader := fmt.Sprintf("sessions (sort: %s %s, status: %s)", m.filters.Sort, sortDir, status)
	if m.searchActive {
		actions = "  " + m.searchInput.View()
	} else if m.searchInput.Value() != "" {
		searchLabel := deckAccentStyle.Render("search:") + " " + m.searchInput.Value()
		actions = "  " + searchLabel + "  " + sortBtn + "  " + filterBtn + "  " + searchBtn
	}

	if len(sessions) == 0 {
		lines := []string{
			deckSectionStyle.Render(sessionHeader) + actions,
			renderRule(m.width),
		}
		if m.searchInput.Value() != "" {
			lines = append(lines, deckMutedStyle.Render("no sessions found: "+m.searchInput.Value()))
		} else {
			lines = append(lines, deckMutedStyle.Render("sessions: no data"))
		}
		return strings.Join(lines, "\n")
	}

	visibleRows := sessionListVisibleRows(len(sessions), availableHeight)
	// Calculate which sessions to show using stable scrolling
	start, end, _ := stableVisibleRange(len(sessions), m.cursor, visibleRows, m.scrollOffset)
	maxVisible := end - start

	lines := []string{
		deckSectionStyle.Render(sessionHeader) + actions,
		renderRule(m.width),
	}

	// Calculate column widths based on actual content
	type rowData struct {
		label        string
		project      string
		date         string
		model        string
		modelColored string
		dur          string
		tokens       string
		barbell      string // Cost-weighted barbell visualization
		costInd      string
		cost         string
		costRaw      string
		tools        string
		msgs         string
		statusCircle string
		statusText   string
	}

	rows := make([]rowData, maxVisible)

	// Determine if any session has a project set
	hasProject := false
	for _, session := range sessions {
		if session.Project != "" {
			hasProject = true
			break
		}
	}

	// Column width tracking
	maxProjectW := 0
	if hasProject {
		maxProjectW = len("project")
	}
	maxLabelW := len("label")
	maxDateW := len("date")
	maxModelW := len("model")
	maxDurW := len("dur")
	maxTokensW := len("tokens")
	maxBarbellW := 7 // Fixed width for barbell (e.g., "⬤──—●")
	maxCostIndW := 0 // Calculate from actual data
	maxCostW := len(sortKeyCost)
	maxToolsW := len("tools")
	maxMsgsW := len("msgs")
	maxStatusW := len("status")

	// First pass: collect data and measure widths
	for i := start; i < end; i++ {
		session := sessions[i]
		rowIdx := i - start

		rows[rowIdx].label = session.Label
		rows[rowIdx].project = session.Project
		rows[rowIdx].date = session.StartTime.Format("Jan 02'06")
		rows[rowIdx].model = session.Model
		rows[rowIdx].modelColored = colorizeModel(session.Model)
		rows[rowIdx].dur = formatDurationMinutes(session.Duration)
		rows[rowIdx].tokens = formatTokens(session.InputTokens + session.OutputTokens)
		rows[rowIdx].barbell = renderCostWeightedBarbell(session.InputTokens, session.OutputTokens, session.InputCost, session.OutputCost, m.overview.Sessions)
		rows[rowIdx].costInd = formatCostIndicator(session.TotalCost, m.overview.Sessions)
		rows[rowIdx].costRaw = formatCost(session.TotalCost)
		rows[rowIdx].cost = formatCostWithScale(session.TotalCost, m.overview.Sessions)
		rows[rowIdx].tools = strconv.Itoa(session.ToolCalls)
		rows[rowIdx].msgs = strconv.Itoa(session.MessageCount)
		rows[rowIdx].statusCircle, rows[rowIdx].statusText = formatStatusWithCircle(session.Status)

		// Measure widths (without ANSI codes for models/status)
		if len(rows[rowIdx].label) > maxLabelW {
			maxLabelW = len(rows[rowIdx].label)
		}
		if hasProject && len(rows[rowIdx].project) > maxProjectW {
			maxProjectW = len(rows[rowIdx].project)
		}
		if len(rows[rowIdx].date) > maxDateW {
			maxDateW = len(rows[rowIdx].date)
		}
		if len(rows[rowIdx].model) > maxModelW {
			maxModelW = len(rows[rowIdx].model)
		}
		durWidth := lipgloss.Width(rows[rowIdx].dur)
		if durWidth > maxDurW {
			maxDurW = durWidth
		}
		if len(rows[rowIdx].tokens) > maxTokensW {
			maxTokensW = len(rows[rowIdx].tokens)
		}
		// Measure cost indicator width (strip ANSI codes)
		costIndWidth := lipgloss.Width(rows[rowIdx].costInd)
		if costIndWidth > maxCostIndW {
			maxCostIndW = costIndWidth
		}
		costWidth := lipgloss.Width(rows[rowIdx].cost)
		if costWidth > maxCostW {
			maxCostW = costWidth
		}
		if len(rows[rowIdx].tools) > maxToolsW {
			maxToolsW = len(rows[rowIdx].tools)
		}
		if len(rows[rowIdx].msgs) > maxMsgsW {
			maxMsgsW = len(rows[rowIdx].msgs)
		}
		if len(session.Status) > maxStatusW {
			maxStatusW = len(session.Status)
		}
	}

	// Calculate total width used by fixed columns (excluding label)
	// Format: "  " + rowNum + " " + label + [gap + project] + gap + model + gap + dur + gap + tokens + gap + barbell + gap + costInd + " " + cost + gap + tools + gap + msgs + gap + status
	colGap := 3
	statusColW := 2 + maxStatusW // circle + space + text
	fixedWidth := 2 + 1 + 1 + colGap + maxDateW + colGap + maxModelW + colGap + maxDurW + colGap + maxTokensW + colGap + maxBarbellW + colGap + maxCostIndW + 1 + maxCostW + colGap + maxToolsW + colGap + maxMsgsW + colGap + statusColW
	if hasProject {
		fixedWidth += colGap + maxProjectW
	}

	// Cap label column width to avoid excessive whitespace
	availableLabelWidth := m.width - fixedWidth
	labelCap := min(maxLabelW, 36)
	if availableLabelWidth > labelCap {
		maxLabelW = labelCap
	} else if availableLabelWidth > maxLabelW {
		maxLabelW = availableLabelWidth
	}

	// Render header with calculated widths and sort indicator
	sortIndicator := ""
	if m.filters.Sort != "" {
		if strings.EqualFold(m.filters.SortDir, "asc") {
			sortIndicator = " ↑"
		} else {
			sortIndicator = " ↓"
		}
	}

	headerParts := []string{
		"  " + padRight("label", maxLabelW),
	}
	headerParts = append(headerParts,
		padRight("model", maxModelW),
		padRight("dur", maxDurW),
		padRight("tokens", maxTokensW),
		padRight("in / out", maxBarbellW), // Barbell visualization column
		padRight(sortKeyCost+func() string {
			if m.filters.Sort == sortKeyCost {
				return sortIndicator
			}
			return ""
		}(), maxCostIndW+1+maxCostW),
		padRight("tools", maxToolsW),
		padRight("msgs", maxMsgsW),
		padRight("status", statusColW),
		padRight("date", maxDateW),
	)
	if hasProject {
		headerParts = append(headerParts, padRight("project", maxProjectW))
	}
	lines = append(lines, deckMutedStyle.Render(strings.Join(headerParts, strings.Repeat(" ", colGap))))

	// Second pass: render rows with consistent widths
	for i := start; i < end; i++ {
		rowIdx := i - start
		rowNum := fmt.Sprintf("%02x", i+1)

		// Build row with proper padding
		// Pad cost indicator to ensure alignment
		costIndPadded := padRightWithColor(rows[rowIdx].costInd, maxCostIndW)
		barbellPadded := padRightWithColor(rows[rowIdx].barbell, maxBarbellW)
		costPadded := padRightWithColor(rows[rowIdx].cost, maxCostW)

		parts := []string{
			deckDimStyle.Render(rowNum) + " " + padRight(rows[rowIdx].label, maxLabelW),
		}
		parts = append(parts,
			padRightWithColor(rows[rowIdx].modelColored, maxModelW),
			padRight(rows[rowIdx].dur, maxDurW),
			padRight(rows[rowIdx].tokens, maxTokensW),
			barbellPadded, // Cost-weighted barbell visualization
			costIndPadded+" "+costPadded,
			padRight(rows[rowIdx].tools, maxToolsW),
			padRight(rows[rowIdx].msgs, maxMsgsW),
			padRightWithColor(rows[rowIdx].statusCircle+" "+rows[rowIdx].statusText, statusColW),
			deckMutedStyle.Render(padRight(rows[rowIdx].date, maxDateW)),
		)
		if hasProject {
			parts = append(parts, deckMutedStyle.Render(padRight(rows[rowIdx].project, maxProjectW)))
		}

		line := strings.Join(parts, strings.Repeat(" ", colGap))

		// Add cursor marker for selected row
		if i == m.cursor {
			line = deckHighlightStyle.Render(">" + line)
		} else {
			line = " " + line
		}

		lines = append(lines, line)
	}

	// Show position indicator if not all sessions are visible
	totalSessions := len(sessions)
	if totalSessions > maxVisible {
		position := fmt.Sprintf("showing %d-%d of %d", start+1, end, totalSessions)
		if m.searchInput.Value() != "" {
			position += fmt.Sprintf(" (filtered from %d)", len(m.overview.Sessions))
		}
		lines = append(lines, "", deckMutedStyle.Render(position))
	}

	return strings.Join(lines, "\n")
}
