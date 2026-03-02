package deckcmder

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/papercomputeco/tapes/pkg/deck"
)

func (m deckModel) viewAnalytics() string {
	if m.analytics == nil {
		w := m.width
		if w <= 0 {
			w = 80
		}
		cassetteLines := renderCassetteTape()
		header1 := renderHeaderLine(w, deckTitleStyle.Render("tapes deck"), cassetteLines[0])
		header2 := renderHeaderLine(w, "", cassetteLines[1])
		header3 := renderHeaderLine(w, deckMutedStyle.Render("loading..."), cassetteLines[2])

		lines := make([]string, 0, 8)
		lines = append(lines, header1, header2, header3, renderRule(w))
		lines = append(lines, "")
		lines = append(lines, renderAnalyticsSectionHeader("session analytics", w))
		lines = append(lines, "")
		lines = append(lines, "  "+m.spinner.View()+" "+deckMutedStyle.Render("analyzing sessions..."))

		return strings.Join(lines, "\n")
	}

	content := m.buildAnalyticsContent()
	w := m.width
	if w <= 0 {
		w = 80
	}

	footer := renderRule(w) + "\n" + m.viewAnalyticsFooter()
	footerLines := strings.Count(footer, "\n") + 1

	visibleHeight := m.height - 2*verticalPadding - footerLines
	if visibleHeight <= 0 {
		visibleHeight = 20
	}
	maxScroll := max(0, len(content)-visibleHeight)
	if m.analyticsScroll > maxScroll {
		m.analyticsScroll = maxScroll
	}
	start := m.analyticsScroll
	end := min(start+visibleHeight, len(content))

	return strings.Join(content[start:end], "\n") + "\n" + footer
}

func (m deckModel) buildAnalyticsContent() []string {
	a := m.analytics
	w := m.width
	if w <= 0 {
		w = 80
	}

	// Header — same style as overview
	cassetteLines := renderCassetteTape()
	header1 := renderHeaderLine(w, deckTitleStyle.Render("tapes deck"), cassetteLines[0])
	header2 := renderHeaderLine(w, "", cassetteLines[1])
	subtitle := deckMutedStyle.Render(fmt.Sprintf("%d sessions analyzed", a.TotalSessions))
	header3 := renderHeaderLine(w, subtitle, cassetteLines[2])

	lines := make([]string, 0, 64)
	lines = append(lines, header1, header2, header3, renderRule(w))

	// Title + period selector
	periodLabel := periodToLabel(m.timePeriod)
	periods := []string{"30d", "3M", "6M"}
	periodParts := make([]string, 0, len(periods))
	for _, p := range periods {
		if p == periodLabel {
			periodParts = append(periodParts, deckHighlightStyle.Render(" "+p+" "))
		} else {
			periodParts = append(periodParts, deckMutedStyle.Render(p))
		}
	}
	titleLine := renderHeaderLine(w,
		deckSectionStyle.Render("SESSION ANALYTICS"),
		strings.Join(periodParts, "  ")+" "+deckDimStyle.Render("(p)"),
	)
	lines = append(lines, "", titleLine, "")

	// ── Tab bar ──
	lines = append(lines, m.renderAnalyticsTabBar(w), "")

	// ── Summary metric cards ──
	lines = append(lines, m.renderAnalyticsSummaryCards(a, w)...)
	lines = append(lines, "")

	halfW := (w - 4) / 2

	switch m.analyticsTabSel {
	case analyticsTabActivity:
		// ── Activity heatmap + Top tools (side by side) ──
		leftPanel := m.renderAnalyticsHeatmap(a.ActivityByDay, halfW)
		rightPanel := m.renderAnalyticsTools(a.TopTools, halfW)
		padPanelLines(leftPanel, halfW)
		combined := joinColumns(leftPanel, rightPanel, 4)
		lines = append(lines, combined...)
		lines = append(lines, "")

		if dayDetail := m.renderAnalyticsDayDetail(w); len(dayDetail) > 0 {
			lines = append(lines, dayDetail...)
			lines = append(lines, "")
		}

	case analyticsTabDistribution:
		// ── Duration + Cost distribution (side by side) ──
		leftHist := m.renderAnalyticsHistogram(a.DurationBuckets, "duration distribution", halfW)
		rightHist := m.renderAnalyticsHistogram(a.CostBuckets, "cost distribution", halfW)
		padPanelLines(leftHist, halfW)
		combined := joinColumns(leftHist, rightHist, 4)
		lines = append(lines, combined...)
		lines = append(lines, "")

		// ── Model comparison + Provider split (side by side) ──
		modelW := (w * 3 / 5) - 2
		providerW := (w * 2 / 5) - 2
		leftModel := m.renderAnalyticsModelTable(a.ModelPerformance, modelW)
		rightProvider := m.renderAnalyticsProviders(a.ProviderBreakdown, providerW)
		padPanelLines(leftModel, modelW)
		combined = joinColumns(leftModel, rightProvider, 4)
		lines = append(lines, combined...)

	case analyticsTabInsights:
		// ── AI Insights (without summaries) ──
		if insightLines := m.renderFacetInsightsNoSummaries(w); len(insightLines) > 0 {
			lines = append(lines, insightLines...)
		} else {
			lines = append(lines, deckMutedStyle.Render("  no ai insights available"))
		}

	case analyticsTabSummaries:
		// ── Recent Summaries ──
		if summaryLines := m.renderFacetSummariesTab(w); len(summaryLines) > 0 {
			lines = append(lines, summaryLines...)
		} else {
			lines = append(lines, deckMutedStyle.Render("  no summaries available"))
		}
	}

	lines = append(lines, "")

	return strings.Split(strings.Join(lines, "\n"), "\n")
}

func (m deckModel) renderAnalyticsTabBar(width int) string {
	tabs := []struct {
		label string
		tab   analyticsTab
	}{
		{"activity", analyticsTabActivity},
		{"distribution + models", analyticsTabDistribution},
		{"ai insights", analyticsTabInsights},
		{"summaries", analyticsTabSummaries},
	}

	parts := make([]string, 0, len(tabs))
	for _, t := range tabs {
		label := strings.ToUpper(t.label)
		if m.analyticsTabSel == t.tab {
			parts = append(parts, deckHighlightStyle.Render(" "+label+" "))
		} else {
			parts = append(parts, deckMutedStyle.Render(" "+label+" "))
		}
	}
	tabBar := strings.Join(parts, deckDimStyle.Render("│"))
	hint := deckDimStyle.Render("(tab)")
	return renderHeaderLine(width, tabBar, hint)
}

// renderAnalyticsSectionHeader renders "LABEL ────────────────" like the web's section-header.
func renderAnalyticsSectionHeader(label string, width int) string {
	rendered := deckMutedStyle.Bold(true).Render(strings.ToUpper(label))
	labelWidth := lipgloss.Width(rendered)
	lineWidth := max(0, width-labelWidth-1)
	return rendered + " " + deckDimStyle.Render(strings.Repeat("─", lineWidth))
}

func (m deckModel) renderAnalyticsSummaryCards(a *deck.AnalyticsOverview, width int) []string {
	type metricCard struct {
		label string
		value string
		sub   string
	}

	modelCount := len(a.ModelPerformance)
	avgDuration := time.Duration(a.AvgDurationNs)

	cards := []metricCard{
		{label: "TOTAL SESSIONS", value: strconv.Itoa(a.TotalSessions), sub: "sessions tracked"},
		{label: "AVG COST/SESSION", value: formatCost(a.AvgSessionCost), sub: "per session avg"},
		{label: "AVG DURATION", value: formatDurationMinutes(avgDuration), sub: "per session avg"},
		{label: "MODELS TRACKED", value: strconv.Itoa(modelCount), sub: fmt.Sprintf("%d providers", len(a.ProviderBreakdown))},
	}

	cols := len(cards)
	gap := 3
	cardWidth := max((width-(cols-1)*gap)/cols, 18)

	labelStyle := lipgloss.NewStyle().Foreground(colorLabel)
	valueStyle := lipgloss.NewStyle().Foreground(colorForeground).Bold(true)

	// Build each card as a bordered box
	cardLines := make([][]string, cols)
	maxHeight := 0
	for i, c := range cards {
		topBorder := deckDimStyle.Render("┌" + strings.Repeat("─", cardWidth-2) + "┐")
		bottomBorder := deckDimStyle.Render("└" + strings.Repeat("─", cardWidth-2) + "┘")

		inner := cardWidth - 4 // 2 for borders, 2 for padding
		labelLine := deckDimStyle.Render("│") + " " + labelStyle.Render(fitCell(c.label, inner)) + " " + deckDimStyle.Render("│")
		valueLine := deckDimStyle.Render("│") + " " + valueStyle.Render(fitCell(c.value, inner)) + " " + deckDimStyle.Render("│")
		subLine := deckDimStyle.Render("│") + " " + deckMutedStyle.Render(fitCell(c.sub, inner)) + " " + deckDimStyle.Render("│")
		emptyLine := deckDimStyle.Render("│") + strings.Repeat(" ", cardWidth-2) + deckDimStyle.Render("│")

		cardLines[i] = []string{topBorder, labelLine, emptyLine, valueLine, subLine, bottomBorder}
		if len(cardLines[i]) > maxHeight {
			maxHeight = len(cardLines[i])
		}
	}

	// Pad columns and join side by side
	padded := make([][]string, cols)
	for i := range cols {
		padded[i] = padLines(cardLines[i], cardWidth, maxHeight)
	}

	result := make([]string, 0, maxHeight)
	gapStr := strings.Repeat(" ", gap)
	for row := range maxHeight {
		parts := make([]string, 0, cols)
		for col := range cols {
			parts = append(parts, padded[col][row])
		}
		result = append(result, strings.Join(parts, gapStr))
	}

	return result
}

func (m deckModel) renderAnalyticsHeatmap(days []deck.DayActivity, width int) []string {
	lines := []string{renderAnalyticsSectionHeader("activity", width)}

	if len(days) == 0 {
		lines = append(lines, deckMutedStyle.Render("  no activity data"))
		return lines
	}

	maxSessions := 0
	totalSessions := 0
	totalCost := 0.0
	activeDays := 0
	peakDay := days[0]
	for _, d := range days {
		totalSessions += d.Sessions
		totalCost += d.Cost
		if d.Sessions > maxSessions {
			maxSessions = d.Sessions
			peakDay = d
		}
		if d.Sessions > 0 {
			activeDays++
		}
	}
	if maxSessions == 0 {
		maxSessions = 1
	}

	// Heatmap grid — 2-char colored blocks with intensity
	// 4 levels: empty, low, medium, high
	cellW := 3 // "██ " visual width
	cellsPerRow := max((width-4)/cellW, 1)

	displayDays := days
	// On half-width panels, limit to 5 most recent days for readability
	if width < 80 {
		recentLimit := 5
		if len(displayDays) > recentLimit {
			displayDays = displayDays[len(displayDays)-recentLimit:]
		}
	} else {
		maxCells := cellsPerRow * 4
		if len(displayDays) > maxCells {
			displayDays = displayDays[len(displayDays)-maxCells:]
		}
	}

	greenHigh := lipgloss.NewStyle().Foreground(colorGreen).Bold(true)
	greenMed := lipgloss.NewStyle().Foreground(colorGreen)
	greenLow := lipgloss.NewStyle().Foreground(colorBrightBlack)
	selectedStyle := lipgloss.NewStyle().Foreground(colorYellow).Bold(true)

	for i := 0; i < len(displayDays); i += cellsPerRow {
		end := min(i+cellsPerRow, len(displayDays))
		chunk := displayDays[i:end]

		var blockRow strings.Builder
		var dateRow strings.Builder
		blockRow.WriteString("  ")
		dateRow.WriteString("  ")
		for _, d := range chunk {
			selected := d.Date == m.analyticsDaySel
			if d.Sessions == 0 {
				cell := deckDimStyle.Render("░░")
				if selected {
					cell = selectedStyle.Render("░░")
				}
				blockRow.WriteString(cell + " ")
			} else {
				intensity := float64(d.Sessions) / float64(maxSessions)
				var cell string
				switch {
				case intensity >= 0.7:
					cell = greenHigh.Render("██")
				case intensity >= 0.3:
					cell = greenMed.Render("▓▓")
				default:
					cell = greenLow.Render("▒▒")
				}
				if selected {
					cell = selectedStyle.Render("██")
				}
				blockRow.WriteString(cell + " ")
			}
			dateLabel := d.Date
			if len(dateLabel) > 5 {
				dateLabel = dateLabel[5:]
			}
			dayPart := dateLabel
			if len(dateLabel) >= 5 {
				dayPart = dateLabel[3:5]
			}
			dateRow.WriteString(deckDimStyle.Render(fmt.Sprintf("%-2s", dayPart)) + " ")
		}
		lines = append(lines, blockRow.String())
		lines = append(lines, dateRow.String())
	}

	if keyRows := m.renderAnalyticsHeatmapKeys(days, width); len(keyRows) > 0 {
		lines = append(lines, "")
		lines = append(lines, keyRows...)
	}

	// Legend
	legend := "  " + deckDimStyle.Render("░░") + " none  " +
		greenLow.Render("▒▒") + " low  " +
		greenMed.Render("▓▓") + " med  " +
		greenHigh.Render("██") + " high"
	lines = append(lines, "", legend)

	// Insights
	insightStyle := lipgloss.NewStyle().Foreground(colorBrightBlack)
	bulletStyle := lipgloss.NewStyle().Foreground(colorGreen)
	bullet := bulletStyle.Render("▸")
	lines = append(lines, "")

	peakDate := peakDay.Date
	if len(peakDate) > 5 {
		peakDate = peakDate[5:]
	}
	lines = append(lines, "  "+bullet+" "+insightStyle.Render(fmt.Sprintf(
		"peak: %s with %d sessions (%s spent)", peakDate, peakDay.Sessions, formatCost(peakDay.Cost))))

	pct := 0
	if len(days) > 0 {
		pct = activeDays * 100 / len(days)
	}
	lines = append(lines, "  "+bullet+" "+insightStyle.Render(fmt.Sprintf(
		"%d of %d days active (%d%%)", activeDays, len(days), pct)))

	if activeDays > 0 {
		avg := float64(totalSessions) / float64(activeDays)
		lines = append(lines, "  "+bullet+" "+insightStyle.Render(fmt.Sprintf(
			"avg %.1f sessions on active days", avg)))
	}

	// Current streak
	streak := 0
	for i := len(days) - 1; i >= 0; i-- {
		if days[i].Sessions > 0 {
			streak++
		} else {
			break
		}
	}
	if streak > 0 {
		label := "day"
		if streak > 1 {
			label = "days"
		}
		lines = append(lines, "  "+bullet+" "+insightStyle.Render(fmt.Sprintf(
			"current streak: %d %s", streak, label)))
	}

	if selectedLine := m.renderAnalyticsSelectedDay(days); selectedLine != "" {
		lines = append(lines, "  "+bullet+" "+insightStyle.Render(selectedLine))
	}

	return lines
}

func (m deckModel) renderAnalyticsHeatmapKeys(days []deck.DayActivity, width int) []string {
	selectable := analyticsSelectableDays(days)
	if len(selectable) == 0 {
		return nil
	}

	// On half-width panels, limit to 5 most recent selectable days (already reversed, so take first 5)
	if width < 80 && len(selectable) > 5 {
		selectable = selectable[:5]
	}

	// Render in chronological order (reverse of selectable) so labels
	// match the heatmap cells: oldest on the left, newest (0) on the right.
	parts := make([]string, len(selectable))
	for i, d := range selectable {
		label := d.Date
		if len(label) > 5 {
			label = label[5:]
		}
		entry := fmt.Sprintf("%d:%s", i+1, label)
		if d.Date == m.analyticsDaySel {
			entry = deckHighlightStyle.Render(entry)
		} else {
			entry = deckMutedStyle.Render(entry)
		}
		// Place in reverse position so 0 (newest) is on the right
		parts[len(selectable)-1-i] = entry
	}

	line := "  " + strings.Join(parts, "  ")
	if width > 0 && lipgloss.Width(line) > width {
		line = "  " + strings.Join(parts, " ")
	}
	return []string{line, deckDimStyle.Render("  1-9 select recent days")}
}

func (m deckModel) renderAnalyticsSelectedDay(days []deck.DayActivity) string {
	if m.analyticsDaySel == "" {
		return ""
	}
	for _, d := range days {
		if d.Date == m.analyticsDaySel {
			label := d.Date
			if len(label) > 5 {
				label = label[5:]
			}
			return fmt.Sprintf("selected: %s · %d sessions · %s spent", label, d.Sessions, formatCost(d.Cost))
		}
	}
	return ""
}

func (m deckModel) renderAnalyticsDayDetail(width int) []string {
	label := "day detail"
	if m.analyticsDaySel != "" {
		label = "day detail " + trimDateLabel(m.analyticsDaySel)
	}

	lines := []string{renderAnalyticsSectionHeader(label, width)}
	if m.analyticsDaySel == "" {
		lines = append(lines, "  "+deckMutedStyle.Render("select a day (1-9) to drill in"))
		lines = append(lines, strings.Repeat(" ", max(0, width-2)))
		lines = append(lines, strings.Repeat(" ", max(0, width-2)))
		lines = append(lines, strings.Repeat(" ", max(0, width-2)))
		return lines
	}
	if m.analyticsDay == nil {
		lines = append(lines, "  "+m.spinner.View()+" "+deckMutedStyle.Render("loading day details..."))
		lines = append(lines, strings.Repeat(" ", max(0, width-2)))
		lines = append(lines, strings.Repeat(" ", max(0, width-2)))
		return lines
	}

	overview := m.analyticsDay
	sessionCount := len(overview.Sessions)
	avgCost := 0.0
	if sessionCount > 0 {
		avgCost = overview.TotalCost / float64(sessionCount)
	}
	metrics := fmt.Sprintf("  sessions: %d · total: %s · avg: %s · success: %s",
		sessionCount,
		formatCost(overview.TotalCost),
		formatCost(avgCost),
		formatPercent(overview.SuccessRate),
	)
	lines = append(lines, deckMutedStyle.Render(metrics))

	if sessionCount == 0 {
		lines = append(lines, "  "+deckMutedStyle.Render("no sessions for this day"))
		return lines
	}

	lines = append(lines, "")
	lines = append(lines, m.renderAnalyticsDaySessions(overview.Sessions, width)...)
	lines = append(lines, deckDimStyle.Render("  h/esc to clear day"))

	return lines
}

func (m deckModel) renderAnalyticsDaySessions(sessions []deck.SessionSummary, width int) []string {
	const (
		minLabelW = 12
		maxRows   = 8
	)

	indexW := 3
	labelW := 24
	modelW := 12
	durW := 7
	costW := 8
	statusW := 9
	gap := 2

	available := max(width-2, 40)
	base := indexW + labelW + modelW + durW + costW + statusW + gap*5
	if base > available {
		diff := base - available
		reduce := min(diff, labelW-minLabelW)
		labelW -= reduce
		diff -= reduce
		if diff > 0 {
			modelW = max(8, modelW-diff)
		}
	}

	gapStr := strings.Repeat(" ", gap)
	header := strings.Join([]string{
		fitCell("#", indexW),
		fitCell("label", labelW),
		fitCell("model", modelW),
		fitCell("dur", durW),
		fitCell("cost", costW),
		fitCell("status", statusW),
	}, gapStr)
	lines := []string{"  " + deckDimStyle.Render(header)}

	limit := min(len(sessions), maxRows)
	for i := range limit {
		s := sessions[i]
		model := s.Model
		if model == "" {
			model = "unknown"
		}
		row := strings.Join([]string{
			fitCell(fmt.Sprintf("%02d", i+1), indexW),
			fitCell(truncateText(s.Label, labelW), labelW),
			fitCell(truncateText(model, modelW), modelW),
			fitCell(formatDurationMinutes(s.Duration), durW),
			fitCell(formatCost(s.TotalCost), costW),
			statusStyleFor(s.Status).Render(fitCell(s.Status, statusW)),
		}, gapStr)
		lines = append(lines, "  "+row)
	}

	if len(sessions) > maxRows {
		lines = append(lines, "  "+deckDimStyle.Render(fmt.Sprintf("... %d more sessions", len(sessions)-maxRows)))
	}

	return lines
}

func (m deckModel) selectAnalyticsDay(key string) (string, bool) {
	if m.analytics == nil {
		return "", false
	}
	index := int(key[0] - '1')
	selectable := analyticsSelectableDays(m.analytics.ActivityByDay)
	if index < 0 || index >= len(selectable) {
		return "", false
	}
	return selectable[index].Date, true
}

func analyticsSelectableDays(days []deck.DayActivity) []deck.DayActivity {
	if len(days) == 0 {
		return nil
	}
	recent := days
	if len(recent) > 9 {
		recent = recent[len(recent)-9:]
	}
	// Reverse so index 0 = most recent day
	reversed := make([]deck.DayActivity, len(recent))
	for i, d := range recent {
		reversed[len(recent)-1-i] = d
	}
	return reversed
}

func analyticsDayExists(days []deck.DayActivity, date string) bool {
	for _, d := range days {
		if d.Date == date {
			return true
		}
	}
	return false
}

func trimDateLabel(dateStr string) string {
	if len(dateStr) > 5 {
		return dateStr[5:]
	}
	return dateStr
}

type analyticsDayRange struct {
	from time.Time
	to   time.Time
}

func parseAnalyticsDay(dateStr string) (analyticsDayRange, error) {
	parsed, err := time.ParseInLocation("2006-01-02", dateStr, time.Local)
	if err != nil {
		return analyticsDayRange{}, fmt.Errorf("parse analytics day: %w", err)
	}
	start := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.Local)
	end := start.AddDate(0, 0, 1)
	return analyticsDayRange{from: start, to: end}, nil
}

func (m deckModel) renderAnalyticsTools(tools []deck.ToolMetric, width int) []string {
	lines := []string{renderAnalyticsSectionHeader("top tools", width)}

	if len(tools) == 0 {
		lines = append(lines, deckMutedStyle.Render("no tool data"))
		return lines
	}

	const toolsCollapsedLimit = 10
	// Sort by sessions (most meaningful metric)
	sort.Slice(tools, func(i, j int) bool {
		if tools[i].Sessions != tools[j].Sessions {
			return tools[i].Sessions > tools[j].Sessions
		}
		return tools[i].Name < tools[j].Name
	})

	displayTools := tools
	hiddenTools := 0
	if !m.insightsExpanded && len(tools) > toolsCollapsedLimit {
		displayTools = tools[:toolsCollapsedLimit]
		hiddenTools = len(tools) - toolsCollapsedLimit
	}

	maxSessions := 0
	for _, t := range displayTools {
		if t.Sessions > maxSessions {
			maxSessions = t.Sessions
		}
	}
	if maxSessions == 0 {
		maxSessions = 1
	}

	nameWidth := 18
	barWidth := max(width-nameWidth-20, 8)

	// Column header
	headerName := fitCell("tool", nameWidth)
	headerBar := strings.Repeat(" ", barWidth+1)
	lines = append(lines, deckDimStyle.Render(headerName+" "+headerBar+"sess avg/s"))

	for _, t := range displayTools {
		name := fitCell(truncateText(t.Name, nameWidth), nameWidth)
		ratio := float64(t.Sessions) / float64(maxSessions)
		filled := min(max(int(ratio*float64(barWidth)), 0), barWidth)
		empty := barWidth - filled

		bar := lipgloss.NewStyle().Foreground(colorBlue).Render(strings.Repeat("█", filled)) +
			deckDimStyle.Render(strings.Repeat("░", empty))

		avg := 0
		if t.Sessions > 0 {
			avg = t.Count / t.Sessions
		}
		meta := fmt.Sprintf("%3d %4d", t.Sessions, avg)
		if t.ErrorCount > 0 {
			meta += deckStatusFailStyle.Render(fmt.Sprintf(" %de", t.ErrorCount))
		}

		lines = append(lines, name+" "+bar+" "+meta)
	}

	if hiddenTools > 0 {
		lines = append(lines, deckMutedStyle.Render(fmt.Sprintf("  +%d more (e)", hiddenTools)))
	} else if m.insightsExpanded && len(tools) > toolsCollapsedLimit {
		lines = append(lines, deckMutedStyle.Render("  show less (e)"))
	}

	// Insights
	insightStyle := lipgloss.NewStyle().Foreground(colorBrightBlack)
	bullet := lipgloss.NewStyle().Foreground(colorBlue).Render("▸")
	lines = append(lines, "")

	totalCalls := 0
	totalErrors := 0
	for _, t := range tools {
		totalCalls += t.Count
		totalErrors += t.ErrorCount
	}

	if len(tools) > 0 {
		lines = append(lines, bullet+" "+insightStyle.Render(fmt.Sprintf(
			"%s is most used (%d sessions)",
			tools[0].Name, tools[0].Sessions)))
	}

	if totalErrors > 0 {
		errRate := float64(totalErrors) / float64(totalCalls) * 100
		lines = append(lines, bullet+" "+insightStyle.Render(fmt.Sprintf(
			"%.1f%% error rate (%d errors)",
			errRate, totalErrors)))
	} else {
		lines = append(lines, bullet+" "+insightStyle.Render("no tool errors detected"))
	}

	lines = append(lines, bullet+" "+insightStyle.Render(fmt.Sprintf(
		"%d unique tools across %d sessions", len(tools), m.analytics.TotalSessions)))

	return lines
}

func (m deckModel) renderAnalyticsHistogram(buckets []deck.Bucket, label string, width int) []string {
	lines := []string{renderAnalyticsSectionHeader(label, width)}

	if len(buckets) == 0 {
		lines = append(lines, deckMutedStyle.Render("no data"))
		return lines
	}

	maxCount := 0
	for _, b := range buckets {
		if b.Count > maxCount {
			maxCount = b.Count
		}
	}
	if maxCount == 0 {
		maxCount = 1
	}

	// Compute label width from longest bucket label
	labelWidth := 6
	for _, b := range buckets {
		if len(b.Label) > labelWidth {
			labelWidth = len(b.Label)
		}
	}
	labelWidth++ // trailing space
	countWidth := 5
	barWidth := max(width-labelWidth-countWidth-4, 8)

	for _, b := range buckets {
		bucketLabel := deckMutedStyle.Render(fitCell(b.Label, labelWidth))
		ratio := float64(b.Count) / float64(maxCount)
		filled := min(max(int(ratio*float64(barWidth)), 0), barWidth)
		empty := barWidth - filled

		bar := deckDimStyle.Render(strings.Repeat("░", empty))
		if filled > 0 {
			bar = lipgloss.NewStyle().Foreground(colorMagenta).Render(strings.Repeat("█", filled)) + bar
		}

		count := lipgloss.NewStyle().Foreground(colorForeground).Bold(true).Render(fmt.Sprintf("%4d", b.Count))
		lines = append(lines, bucketLabel+" "+bar+" "+count)
	}

	// Insights
	insightStyle := lipgloss.NewStyle().Foreground(colorBrightBlack)
	bullet := lipgloss.NewStyle().Foreground(colorMagenta).Render("▸")
	lines = append(lines, "")

	// Find the most common bucket (mode)
	totalCount := 0
	modeBucket := buckets[0]
	for _, b := range buckets {
		totalCount += b.Count
		if b.Count > modeBucket.Count {
			modeBucket = b
		}
	}
	pct := modeBucket.Count * 100 / max(totalCount, 1)
	lines = append(lines, bullet+" "+insightStyle.Render(fmt.Sprintf(
		"most common: %s (%d%% of sessions)", modeBucket.Label, pct)))

	return lines
}

func (m deckModel) renderAnalyticsModelTable(models []deck.ModelPerformance, width int) []string {
	lines := []string{renderAnalyticsSectionHeader("model comparison", width)}

	if len(models) == 0 {
		lines = append(lines, deckMutedStyle.Render("no model data"))
		return lines
	}

	// Table with borders
	topBorder := deckDimStyle.Render("┌" + strings.Repeat("─", width-2) + "┐")
	bottomBorder := deckDimStyle.Render("└" + strings.Repeat("─", width-2) + "┘")

	inner := width - 4 // borders + padding

	// Column widths — 6 columns with 2-char gaps between them
	dataCols := 5
	colGap := 2
	nameW := max(inner*3/10, 14)
	colW := max((inner-nameW-(dataCols-1)*colGap)/dataCols, 7)
	gap := strings.Repeat(" ", colGap)

	headerParts := []string{
		fitCell("MODEL", nameW),
		fitCell("SESS", colW),
		fitCell("AVG $", colW),
		fitCell("AVG DUR", colW),
		fitCell("TOKENS", colW),
		fitCell("SUCCESS", colW),
	}
	headerLine := strings.Join(headerParts, gap)
	headerRow := deckDimStyle.Render("│") + " " +
		deckMutedStyle.Render(fitCell(headerLine, inner)) + " " +
		deckDimStyle.Render("│")
	divider := deckDimStyle.Render("├" + strings.Repeat("─", width-2) + "┤")

	lines = append(lines, topBorder, headerRow, divider)

	for _, mp := range models {
		modelName := colorizeModel(truncateText(mp.Model, nameW-1))
		modelPadded := padRightWithColor(modelName, nameW)

		avgDur := formatDurationMinutes(time.Duration(mp.AvgDurationNs))
		tokens := formatTokens(mp.AvgTokens)
		cost := formatCost(mp.AvgCost)

		// Color success rate based on threshold (like the web)
		successStr := formatPercent(mp.SuccessRate)
		var successStyled string
		switch {
		case mp.SuccessRate >= 0.8:
			successStyled = deckStatusOKStyle.Render(successStr)
		case mp.SuccessRate >= 0.5:
			successStyled = deckStatusWarnStyle.Render(successStr)
		default:
			successStyled = deckStatusFailStyle.Render(successStr)
		}

		dataParts := []string{
			fitCell(strconv.Itoa(mp.Sessions), colW),
			fitCell(cost, colW),
			fitCell(avgDur, colW),
			fitCell(tokens, colW),
		}
		dataLine := strings.Join(dataParts, gap)

		row := deckDimStyle.Render("│") + " " +
			modelPadded + gap + dataLine + gap + padRightWithColor(successStyled, colW) + " " +
			deckDimStyle.Render("│")
		lines = append(lines, row)
	}

	lines = append(lines, bottomBorder)

	// Insights
	insightStyle := lipgloss.NewStyle().Foreground(colorBrightBlack)
	bullet := lipgloss.NewStyle().Foreground(colorMagenta).Render("▸")
	lines = append(lines, "")

	if len(models) > 0 {
		// Most cost-effective: lowest avg cost among models with >=2 sessions
		bestValue := models[0]
		bestSuccess := models[0]
		for _, mp := range models {
			if mp.Sessions >= 2 && mp.AvgCost < bestValue.AvgCost {
				bestValue = mp
			}
			if mp.SuccessRate > bestSuccess.SuccessRate {
				bestSuccess = mp
			}
		}
		lines = append(lines, bullet+" "+insightStyle.Render(fmt.Sprintf(
			"%s is most cost-effective at %s/session", bestValue.Model, formatCost(bestValue.AvgCost))))
		if bestSuccess.Model != bestValue.Model {
			lines = append(lines, bullet+" "+insightStyle.Render(fmt.Sprintf(
				"%s has highest success rate at %s", bestSuccess.Model, formatPercent(bestSuccess.SuccessRate))))
		}
	}

	return lines
}

func (m deckModel) renderAnalyticsProviders(providers map[string]int, width int) []string {
	lines := []string{renderAnalyticsSectionHeader("provider split", width)}

	if len(providers) == 0 {
		lines = append(lines, deckMutedStyle.Render("no provider data"))
		return lines
	}

	total := 0
	for _, count := range providers {
		total += count
	}
	if total == 0 {
		total = 1
	}

	type providerEntry struct {
		name  string
		count int
	}
	sorted := make([]providerEntry, 0, len(providers))
	for name, count := range providers {
		sorted = append(sorted, providerEntry{name: name, count: count})
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].count != sorted[j].count {
			return sorted[i].count > sorted[j].count
		}
		return sorted[i].name < sorted[j].name
	})

	// Stacked bar (like the web's provider__bar)
	barWidth := max(width-2, 10)
	var stackedBar strings.Builder
	providerColor := func(name string) lipgloss.Style {
		switch strings.ToLower(name) {
		case "anthropic":
			return lipgloss.NewStyle().Foreground(colorMagenta)
		case "openai":
			return lipgloss.NewStyle().Foreground(colorGreen)
		case "google":
			return lipgloss.NewStyle().Foreground(colorYellow)
		default:
			return lipgloss.NewStyle().Foreground(colorBlue)
		}
	}

	for _, p := range sorted {
		segWidth := max(int(float64(p.count)/float64(total)*float64(barWidth)), 1)
		stackedBar.WriteString(providerColor(p.name).Render(strings.Repeat("█", segWidth)))
	}
	lines = append(lines, stackedBar.String(), "")

	// Legend with dots (like the web's provider__legend)
	for _, p := range sorted {
		pct := float64(p.count) / float64(total) * 100
		dot := providerColor(p.name).Render("●")
		legend := fmt.Sprintf("%s %-12s %3.0f%% (%d)", dot, p.name, pct, p.count)
		lines = append(lines, legend)
	}

	// Insights
	insightStyle := lipgloss.NewStyle().Foreground(colorBrightBlack)
	bullet := lipgloss.NewStyle().Foreground(colorMagenta).Render("▸")
	lines = append(lines, "")

	if len(sorted) > 0 {
		topPct := float64(sorted[0].count) / float64(total) * 100
		lines = append(lines, bullet+" "+insightStyle.Render(fmt.Sprintf(
			"%s is primary provider at %.0f%% of sessions", sorted[0].name, topPct)))
	}
	if len(sorted) > 1 {
		lines = append(lines, bullet+" "+insightStyle.Render(fmt.Sprintf(
			"%d providers in use", len(sorted))))
	}

	return lines
}

// facetLabel holds a human-readable label and description for a facet key.
type facetLabel struct {
	label string
	desc  string
}

var goalLabels = map[string]facetLabel{
	"debug_investigate":   {label: "Debug / Investigate", desc: "Diagnosing errors, tracing issues, reading logs"},
	"implement_feature":   {label: "Implement Feature", desc: "Building new functionality from scratch"},
	"fix_bug":             {label: "Fix Bug", desc: "Correcting broken behavior in existing code"},
	"write_script_tool":   {label: "Write Script / Tool", desc: "One-off scripts, automation, or CLI tools"},
	"refactor_code":       {label: "Refactor Code", desc: "Restructuring code without changing behavior"},
	"configure_system":    {label: "Configure System", desc: "Setting up infra, CI/CD, environment configs"},
	"create_pr_commit":    {label: "Create PR / Commit", desc: "Preparing code for review and submission"},
	"analyze_data":        {label: "Analyze Data", desc: "Querying, exploring, or summarizing data"},
	"understand_codebase": {label: "Understand Codebase", desc: "Reading code to learn how something works"},
	"write_tests":         {label: "Write Tests", desc: "Adding unit, integration, or e2e tests"},
	"write_docs":          {label: "Write Docs", desc: "Creating or updating documentation"},
	"deploy_infra":        {label: "Deploy / Infra", desc: "Deploying code or managing infrastructure"},
	"warmup_minimal":      {label: "Warmup / Minimal", desc: "Brief session with little substantive work"},
}

var outcomeLabels = map[string]facetLabel{
	"fully_achieved":     {label: "Fully Achieved", desc: "Goal completed successfully"},
	"mostly_achieved":    {label: "Mostly Achieved", desc: "Goal met with minor gaps remaining"},
	"partially_achieved": {label: "Partially Achieved", desc: "Some progress but significant work remains"},
	"not_achieved":       {label: "Not Achieved", desc: "Goal was not accomplished"},
}

var sessionTypeLabels = map[string]facetLabel{
	"single_task":          {label: "Single Task", desc: "Focused on one specific objective"},
	"multi_task":           {label: "Multi-Task", desc: "Tackled several distinct objectives"},
	"iterative_refinement": {label: "Iterative Refinement", desc: "Repeated cycles of adjustment and improvement"},
	"exploration":          {label: "Exploration", desc: "Open-ended investigation or learning"},
}

var frictionLabels = map[string]facetLabel{
	"wrong_approach":        {label: "Wrong Approach", desc: "Went down an incorrect path before correcting"},
	"buggy_code":            {label: "Buggy Code", desc: "Generated code that had bugs needing fixes"},
	"misunderstood_request": {label: "Misunderstood Request", desc: "Misinterpreted what the user was asking"},
	"tool_failure":          {label: "Tool Failure", desc: "A tool call failed or returned unexpected results"},
	"unclear_requirements":  {label: "Unclear Requirements", desc: "Ambiguous or incomplete requirements"},
	"scope_creep":           {label: "Scope Creep", desc: "Task expanded beyond the original ask"},
	"environment_issue":     {label: "Environment Issue", desc: "Problems with setup, deps, or config"},
}

func facetDisplayName(key string, labels map[string]facetLabel) string {
	if l, ok := labels[key]; ok {
		return l.label
	}
	return strings.ReplaceAll(key, "_", " ")
}

func (m deckModel) renderFacetInsightsNoSummaries(width int) []string {
	if m.facetLoadFn == nil {
		return nil
	}

	if m.facetAnalytics == nil {
		if m.facetWorker != nil {
			done, total := m.facetWorker.Progress()
			if total > 0 {
				lines := make([]string, 0, 2)
				lines = append(lines, renderAnalyticsSectionHeader("ai insights", width))
				lines = append(lines, "  "+m.spinner.View()+" "+deckMutedStyle.Render(
					fmt.Sprintf("analyzing sessions... %d of %d", done, total)))
				return lines
			}
		}
		return nil
	}

	fa := m.facetAnalytics
	halfW := (width - 4) / 2

	leftGoals := m.renderFacetDistribution(fa.GoalDistribution, "goal categories", colorBlue, halfW, goalLabels)
	rightOutcomes := m.renderFacetOutcomes(fa.OutcomeDistribution, halfW)
	padPanelLines(leftGoals, halfW)
	goalOutcome := joinColumns(leftGoals, rightOutcomes, 4)

	leftFriction := m.renderFacetFriction(fa.TopFriction, halfW)
	rightTypes := m.renderFacetDistribution(fa.SessionTypes, "session types", colorMagenta, halfW, sessionTypeLabels)
	padPanelLines(leftFriction, halfW)
	frictionTypes := joinColumns(leftFriction, rightTypes, 4)

	lines := make([]string, 0, 1+len(goalOutcome)+1+len(frictionTypes))
	lines = append(lines, renderAnalyticsSectionHeader("ai insights", width))
	lines = append(lines, goalOutcome...)
	lines = append(lines, "")
	lines = append(lines, frictionTypes...)

	return lines
}

const (
	summariesDisplayLimit  = 10
	summariesVisibleWindow = 5
)

func (m deckModel) renderFacetSummariesTab(width int) []string {
	if m.facetLoadFn == nil || m.facetAnalytics == nil {
		return nil
	}
	summaries := m.facetAnalytics.RecentSummaries
	if len(summaries) == 0 {
		return nil
	}

	display := summaries
	if len(display) > summariesDisplayLimit {
		display = display[:summariesDisplayLimit]
	}

	// Compute visible window around cursor
	total := len(display)
	windowSize := min(summariesVisibleWindow, total)
	start := max(m.summaryCursor-windowSize/2, 0)
	if start+windowSize > total {
		start = total - windowSize
	}
	end := start + windowSize

	lines := []string{renderAnalyticsSectionHeader(
		fmt.Sprintf("recent summaries (%d/%d)", m.summaryCursor+1, total), width)}

	goalStyle := lipgloss.NewStyle().Foreground(colorBlue).Bold(true)
	activeGoalStyle := lipgloss.NewStyle().Foreground(colorBlue).Bold(true).Background(colorHighlightBg)
	maxTextW := width - 8

	if start > 0 {
		lines = append(lines, deckDimStyle.Render(fmt.Sprintf("    ▲ %d more", start)))
	}

	for i := start; i < end; i++ {
		s := display[i]
		active := i == m.summaryCursor
		prefix := "  "
		gs := goalStyle
		if active {
			prefix = deckAccentStyle.Render("▸ ")
			gs = activeGoalStyle
		}

		goal := s.UnderlyingGoal
		summary := s.BriefSummary
		if goal == "" && summary == "" {
			continue
		}

		if goal != "" {
			goalText := goal
			if len(goalText) > maxTextW {
				goalText = goalText[:maxTextW-3] + "..."
			}
			lines = append(lines, prefix+gs.Render(goalText))
		}
		if summary != "" {
			summaryText := summary
			if len(summaryText) > maxTextW {
				summaryText = summaryText[:maxTextW-3] + "..."
			}
			lines = append(lines, prefix+deckMutedStyle.Render(summaryText))
		}
		if s.GoalCategory != "" {
			catLabel := facetDisplayName(s.GoalCategory, goalLabels)
			lines = append(lines, prefix+facetDescStyle.Render("["+catLabel+"]"))
		}
		lines = append(lines, "")
	}

	if end < total {
		lines = append(lines, deckDimStyle.Render(fmt.Sprintf("    ▼ %d more", total-end)))
	}

	return lines
}

const facetCollapsedLimit = 5

func (m deckModel) renderFacetDistribution(dist map[string]int, label string, color lipgloss.TerminalColor, width int, labels map[string]facetLabel) []string {
	lines := []string{renderAnalyticsSectionHeader(label, width)}

	if len(dist) == 0 {
		lines = append(lines, deckMutedStyle.Render("  no data"))
		return lines
	}

	type entry struct {
		key   string
		count int
	}
	var entries []entry
	for name, count := range dist {
		entries = append(entries, entry{name, count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].key < entries[j].key
	})

	maxCount := 1
	if len(entries) > 0 {
		maxCount = entries[0].count
	}

	display := entries
	hidden := 0
	if !m.insightsExpanded && len(entries) > facetCollapsedLimit {
		display = entries[:facetCollapsedLimit]
		hidden = len(entries) - facetCollapsedLimit
	}

	nameWidth := 22
	barWidth := max(width-nameWidth-10, 8)
	maxDescW := max(width-nameWidth-1, 10)

	for _, e := range display {
		name := fitCell(facetDisplayName(e.key, labels), nameWidth)
		ratio := float64(e.count) / float64(maxCount)
		filled := min(max(int(ratio*float64(barWidth)), 1), barWidth)
		empty := barWidth - filled

		bar := lipgloss.NewStyle().Foreground(color).Render(strings.Repeat("█", filled)) +
			deckDimStyle.Render(strings.Repeat("░", empty))

		lines = append(lines, name+" "+bar+" "+fmt.Sprintf("%3d", e.count))

		if l, ok := labels[e.key]; ok && l.desc != "" {
			desc := l.desc
			if len(desc) > maxDescW {
				desc = desc[:maxDescW-3] + "..."
			}
			lines = append(lines, strings.Repeat(" ", nameWidth+1)+facetDescStyle.Render(desc))
		}
	}

	if hidden > 0 {
		lines = append(lines, deckMutedStyle.Render(fmt.Sprintf("  +%d more (e)", hidden)))
	} else if m.insightsExpanded && len(entries) > facetCollapsedLimit {
		lines = append(lines, deckMutedStyle.Render("  show less (e)"))
	}

	return lines
}

func (m deckModel) renderFacetOutcomes(dist map[string]int, width int) []string {
	lines := []string{renderAnalyticsSectionHeader("outcomes", width)}

	if len(dist) == 0 {
		lines = append(lines, deckMutedStyle.Render("  no data"))
		return lines
	}

	outcomeColors := map[string]lipgloss.TerminalColor{
		"fully_achieved":     colorGreen,
		"mostly_achieved":    colorBlue,
		"partially_achieved": colorYellow,
		"not_achieved":       colorRed,
	}

	type entry struct {
		key   string
		count int
	}
	var entries []entry
	total := 0
	for name, count := range dist {
		entries = append(entries, entry{name, count})
		total += count
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].key < entries[j].key
	})
	if total == 0 {
		total = 1
	}

	// Stacked bar
	barWidth := max(width-2, 10)
	var stackedBar strings.Builder
	for _, e := range entries {
		segWidth := max(int(float64(e.count)/float64(total)*float64(barWidth)), 1)
		c := colorBlue
		if oc, ok := outcomeColors[e.key]; ok {
			c = oc
		}
		stackedBar.WriteString(lipgloss.NewStyle().Foreground(c).Render(strings.Repeat("█", segWidth)))
	}
	lines = append(lines, stackedBar.String(), "")

	// Legend with dots, percentage, count, and description
	maxDescW := max(width-4, 10)
	for _, e := range entries {
		c := colorBlue
		if oc, ok := outcomeColors[e.key]; ok {
			c = oc
		}
		dot := lipgloss.NewStyle().Foreground(c).Render("●")
		label := facetDisplayName(e.key, outcomeLabels)
		pct := float64(e.count) / float64(total) * 100
		lines = append(lines, fmt.Sprintf("%s %s %.0f%% (%d)", dot, label, pct, e.count))

		if l, ok := outcomeLabels[e.key]; ok && l.desc != "" {
			desc := l.desc
			if len(desc) > maxDescW {
				desc = desc[:maxDescW-3] + "..."
			}
			lines = append(lines, "  "+facetDescStyle.Render(desc))
		}
	}

	return lines
}

func (m deckModel) renderFacetFriction(friction []deck.FrictionItem, width int) []string {
	lines := []string{renderAnalyticsSectionHeader("friction points", width)}

	if len(friction) == 0 {
		lines = append(lines, deckMutedStyle.Render("  no friction data"))
		return lines
	}

	display := friction
	if len(display) > 8 {
		display = display[:8]
	}

	bullet := lipgloss.NewStyle().Foreground(colorRed).Render("▸")
	countStyle := lipgloss.NewStyle().Foreground(colorYellow).Bold(true)

	for _, f := range display {
		label := facetDisplayName(f.Type, frictionLabels)
		lines = append(lines, fmt.Sprintf("  %s %s %s",
			bullet,
			countStyle.Render(strconv.Itoa(f.Count)),
			label))

		if l, ok := frictionLabels[f.Type]; ok && l.desc != "" {
			lines = append(lines, "       "+facetDescStyle.Render(l.desc))
		}
	}

	return lines
}

func (m deckModel) viewAnalyticsFooter() string {
	helpText := "j/k scroll • tab switch • h back • p period"
	if m.analyticsTabSel == analyticsTabActivity {
		helpText += " • 1-9 day"
	}
	if m.analyticsTabSel == analyticsTabInsights {
		helpText += " • e expand"
	}
	helpText += " • q quit"
	return deckMutedStyle.Render(helpText)
}
