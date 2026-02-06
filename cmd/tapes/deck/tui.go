package deckcmder

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	bubbletea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"

	"github.com/papercomputeco/tapes/pkg/deck"
)

func init() {
	// Force TrueColor profile to fix lipgloss color detection issue
	// See: https://github.com/charmbracelet/lipgloss/issues/439
	renderer := lipgloss.NewRenderer(os.Stdout, termenv.WithProfile(termenv.TrueColor))
	renderer.SetColorProfile(termenv.TrueColor)
	lipgloss.SetDefaultRenderer(renderer)
}

type deckView int

const (
	viewOverview deckView = iota
	viewSession
)

const trackedSessionShortcuts = 9

type deckModel struct {
	query         *deck.Query
	filters       deck.Filters
	overview      *deck.Overview
	detail        *deck.SessionDetail
	view          deckView
	cursor        int
	messageCursor int
	width         int
	height        int
	sortIndex     int
	statusIndex   int
	messageSort   int
	trackToggles  map[int]bool
	replayActive  bool
	replayOnLoad  bool
	keys          deckKeyMap
	help          help.Model
}

var (
	deckTitleStyle      = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("214"))
	deckMutedStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	deckAccentStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("215"))
	deckDimStyle        = lipgloss.NewStyle().Foreground(lipgloss.Color("236"))
	deckSectionStyle    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("252"))
	deckDividerStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("237"))
	deckMetricLabel     = lipgloss.NewStyle().Foreground(lipgloss.Color("246")).Bold(true)
	deckMetricValue     = lipgloss.NewStyle().Foreground(lipgloss.Color("252"))
	deckHighlightStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("235")).Background(lipgloss.Color("214")).Bold(true)
	deckStatusOKStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("70"))
	deckStatusFailStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("203"))
	deckStatusWarnStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	deckRoleUserStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("111"))
	deckRoleAsstStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("220"))
)

var (
	sortOrder        = []string{"cost", "time", "tokens", "duration"}
	messageSortOrder = []string{"time", "tokens", "cost", "delta"}
	statusFilters    = []string{"", deck.StatusCompleted, deck.StatusFailed, deck.StatusAbandoned}
)

type deckKeyMap struct {
	Up     key.Binding
	Down   key.Binding
	Enter  key.Binding
	Back   key.Binding
	Sort   key.Binding
	Filter key.Binding
	Track  key.Binding
	Replay key.Binding
	Quit   key.Binding
}

func (k deckKeyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Down, k.Up, k.Enter, k.Back, k.Sort, k.Filter, k.Track, k.Replay, k.Quit}
}

func (k deckKeyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{{k.Down, k.Up, k.Enter, k.Back}, {k.Sort, k.Filter, k.Track, k.Replay, k.Quit}}
}

func defaultKeyMap() deckKeyMap {
	return deckKeyMap{
		Up:     key.NewBinding(key.WithKeys("k", "up"), key.WithHelp("k", "up")),
		Down:   key.NewBinding(key.WithKeys("j", "down"), key.WithHelp("j", "down")),
		Enter:  key.NewBinding(key.WithKeys("enter", "l"), key.WithHelp("enter", "drill")),
		Back:   key.NewBinding(key.WithKeys("h", "esc"), key.WithHelp("h", "back")),
		Sort:   key.NewBinding(key.WithKeys("s"), key.WithHelp("s", "sort")),
		Filter: key.NewBinding(key.WithKeys("f"), key.WithHelp("f", "status")),
		Track:  key.NewBinding(key.WithKeys("1", "2", "3", "4", "5", "6", "7", "8", "9"), key.WithHelp("1-9", "sessions")),
		Replay: key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "replay")),
		Quit:   key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
	}
}

type sessionLoadedMsg struct {
	detail *deck.SessionDetail
	err    error
}

type overviewLoadedMsg struct {
	overview *deck.Overview
	err      error
}

type replayTickMsg time.Time

func runDeckTUI(ctx context.Context, query *deck.Query, filters deck.Filters) error {
	overview, err := query.Overview(ctx, filters)
	if err != nil {
		return err
	}

	model := newDeckModel(query, filters, overview)

	if filters.Session != "" {
		detail, err := query.SessionDetail(ctx, filters.Session)
		if err != nil {
			return err
		}
		model.view = viewSession
		model.detail = detail
	}

	program := bubbletea.NewProgram(model,
		bubbletea.WithContext(ctx),
		bubbletea.WithAltScreen(),
	)
	_, err = program.Run()
	return err
}

func newDeckModel(query *deck.Query, filters deck.Filters, overview *deck.Overview) deckModel {
	toggles := map[int]bool{}
	for i := range trackedSessionShortcuts {
		toggles[i] = true
	}

	sortIndex := 0
	for i, sortKey := range sortOrder {
		if sortKey == filters.Sort {
			sortIndex = i
		}
	}

	statusIndex := 0
	for i, status := range statusFilters {
		if status == filters.Status {
			statusIndex = i
		}
	}

	return deckModel{
		query:        query,
		filters:      filters,
		overview:     overview,
		view:         viewOverview,
		trackToggles: toggles,
		sortIndex:    sortIndex,
		statusIndex:  statusIndex,
		messageSort:  0,
		keys:         defaultKeyMap(),
		help:         help.New(),
	}
}

func (m deckModel) Init() bubbletea.Cmd {
	return nil
}

func (m deckModel) Update(msg bubbletea.Msg) (bubbletea.Model, bubbletea.Cmd) {
	switch msg := msg.(type) {
	case bubbletea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case overviewLoadedMsg:
		if msg.err != nil {
			return m, nil
		}
		m.overview = msg.overview
		if m.cursor >= len(m.overview.Sessions) {
			m.cursor = clamp(m.cursor, len(m.overview.Sessions)-1)
		}
		return m, nil
	case sessionLoadedMsg:
		if msg.err != nil {
			return m, nil
		}
		m.detail = msg.detail
		m.messageCursor = 0
		m.view = viewSession
		m.messageSort = 0
		if m.replayOnLoad {
			m.replayOnLoad = false
			m.replayActive = true
			return m, replayTick()
		}
		return m, nil
	case replayTickMsg:
		if !m.replayActive || m.detail == nil {
			return m, nil
		}
		if m.messageCursor >= len(m.sortedMessages())-1 {
			m.replayActive = false
			return m, nil
		}
		m.messageCursor++
		return m, replayTick()
	case bubbletea.KeyMsg:
		return m.handleKey(msg)
	}

	return m, nil
}

func (m deckModel) View() string {
	switch m.view {
	case viewOverview:
		return m.viewOverview()
	case viewSession:
		return m.viewSession()
	}
	return m.viewOverview()
}

func (m deckModel) handleKey(msg bubbletea.KeyMsg) (bubbletea.Model, bubbletea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "q":
		return m, bubbletea.Quit
	case "j", "down":
		return m.moveCursor(1)
	case "k", "up":
		return m.moveCursor(-1)
	case "l", "enter":
		if m.view == viewOverview {
			return m.enterSession()
		}
	case "h", "esc":
		if m.view == viewSession {
			m.view = viewOverview
			m.replayActive = false
		}
	case "s":
		if m.view == viewOverview {
			return m.cycleSort()
		}
		if m.view == viewSession {
			return m.cycleMessageSort()
		}
	case "f":
		if m.view == viewOverview {
			return m.cycleStatus()
		}
	case "r":
		if m.view == viewSession {
			if m.replayActive {
				m.replayActive = false
				return m, nil
			}
			m.replayActive = true
			m.messageCursor = 0
			return m, replayTick()
		}
		if m.view == viewOverview {
			if len(m.overview.Sessions) == 0 {
				return m, nil
			}
			m.replayOnLoad = true
			return m.enterSession()
		}
	}

	if m.view == viewOverview {
		if idx, ok := numberKey(msg.String()); ok {
			m.toggleTrack(idx)
		}
	}

	return m, nil
}

func (m deckModel) moveCursor(delta int) (bubbletea.Model, bubbletea.Cmd) {
	if m.view == viewOverview {
		if len(m.overview.Sessions) == 0 {
			return m, nil
		}
		m.cursor = clamp(m.cursor+delta, len(m.overview.Sessions)-1)
		return m, nil
	}

	if m.detail == nil || len(m.detail.Messages) == 0 {
		return m, nil
	}
	m.messageCursor = clamp(m.messageCursor+delta, len(m.detail.Messages)-1)
	return m, nil
}

func (m deckModel) enterSession() (bubbletea.Model, bubbletea.Cmd) {
	if len(m.overview.Sessions) == 0 {
		return m, nil
	}

	session := m.overview.Sessions[m.cursor]
	return m, loadSessionCmd(m.query, session.ID)
}

func (m deckModel) cycleSort() (bubbletea.Model, bubbletea.Cmd) {
	m.sortIndex = (m.sortIndex + 1) % len(sortOrder)
	m.filters.Sort = sortOrder[m.sortIndex]
	return m, loadOverviewCmd(m.query, m.filters)
}

func (m deckModel) cycleStatus() (bubbletea.Model, bubbletea.Cmd) {
	m.statusIndex = (m.statusIndex + 1) % len(statusFilters)
	m.filters.Status = statusFilters[m.statusIndex]
	return m, loadOverviewCmd(m.query, m.filters)
}

func (m deckModel) cycleMessageSort() (bubbletea.Model, bubbletea.Cmd) {
	m.messageSort = (m.messageSort + 1) % len(messageSortOrder)
	if len(m.sortedMessages()) == 0 {
		m.messageCursor = 0
		return m, nil
	}
	m.messageCursor = clamp(m.messageCursor, len(m.sortedMessages())-1)
	return m, nil
}

func (m deckModel) toggleTrack(idx int) {
	if idx < 0 || idx >= trackedSessionShortcuts {
		return
	}
	m.trackToggles[idx] = !m.trackToggles[idx]
}

func (m deckModel) viewOverview() string {
	if m.overview == nil {
		return deckMutedStyle.Render("no data")
	}

	selected, filtered := m.selectedSessions()
	stats := summarizeSessions(selected)

	lastWindow := formatDuration(stats.TotalDuration)
	headerLeft := deckTitleStyle.Render("tapes deck")
	headerRight := deckMutedStyle.Render(m.headerSessionCount(lastWindow, len(selected), len(m.overview.Sessions), filtered))
	header := renderHeaderLine(m.width, headerLeft, headerRight)
	lines := make([]string, 0, 10)
	lines = append(lines, header, renderRule(m.width), "")

	lines = append(lines, m.viewMetrics(stats))
	lines = append(lines, "", m.viewCostByModel(stats), "", m.viewSessionList(), "", m.viewFooter())

	return strings.Join(lines, "\n")
}

func (m deckModel) viewMetrics(stats deckOverviewStats) string {
	avgCost := safeDivide(stats.TotalCost, float64(max(1, stats.TotalSessions)))
	avgTime := time.Duration(int64(stats.TotalDuration) / int64(max(1, stats.TotalSessions)))
	avgTools := stats.TotalToolCalls / max(1, stats.TotalSessions)

	headers := []string{"TOTAL SPEND", "TOKENS USED", "AGENT TIME", "TOOL CALLS", "SUCCESS"}
	values := []string{
		formatCost(stats.TotalCost),
		fmt.Sprintf("%s in %s out", formatTokens(stats.InputTokens), formatTokens(stats.OutputTokens)),
		formatDuration(stats.TotalDuration),
		strconv.Itoa(stats.TotalToolCalls),
		formatPercent(stats.SuccessRate),
	}
	avgValues := []string{
		formatCost(avgCost) + " avg",
		fmt.Sprintf("%s in %s out", formatTokens(avgTokenCount(stats.InputTokens, stats.TotalSessions)), formatTokens(avgTokenCount(stats.OutputTokens, stats.TotalSessions))),
		formatDuration(avgTime) + " avg",
		fmt.Sprintf("%d avg", avgTools),
		fmt.Sprintf("%d/%d", stats.Completed, stats.TotalSessions),
	}

	lines := []string{
		renderMetricRow(m.width, headers, deckMetricLabel),
		renderMetricRow(m.width, values, deckMetricValue),
		deckMutedStyle.Render(renderMetricRow(m.width, avgValues, deckMutedStyle)),
	}

	return strings.Join(lines, "\n")
}

func (m deckModel) viewCostByModel(stats deckOverviewStats) string {
	if len(stats.CostByModel) == 0 {
		return deckMutedStyle.Render("cost by model: no data")
	}

	lines := []string{deckSectionStyle.Render("cost by model"), renderRule(m.width)}
	maxCost := 0.0
	for _, cost := range stats.CostByModel {
		if cost.TotalCost > maxCost {
			maxCost = cost.TotalCost
		}
	}

	for _, cost := range sortedModelCosts(stats.CostByModel) {
		bar := renderBar(cost.TotalCost, maxCost, 24)
		line := fmt.Sprintf("* %-16s %s %s  %d sessions", cost.Model, deckAccentStyle.Render(bar), formatCost(cost.TotalCost), cost.SessionCount)
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

func (m deckModel) viewSessionList() string {
	if len(m.overview.Sessions) == 0 {
		return deckMutedStyle.Render("sessions: no data")
	}

	status := m.filters.Status
	if status == "" {
		status = "all"
	}
	lines := []string{deckSectionStyle.Render(fmt.Sprintf("sessions (sort: %s, status: %s)", m.filters.Sort, status)), renderRule(m.width)}
	lines = append(lines, deckMutedStyle.Render("  label           model        dur     tokens    cost    tools  msgs  status"))
	for i := range m.overview.Sessions {
		session := m.overview.Sessions[i]
		cursor := " "
		if i == m.cursor {
			cursor = ">"
		}

		var toggle string
		if i < trackedSessionShortcuts {
			if m.trackToggles[i] {
				toggle = strconv.Itoa(i + 1)
			} else {
				toggle = "-"
			}
		} else {
			toggle = " "
		}

		statusValue := session.Status
		statusStyle := statusStyleFor(statusValue)
		line := fmt.Sprintf("%s %s %-14s %-12s %6s %9s %8s  %5d  %4d  %s",
			cursor,
			toggle,
			truncateText(session.Label, 14),
			truncateText(session.Model, 12),
			formatDuration(session.Duration),
			formatTokens(session.InputTokens+session.OutputTokens),
			formatCost(session.TotalCost),
			session.ToolCalls,
			session.MessageCount,
			statusStyle.Render(statusValue),
		)

		if i < trackedSessionShortcuts && !m.trackToggles[i] {
			line = deckDimStyle.Render(line)
		}
		if i == m.cursor {
			line = deckHighlightStyle.Render(line)
		}

		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

func (m deckModel) viewSession() string {
	if m.detail == nil {
		return deckMutedStyle.Render("no session selected")
	}

	statusStyle := statusStyleFor(m.detail.Summary.Status)
	statusDot := statusStyle.Render("●")
	headerLeft := deckTitleStyle.Render("⏏ tapes deck › " + m.detail.Summary.Label)
	headerRight := deckMutedStyle.Render(fmt.Sprintf("%s · %s %s", m.detail.Summary.ID, statusDot, m.detail.Summary.Status))
	header := renderHeaderLine(m.width, headerLeft, headerRight)
	lines := make([]string, 0, 20)
	lines = append(lines, header, renderRule(m.width), "")

	lines = append(lines, deckSectionStyle.Render("session"), renderRule(m.width))
	lines = append(lines, deckMutedStyle.Render("MODEL           DURATION        INPUT COST      OUTPUT COST     TOTAL"))
	lines = append(lines, fmt.Sprintf("%-15s %-15s %-14s %-14s %s",
		truncateText(m.detail.Summary.Model, 15),
		formatDuration(m.detail.Summary.Duration),
		formatCost(m.detail.Summary.InputCost),
		formatCost(m.detail.Summary.OutputCost),
		formatCost(m.detail.Summary.TotalCost),
	))

	lines = append(lines, deckMutedStyle.Render(fmt.Sprintf("%-15s %-15s %-14s %-14s",
		"",
		"",
		formatTokens(m.detail.Summary.InputTokens)+" tokens",
		formatTokens(m.detail.Summary.OutputTokens)+" tokens",
	)))

	inputRate, outputRate := costPerMTok(m.detail.Summary.InputCost, m.detail.Summary.InputTokens), costPerMTok(m.detail.Summary.OutputCost, m.detail.Summary.OutputTokens)
	lines = append(lines, deckMutedStyle.Render(fmt.Sprintf("%-15s %-15s $%.2f/$%.2f MTok",
		"",
		"",
		inputRate,
		outputRate,
	)))

	inputPercent, outputPercent := splitPercent(m.detail.Summary.InputCost, m.detail.Summary.OutputCost)
	lines = append(lines, "")
	lines = append(lines, renderSplitBar("input", inputPercent, 26))
	lines = append(lines, renderSplitBar("output", outputPercent, 26))

	costPerMessage := safeDivide(m.detail.Summary.TotalCost, float64(max(1, m.detail.Summary.MessageCount)))
	costPerMinute := costPerMinute(m.detail.Summary.TotalCost, m.detail.Summary.Duration)
	toolsPerTurn := float64(m.detail.Summary.ToolCalls) / float64(max(1, m.detail.Summary.MessageCount))
	lines = append(lines, "")
	lines = append(lines, deckMutedStyle.Render(fmt.Sprintf("cost/message: %s    cost/minute: %s    tools/turn: %.1f",
		formatCost(costPerMessage),
		formatCost(costPerMinute),
		toolsPerTurn,
	)))

	screenHeight := m.height
	if screenHeight <= 0 {
		screenHeight = 40
	}
	footerHeight := 2
	remaining := max(screenHeight-len(lines)-footerHeight, 8)
	gap := 3
	leftWidth := max((m.width-gap)*2/3, 30)
	rightWidth := m.width - gap - leftWidth
	if rightWidth < 24 {
		rightWidth = 24
		leftWidth = m.width - gap - rightWidth
	}

	leftBlock := m.renderTimelineBlock(leftWidth, remaining)
	rightBlock := m.renderDetailBlock(rightWidth, remaining)
	lines = append(lines, joinColumns(leftBlock, rightBlock, gap)...)

	lines = append(lines, "", m.viewSessionFooter())

	return strings.Join(lines, "\n")
}

func (m deckModel) viewFooter() string {
	return deckMutedStyle.Render(m.help.View(m.keys))
}

func (m deckModel) viewSessionFooter() string {
	return deckMutedStyle.Render(m.help.View(m.keys))
}

func loadOverviewCmd(query *deck.Query, filters deck.Filters) bubbletea.Cmd {
	return func() bubbletea.Msg {
		overview, err := query.Overview(context.Background(), filters)
		return overviewLoadedMsg{overview: overview, err: err}
	}
}

func loadSessionCmd(query *deck.Query, sessionID string) bubbletea.Cmd {
	return func() bubbletea.Msg {
		detail, err := query.SessionDetail(context.Background(), sessionID)
		return sessionLoadedMsg{detail: detail, err: err}
	}
}

func replayTick() bubbletea.Cmd {
	return bubbletea.Tick(300*time.Millisecond, func(t time.Time) bubbletea.Msg {
		return replayTickMsg(t)
	})
}

func sortedModelCosts(costs map[string]deck.ModelCost) []deck.ModelCost {
	items := make([]deck.ModelCost, 0, len(costs))
	for _, cost := range costs {
		items = append(items, cost)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].TotalCost > items[j].TotalCost
	})

	return items
}

func numberKey(key string) (int, bool) {
	switch key {
	case "1", "2", "3", "4", "5", "6", "7", "8", "9":
		return int(key[0] - '1'), true
	default:
		return 0, false
	}
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

func formatCost(value float64) string {
	return fmt.Sprintf("$%.3f", value)
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
	seconds := int(value.Seconds()) % 60
	hours := minutes / 60
	minutes %= 60
	if hours > 0 {
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
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

func renderMetricRow(width int, items []string, style lipgloss.Style) string {
	if len(items) == 0 {
		return ""
	}
	lineWidth := width
	if lineWidth <= 0 {
		lineWidth = 80
	}
	cols := len(items)
	spaceWidth := (cols - 1) * 2
	colWidth := max((lineWidth-spaceWidth)/cols, 12)
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, style.Render(fitCell(item, colWidth)))
	}
	return strings.Join(parts, "  ")
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

func renderSplitBar(label string, percent float64, width int) string {
	if width <= 0 {
		width = 24
	}
	filled := min(max(int((percent/100)*float64(width)), 0), width)
	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	return fmt.Sprintf("%s %s  %2.0f%%", label, bar, percent)
}

func renderSectionDivider(width int, title string) string {
	lineWidth := width
	if lineWidth <= 0 {
		lineWidth = 80
	}
	label := fmt.Sprintf("─── %s ", title)
	remaining := lineWidth - lipgloss.Width(label) - 2
	if remaining < 0 {
		return "  " + label
	}
	return "  " + label + strings.Repeat("─", remaining)
}

func (m deckModel) renderTimelineBlock(width, height int) []string {
	lines := []string{renderSectionDivider(width, fmt.Sprintf("timeline (sort: %s)", messageSortOrder[m.messageSort]))}
	if m.detail == nil || len(m.detail.Messages) == 0 {
		lines = append(lines, deckMutedStyle.Render("no messages"))
		return padLines(lines, width, height)
	}
	if height < 3 {
		height = 3
	}
	maxVisible := max(height-1, 1)

	messages := m.sortedMessages()
	start, end := visibleRange(len(messages), m.messageCursor, maxVisible)
	for i := start; i < end; i++ {
		msg := messages[i]
		cursor := " "
		if i == m.messageCursor {
			cursor = ">"
		}

		role := roleLabel(msg.Role)
		tools := formatTools(msg.ToolCalls)
		line := renderTimelineLine(
			width,
			cursor,
			msg.Timestamp.Format("15:04:05"),
			role,
			formatTokensDetail(msg.TotalTokens),
			formatCost(msg.TotalCost),
			tools,
			formatDelta(msg.Delta),
		)

		if i == m.messageCursor {
			line = deckHighlightStyle.Render(line)
		}

		lines = append(lines, line)
	}

	return padLines(lines, width, height)
}

func (m deckModel) renderDetailBlock(width, height int) []string {
	lines := []string{renderSectionDivider(width, "details | message")}
	if m.detail == nil || len(m.detail.Messages) == 0 {
		lines = append(lines, deckMutedStyle.Render("no message"))
		return padLines(lines, width, height)
	}
	if height < 3 {
		height = 3
	}
	messages := m.sortedMessages()
	if len(messages) == 0 {
		lines = append(lines, deckMutedStyle.Render("no message"))
		return padLines(lines, width, height)
	}
	msg := messages[m.messageCursor]
	role := roleLabel(msg.Role)

	lines = append(lines,
		"role: "+role,
		fmt.Sprintf("time: %s  delta: %s", msg.Timestamp.Format("15:04:05"), formatDelta(msg.Delta)),
		fmt.Sprintf("tokens: in %s  out %s  total %s", formatTokensDetail(msg.InputTokens), formatTokensDetail(msg.OutputTokens), formatTokensDetail(msg.TotalTokens)),
		fmt.Sprintf("cost:   in %s  out %s  total %s", formatCost(msg.InputCost), formatCost(msg.OutputCost), formatCost(msg.TotalCost)),
	)

	tools := ""
	if len(msg.ToolCalls) > 0 {
		tools = "tools: " + strings.Join(msg.ToolCalls, " ")
	}
	if tools != "" {
		lines = append(lines, wrapText(tools, max(20, width-2))...)
	}

	text := strings.TrimSpace(msg.Text)
	if text != "" {
		lines = append(lines, "message:")
		lines = append(lines, wrapText(text, max(20, width-2))...)
	}

	return padLines(lines, width, height)
}

func renderTimelineLine(width int, cursor, timestamp, role, tokens, cost, tools, delta string) string {
	lineWidth := width
	if lineWidth <= 0 {
		lineWidth = 80
	}
	gap := 2
	cursorWidth := 1
	timeWidth := 8
	roleWidth := 12
	tokensWidth := 9
	costWidth := 7
	deltaWidth := 6
	baseWidth := cursorWidth + timeWidth + roleWidth + tokensWidth + costWidth + deltaWidth + gap*6
	toolWidth := max(lineWidth-baseWidth, 0)

	columns := []string{
		fitCell(cursor, cursorWidth),
		fitCell(timestamp, timeWidth),
		fitCell(role, roleWidth),
		fitCellRight(tokens, tokensWidth),
		fitCellRight(cost, costWidth),
		fitCellRight(delta, deltaWidth),
	}

	if toolWidth > 0 {
		columns = append(columns, fitCell(truncateText(tools, toolWidth), toolWidth))
	} else {
		columns = append(columns, "")
	}

	return strings.Join(columns, "  ")
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

func (m deckModel) selectedSessions() ([]deck.SessionSummary, bool) {
	if m.overview == nil || len(m.overview.Sessions) == 0 {
		return nil, false
	}

	maxVisible := min(trackedSessionShortcuts-1, len(m.overview.Sessions)-1)
	filtered := false
	for i := 0; i <= maxVisible; i++ {
		if !m.trackToggles[i] {
			filtered = true
			break
		}
	}
	if !filtered {
		return m.overview.Sessions, false
	}

	selected := make([]deck.SessionSummary, 0, maxVisible+1)
	for i := 0; i <= maxVisible; i++ {
		if m.trackToggles[i] {
			selected = append(selected, m.overview.Sessions[i])
		}
	}

	return selected, true
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
	messages := make([]deck.SessionMessage, len(m.detail.Messages))
	copy(messages, m.detail.Messages)
	sortKey := messageSortOrder[m.messageSort%len(messageSortOrder)]
	sort.SliceStable(messages, func(i, j int) bool {
		switch sortKey {
		case "tokens":
			if messages[i].TotalTokens == messages[j].TotalTokens {
				return messages[i].Timestamp.Before(messages[j].Timestamp)
			}
			return messages[i].TotalTokens > messages[j].TotalTokens
		case "cost":
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

	return messages
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
	lineWidth := lipgloss.Width(value)
	if lineWidth >= width {
		return value
	}
	return value + strings.Repeat(" ", width-lineWidth)
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

func visibleRange(total, cursor, size int) (int, int) {
	if total <= 0 || size <= 0 {
		return 0, 0
	}
	if total <= size {
		return 0, total
	}
	if cursor < 0 {
		cursor = 0
	}
	if cursor >= total {
		cursor = total - 1
	}
	start := max(cursor-(size/2), 0)
	end := start + size
	if end > total {
		end = total
		start = max(end-size, 0)
	}
	return start, end
}

func costPerMTok(cost float64, tokens int64) float64 {
	if tokens <= 0 {
		return 0
	}
	return (cost / float64(tokens)) * 1_000_000
}

func safeDivide(value, divisor float64) float64 {
	if divisor == 0 {
		return 0
	}
	return value / divisor
}

func costPerMinute(totalCost float64, duration time.Duration) float64 {
	minutes := duration.Minutes()
	if minutes <= 0 {
		return 0
	}
	return totalCost / minutes
}

func formatDelta(value time.Duration) string {
	if value <= 0 {
		return ""
	}
	return "+" + formatDuration(value)
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

func formatTools(toolCalls []string) string {
	if len(toolCalls) == 0 {
		return ""
	}
	list := strings.Join(toolCalls, " ")
	return "⚡" + list
}

func roleLabel(role string) string {
	switch role {
	case "assistant":
		return deckRoleAsstStyle.Render("● assistant")
	case "user":
		return deckRoleUserStyle.Render("○ user")
	default:
		return role
	}
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
