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
	"github.com/charmbracelet/x/ansi"
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
	viewModal
)

type timePeriod int

const (
	period30d timePeriod = iota
	period3m
	period6m
)

const (
	horizontalPadding = 2
	verticalPadding   = 1
)

const (
	sortKeyCost   = "cost"
	roleUser      = "user"
	roleAssistant = "assistant"
	circleLarge   = "⬤"
)

const (
	sessionListChromeLines   = 3
	sessionListPositionLines = 2
)

type deckModel struct {
	query         deck.Querier
	filters       deck.Filters
	overview      *deck.Overview
	detail        *deck.SessionDetail
	view          deckView
	cursor        int
	scrollOffset  int
	messageCursor int
	width         int
	height        int
	sortIndex     int
	statusIndex   int
	messageSort   int
	timePeriod    timePeriod
	modalCursor   int
	modalTab      modalTab
	replayActive  bool
	replayOnLoad  bool
	refreshEvery  time.Duration
	keys          deckKeyMap
	help          help.Model
}

type modalTab int

const (
	modalSort modalTab = iota
	modalFilter
)

var (
	// Core color palette - modern, high-contrast design
	colorForeground  = lipgloss.Color("#E6E4D9") // BoneParchment - primary text
	colorRed         = lipgloss.Color("#FF6B4A") // Vibrant Orange - primary accent
	colorGreen       = lipgloss.Color("#4DA667") // Forest Green
	colorYellow      = lipgloss.Color("#F2B84B") // Caution Gold
	colorBlue        = lipgloss.Color("#4EB1E9") // Electric Cyan
	colorMagenta     = lipgloss.Color("#B656B1") // Royal Purple
	colorBrightBlack = lipgloss.Color("#4A4A4A") // Muted Slate - inactive elements
	colorDimmed      = lipgloss.Color("#2A2A2B") // Dimmed Obsidian - subtle elements

	// Complementary shades for UI depth
	colorHighlightBg = lipgloss.Color("#252526") // Subtle highlight background
	colorPanelBg     = lipgloss.Color("#212122") // Panel background (slightly lighter than main)

	// Orange gradient for cost visualization (light to bright)
	costOrangeGradient = []string{
		"#B6512B", // Dim orange (cheapest)
		"#D96840", // Medium orange
		"#FF7A45", // Bright orange
		"#FF8F4D", // Hot orange
		"#FFB25A", // Hottest orange (most expensive)
	}

	// UI element styles using the palette
	deckTitleStyle       = lipgloss.NewStyle().Bold(true).Foreground(colorYellow)
	deckMutedStyle       = lipgloss.NewStyle().Foreground(colorBrightBlack)
	deckAccentStyle      = lipgloss.NewStyle().Foreground(colorRed) // Primary accent is now orange
	deckDimStyle         = lipgloss.NewStyle().Foreground(colorDimmed)
	deckSectionStyle     = lipgloss.NewStyle().Bold(true).Foreground(colorForeground)
	deckDividerStyle     = lipgloss.NewStyle().Foreground(colorDimmed)
	deckHighlightStyle   = lipgloss.NewStyle().Background(colorHighlightBg)
	deckStatusOKStyle    = lipgloss.NewStyle().Foreground(colorGreen)
	deckStatusFailStyle  = lipgloss.NewStyle().Foreground(colorRed)
	deckStatusWarnStyle  = lipgloss.NewStyle().Foreground(colorYellow)
	deckRoleUserStyle    = lipgloss.NewStyle().Foreground(colorBlue)
	deckRoleAsstStyle    = lipgloss.NewStyle().Foreground(colorRed) // Assistant uses primary accent orange
	deckModalBgStyle     = lipgloss.NewStyle().Background(colorPanelBg).Foreground(colorForeground).Padding(1, 2)
	deckTabBoxStyle      = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(colorDimmed).Padding(0, 1)
	deckTabActiveStyle   = lipgloss.NewStyle().Bold(true).Foreground(colorForeground)
	deckTabInactiveStyle = lipgloss.NewStyle().Foreground(colorBrightBlack)
)

// Model color schemes by provider (intensity indicates tier/usage)
// Using the new color palette
var (
	// Anthropic - Royal Purple family
	claudeColors = map[string]string{
		"opus":   "#D97BC1", // Bright purple (highest tier)
		"sonnet": "#B656B1", // Base Royal Purple
		"haiku":  "#8E3F8A", // Deep purple (lowest tier)
	}
	// OpenAI - Cyan family
	openaiColors = map[string]string{
		"gpt-4o":      "#7DD9FF", // Bright cyan
		"gpt-4":       "#4EB1E9", // Base cyan
		"gpt-4o-mini": "#3889B8", // Medium cyan
		"gpt-3.5":     "#2A6588", // Dark cyan
	}
	// Google - Electric Cyan family
	googleColors = map[string]string{
		"gemini-2.0":     "#7DD9FF", // Bright cyan
		"gemini-1.5-pro": "#4EB1E9", // Base Electric Cyan
		"gemini-1.5":     "#3889B8", // Medium cyan
		"gemma":          "#2A6588", // Dark cyan
	}
)

var (
	sortOrder        = []string{sortKeyCost, "time", "tokens", "duration"}
	sortDirOptions   = []string{"asc", sortDirDesc}
	messageSortOrder = []string{"time", "tokens", sortKeyCost, "delta"}
	statusFilters    = []string{"", deck.StatusCompleted, deck.StatusFailed, deck.StatusAbandoned}
)

type deckKeyMap struct {
	Up     key.Binding
	Down   key.Binding
	Enter  key.Binding
	Back   key.Binding
	Sort   key.Binding
	Filter key.Binding
	Period key.Binding
	Replay key.Binding
	Quit   key.Binding
}

func (k deckKeyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Down, k.Up, k.Enter, k.Back, k.Sort, k.Filter, k.Period, k.Replay, k.Quit}
}

func (k deckKeyMap) FullHelp() [][]key.Binding {
	return [][]key.Binding{{k.Down, k.Up, k.Enter, k.Back}, {k.Sort, k.Filter, k.Period, k.Replay, k.Quit}}
}

func defaultKeyMap() deckKeyMap {
	return deckKeyMap{
		Up:     key.NewBinding(key.WithKeys("k", "up"), key.WithHelp("k", "up")),
		Down:   key.NewBinding(key.WithKeys("j", "down"), key.WithHelp("j", "down")),
		Enter:  key.NewBinding(key.WithKeys("enter", "l"), key.WithHelp("enter", "drill")),
		Back:   key.NewBinding(key.WithKeys("h", "esc"), key.WithHelp("h", "back")),
		Sort:   key.NewBinding(key.WithKeys("s"), key.WithHelp("s", "sort")),
		Filter: key.NewBinding(key.WithKeys("f"), key.WithHelp("f", "status")),
		Period: key.NewBinding(key.WithKeys("p"), key.WithHelp("p", "period")),
		Replay: key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "replay")),
		Quit:   key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
	}
}

type sessionLoadedMsg struct {
	detail *deck.SessionDetail
	err    error
	keepUI bool
}

type overviewLoadedMsg struct {
	overview *deck.Overview
	err      error
}

type replayTickMsg time.Time

type refreshTickMsg time.Time

// RunDeckTUI starts the deck TUI with the provided query implementation.
// This function is exported to allow sandbox and testing environments to inject mock data.
func RunDeckTUI(ctx context.Context, query deck.Querier, filters deck.Filters, refreshEvery time.Duration) error {
	overview, err := query.Overview(ctx, filters)
	if err != nil {
		return err
	}

	model := newDeckModel(query, filters, overview, refreshEvery)

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

func newDeckModel(query deck.Querier, filters deck.Filters, overview *deck.Overview, refreshEvery time.Duration) deckModel {
	if filters.Sort == "" {
		filters.Sort = sortKeyCost
	}
	if filters.SortDir == "" {
		filters.SortDir = sortDirDesc
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

	// Determine initial time period from filters
	period := period30d
	if filters.Since > 0 {
		if filters.Since >= 180*24*time.Hour {
			period = period6m
		} else if filters.Since >= 90*24*time.Hour {
			period = period3m
		}
	}

	return deckModel{
		query:        query,
		filters:      filters,
		overview:     overview,
		view:         viewOverview,
		sortIndex:    sortIndex,
		statusIndex:  statusIndex,
		messageSort:  0,
		timePeriod:   period,
		modalTab:     modalSort,
		refreshEvery: refreshEvery,
		keys:         defaultKeyMap(),
		help:         help.New(),
	}
}

func (m deckModel) Init() bubbletea.Cmd {
	if m.refreshEvery <= 0 {
		return nil
	}
	return refreshTick(m.refreshEvery)
}

func (m deckModel) Update(msg bubbletea.Msg) (bubbletea.Model, bubbletea.Cmd) {
	switch msg := msg.(type) {
	case bubbletea.WindowSizeMsg:
		m.width = msg.Width - (2 * horizontalPadding)
		m.height = msg.Height - (2 * verticalPadding)
		return m, nil
	case overviewLoadedMsg:
		if msg.err != nil {
			return m, nil
		}
		// Preserve cursor position by remembering the selected session ID
		var selectedSessionID string
		if m.overview != nil && m.cursor < len(m.overview.Sessions) {
			selectedSessionID = m.overview.Sessions[m.cursor].ID
		}

		m.overview = msg.overview

		// Try to find the previously selected session in the new list
		if selectedSessionID != "" {
			for i, session := range m.overview.Sessions {
				if session.ID == selectedSessionID {
					m.cursor = i
					// Clamp scroll offset to keep cursor visible
					visibleRows := sessionListVisibleRows(len(m.overview.Sessions), m.sessionListHeight())
					_, _, m.scrollOffset = stableVisibleRange(
						len(m.overview.Sessions), m.cursor, visibleRows, m.scrollOffset,
					)
					return m, nil
				}
			}
		}

		// If session not found or no previous selection, clamp cursor and reset scroll
		if m.cursor >= len(m.overview.Sessions) {
			m.cursor = clamp(m.cursor, len(m.overview.Sessions)-1)
		}
		m.scrollOffset = 0
		return m, nil
	case sessionLoadedMsg:
		if msg.err != nil {
			return m, nil
		}
		m.detail = msg.detail
		m.view = viewSession
		if msg.keepUI {
			maxCursor := len(m.sortedMessages()) - 1
			if maxCursor >= 0 {
				m.messageCursor = clamp(m.messageCursor, maxCursor)
			} else {
				m.messageCursor = 0
			}
			return m, nil
		}
		m.messageCursor = 0
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
	case refreshTickMsg:
		if m.refreshEvery <= 0 {
			return m, nil
		}
		refreshCmd := m.refreshCmd()
		if refreshCmd == nil {
			return m, refreshTick(m.refreshEvery)
		}
		return m, bubbletea.Batch(refreshTick(m.refreshEvery), refreshCmd)
	case bubbletea.KeyMsg:
		return m.handleKey(msg)
	}

	return m, nil
}

func (m deckModel) View() string {
	var base string
	switch m.view {
	case viewOverview:
		base = m.viewOverview()
	case viewSession:
		base = m.viewSession()
	case viewModal:
		base = m.viewOverview()
		return addPadding(m.overlayModal(base, m.viewModal()))
	default:
		base = m.viewOverview()
	}
	return addPadding(base)
}

func (m deckModel) handleKey(msg bubbletea.KeyMsg) (bubbletea.Model, bubbletea.Cmd) {
	// Handle modal views
	if m.view == viewModal {
		return m.handleModalKey(msg)
	}

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
			// Re-clamp scroll offset in case terminal was resized
			if m.overview != nil && len(m.overview.Sessions) > 0 {
				visibleRows := sessionListVisibleRows(len(m.overview.Sessions), m.sessionListHeight())
				_, _, m.scrollOffset = stableVisibleRange(
					len(m.overview.Sessions), m.cursor, visibleRows, m.scrollOffset,
				)
			}
		}
	case "s":
		if m.view == viewOverview {
			m.view = viewModal
			m.modalTab = modalSort
			m.modalCursor = m.sortIndex
			return m, nil
		}
		if m.view == viewSession {
			return m.cycleMessageSort()
		}
	case "f":
		if m.view == viewOverview {
			m.view = viewModal
			m.modalTab = modalFilter
			m.modalCursor = m.statusIndex
			return m, nil
		}
	case "p":
		if m.view == viewOverview {
			return m.cyclePeriod()
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

	return m, nil
}

func (m deckModel) handleModalKey(msg bubbletea.KeyMsg) (bubbletea.Model, bubbletea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "q":
		return m, bubbletea.Quit
	case "esc", "h":
		m.view = viewOverview
		return m, nil
	case "s":
		m.modalTab = modalSort
		m.modalCursor = m.sortIndex
		return m, nil
	case "f":
		m.modalTab = modalFilter
		m.modalCursor = m.statusIndex
		return m, nil
	case "left":
		m.modalTab = modalSort
		m.modalCursor = m.sortIndex
		return m, nil
	case "right":
		m.modalTab = modalFilter
		m.modalCursor = m.statusIndex
		return m, nil
	case "j", "down":
		switch m.modalTab {
		case modalSort:
			m.modalCursor = (m.modalCursor + 1) % (len(sortOrder) + len(sortDirOptions))
		case modalFilter:
			m.modalCursor = (m.modalCursor + 1) % len(statusFilters)
		}
		return m, nil
	case "k", "up":
		switch m.modalTab {
		case modalSort:
			m.modalCursor = (m.modalCursor - 1 + len(sortOrder) + len(sortDirOptions)) % (len(sortOrder) + len(sortDirOptions))
		case modalFilter:
			m.modalCursor = (m.modalCursor - 1 + len(statusFilters)) % len(statusFilters)
		}
		return m, nil
	case "enter", "l":
		switch m.modalTab {
		case modalSort:
			if m.modalCursor < len(sortOrder) {
				m.sortIndex = m.modalCursor
				m.filters.Sort = sortOrder[m.sortIndex]
			} else {
				dirIndex := m.modalCursor - len(sortOrder)
				if dirIndex >= 0 && dirIndex < len(sortDirOptions) {
					m.filters.SortDir = sortDirOptions[dirIndex]
				}
			}
			return m, loadOverviewCmd(m.query, m.filters)
		case modalFilter:
			m.statusIndex = m.modalCursor
			m.filters.Status = statusFilters[m.statusIndex]
			m.view = viewOverview
			return m, loadOverviewCmd(m.query, m.filters)
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
		// Update scroll offset to keep cursor visible without jumping
		visibleRows := sessionListVisibleRows(len(m.overview.Sessions), m.sessionListHeight())
		_, _, m.scrollOffset = stableVisibleRange(
			len(m.overview.Sessions), m.cursor, visibleRows, m.scrollOffset,
		)
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
	return m, loadSessionCmd(m.query, session.ID, false)
}

func (m deckModel) cyclePeriod() (bubbletea.Model, bubbletea.Cmd) {
	m.timePeriod = (m.timePeriod + 1) % 3
	m.filters.Since = periodToDuration(m.timePeriod)
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
	selected := m.selectedSessions()
	stats := summarizeSessions(selected)

	lastWindow := formatDuration(stats.TotalDuration)
	headerLeft := deckTitleStyle.Render("tapes deck")
	filtered := len(selected) != len(m.overview.Sessions)
	sessionCount := deckMutedStyle.Render(m.headerSessionCount(lastWindow, len(selected), len(m.overview.Sessions), filtered))

	cassetteLines := renderCassetteTape()

	header1 := renderHeaderLine(m.width, headerLeft, cassetteLines[0])
	header2 := renderHeaderLine(m.width, "", cassetteLines[1])
	header3 := renderHeaderLine(m.width, sessionCount, cassetteLines[2])

	metrics := m.viewMetrics(stats)
	insights := m.viewInsights(stats)
	costByModel := m.viewCostByModel(stats)

	lines := make([]string, 0, 10)
	lines = append(lines, header1, header2, header3, renderRule(m.width), "")
	lines = append(lines, metrics)
	if insights != "" {
		lines = append(lines, "", insights)
	}
	lines = append(lines, "", costByModel, "")

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
		return deckMutedStyle.Render("no data")
	}

	above, footer := m.overviewChrome()
	chromeLines := countWrappedLines(above, m.width) + countWrappedLines(footer, m.width) + 1 + 2*verticalPadding
	availableHeight := max(m.height-chromeLines, 5)

	return above + m.viewSessionList(availableHeight) + "\n\n" + footer
}

func (m deckModel) viewMetrics(stats deckOverviewStats) string {
	// Period selector header with box background for active
	periodLabel := periodToLabel(m.timePeriod)
	periods := []string{"30d", "3M", "6M"}
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
	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#8A8079")).Bold(true)
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
	lines := make([]string, 0, len(costs)+2)
	lines = append(lines, topBorder)

	barWidth := 15 // Bar width for cost visualization

	for _, cost := range costs {
		bar := renderBar(cost.TotalCost, maxCost, barWidth)
		// Use model color for the bar
		modelColorHex := getModelColor(cost.Model)
		coloredBar := lipgloss.NewStyle().Foreground(lipgloss.Color(modelColorHex)).Render(bar)

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

func getModelColor(model string) string {
	modelLower := strings.ToLower(model)

	// Check for Claude models
	for tier, color := range claudeColors {
		if strings.Contains(modelLower, tier) {
			return color
		}
	}

	// Check for OpenAI models
	for modelName, color := range openaiColors {
		if strings.Contains(modelLower, modelName) || strings.Contains(modelLower, strings.ReplaceAll(modelName, "-", "")) {
			return color
		}
	}

	// Check for Google models
	for modelName, color := range googleColors {
		if strings.Contains(modelLower, modelName) || strings.Contains(modelLower, strings.ReplaceAll(modelName, "-", "")) {
			return color
		}
	}

	return "#FF6B4A" // Default to vibrant orange
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

func (m deckModel) viewInsights(stats deckOverviewStats) string {
	if stats.TotalSessions == 0 {
		return deckMutedStyle.Render("insights: no data")
	}

	total := max(1, stats.TotalSessions)
	completedPct := safeDivide(float64(stats.Completed), float64(total))
	failedPct := safeDivide(float64(stats.Failed), float64(total))
	abandonedPct := safeDivide(float64(stats.Abandoned), float64(total))

	completedLabel := fmt.Sprintf("%s %s (%d)", statusStyleFor(deck.StatusCompleted).Render("●"), formatPercent(completedPct), stats.Completed)
	failedLabel := fmt.Sprintf("%s %s (%d)", statusStyleFor(deck.StatusFailed).Render("●"), formatPercent(failedPct), stats.Failed)
	abandonedLabel := fmt.Sprintf("%s %s (%d)", statusStyleFor(deck.StatusAbandoned).Render("●"), formatPercent(abandonedPct), stats.Abandoned)
	outcomes := fmt.Sprintf("outcomes: %s · %s · %s", completedLabel, failedLabel, abandonedLabel)

	costPerSession := safeDivide(stats.TotalCost, float64(total))
	costPerMin := costPerMinute(stats.TotalCost, stats.TotalDuration)
	tokensPerMin := tokensPerMinute(stats.InputTokens+stats.OutputTokens, stats.TotalDuration)
	efficiency := fmt.Sprintf("efficiency: %s/session · %s/min · %s tok/min", formatCost(costPerSession), formatCost(costPerMin), formatTokens(tokensPerMin))

	avgTools := safeDivide(float64(stats.TotalToolCalls), float64(total))
	tools := fmt.Sprintf("tools: %d total · %.1f avg/session", stats.TotalToolCalls, avgTools)

	lines := []string{
		deckMutedStyle.Render(outcomes),
		deckMutedStyle.Render(efficiency),
		deckMutedStyle.Render(tools),
	}

	return strings.Join(lines, "\n")
}

func (m deckModel) viewSessionList(availableHeight int) string {
	if len(m.overview.Sessions) == 0 {
		return deckMutedStyle.Render("sessions: no data")
	}

	visibleRows := sessionListVisibleRows(len(m.overview.Sessions), availableHeight)
	// Calculate which sessions to show using stable scrolling
	start, end, _ := stableVisibleRange(len(m.overview.Sessions), m.cursor, visibleRows, m.scrollOffset)
	maxVisible := end - start

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
	actions := "  " + sortBtn + "  " + filterBtn

	lines := []string{
		deckSectionStyle.Render(fmt.Sprintf("sessions (sort: %s %s, status: %s)", m.filters.Sort, sortDir, status)) + actions,
		renderRule(m.width),
	}

	// Calculate column widths based on actual content
	type rowData struct {
		label        string
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

	// Column width tracking
	maxLabelW := len("label")
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
		session := m.overview.Sessions[i]
		rowIdx := i - start

		rows[rowIdx].label = session.Label
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
	// Format: "  " + rowNum + " " + label + gap + model + gap + dur + gap + tokens + gap + barbell + gap + costInd + " " + cost + gap + tools + gap + msgs + gap + status
	colGap := 3
	fixedWidth := 2 + 1 + 1 + colGap + maxModelW + colGap + maxDurW + colGap + maxTokensW + colGap + maxBarbellW + colGap + maxCostIndW + 1 + maxCostW + colGap + maxToolsW + colGap + maxMsgsW + colGap + 1 + maxStatusW

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
		"status",
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
			padRightWithColor(rows[rowIdx].modelColored, maxModelW),
			padRight(rows[rowIdx].dur, maxDurW),
			padRight(rows[rowIdx].tokens, maxTokensW),
			barbellPadded, // Cost-weighted barbell visualization
			costIndPadded + " " + costPadded,
			padRight(rows[rowIdx].tools, maxToolsW),
			padRight(rows[rowIdx].msgs, maxMsgsW),
			rows[rowIdx].statusCircle + " " + rows[rowIdx].statusText,
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
	totalSessions := len(m.overview.Sessions)
	if totalSessions > maxVisible {
		position := fmt.Sprintf("showing %d-%d of %d", start+1, end, totalSessions)
		lines = append(lines, "", deckMutedStyle.Render(position))
	}

	return strings.Join(lines, "\n")
}

func (m deckModel) viewSession() string {
	if m.detail == nil {
		return deckMutedStyle.Render("no session selected")
	}

	// Breadcrumb navigation: tapes > session-name
	statusStyle := statusStyleFor(m.detail.Summary.Status)
	statusDot := statusStyle.Render("●")
	breadcrumb := deckAccentStyle.Render("tapes") + deckMutedStyle.Render(" > ") + deckTitleStyle.Render(m.detail.Summary.Label)
	headerRight := deckMutedStyle.Render(fmt.Sprintf("%s · %s %s", m.detail.Summary.ID, statusDot, m.detail.Summary.Status))
	header := renderHeaderLine(m.width, breadcrumb, headerRight)
	lines := make([]string, 0, 30)
	lines = append(lines, header, renderRule(m.width), "")

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

	labelStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#8A8079")).Bold(true)
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
	helpText := "j down • k up • enter drill • h back • s sort • f status • p period • r replay • q quit"
	return deckMutedStyle.Render(helpText)
}

func (m deckModel) viewSessionFooter() string {
	return deckMutedStyle.Render(m.help.View(m.keys))
}

func (m deckModel) viewModal() string {
	sortLabel := "Sort " + deckMutedStyle.Render("s")
	filterLabel := "Filter " + deckMutedStyle.Render("f")

	sortTab := deckTabActiveStyle.Render(sortLabel)
	filterTab := deckTabInactiveStyle.Render(filterLabel)
	if m.modalTab == modalFilter {
		sortTab = deckTabInactiveStyle.Render(sortLabel)
		filterTab = deckTabActiveStyle.Render(filterLabel)
	}

	tabSwitcher := deckTabBoxStyle.Render(sortTab + "  " + filterTab)

	bodyLines := []string{}
	if m.modalTab == modalSort {
		sortLabels := map[string]string{
			sortKeyCost: "Total Cost",
			"time":      "End Time",
			"tokens":    "Total Tokens",
			"duration":  "Duration",
		}

		for i, sortKey := range sortOrder {
			label := sortLabels[sortKey]
			if label == "" {
				label = sortKey
			}

			cursor := "  "
			switch i {
			case m.modalCursor:
				cursor = "> "
				label = deckHighlightStyle.Render(label)
			case m.sortIndex:
				label = deckAccentStyle.Render(label)
			}

			bodyLines = append(bodyLines, cursor+label)
		}

		bodyLines = append(bodyLines, "", deckMutedStyle.Render("Order"))

		orderLabels := map[string]string{
			"asc":       "Order: Ascending",
			sortDirDesc: "Order: Descending",
		}
		for i, dir := range sortDirOptions {
			label := orderLabels[dir]
			if label == "" {
				label = dir
			}

			cursor := "  "
			rowIndex := len(sortOrder) + i
			if rowIndex == m.modalCursor {
				cursor = "> "
				label = deckHighlightStyle.Render(label)
			} else if strings.EqualFold(m.filters.SortDir, dir) {
				label = deckAccentStyle.Render(label)
			}

			bodyLines = append(bodyLines, cursor+label)
		}
	} else {
		filterLabels := map[string]string{
			"":                   "All Sessions",
			deck.StatusCompleted: "Completed",
			deck.StatusFailed:    "Failed",
			deck.StatusAbandoned: "Abandoned",
		}

		for i, status := range statusFilters {
			label := filterLabels[status]
			if label == "" {
				label = "Unknown"
			}

			cursor := "  "
			switch i {
			case m.modalCursor:
				cursor = "> "
				label = deckHighlightStyle.Render(label)
			case m.statusIndex:
				label = deckAccentStyle.Render(label)
			}

			bodyLines = append(bodyLines, cursor+label)
		}
	}

	helpLine := deckMutedStyle.Render("↑↓ navigate • enter select • ←/→ tab • s sort • f filter • esc cancel")

	maxWidth := ansi.StringWidth(tabSwitcher)
	for _, line := range bodyLines {
		if width := ansi.StringWidth(line); width > maxWidth {
			maxWidth = width
		}
	}
	if width := ansi.StringWidth(helpLine); width > maxWidth {
		maxWidth = width
	}

	lines := make([]string, 0, 2+len(bodyLines)+2)
	lines = append(lines, lipgloss.PlaceHorizontal(maxWidth, lipgloss.Center, tabSwitcher), "")
	lines = append(lines, bodyLines...)
	lines = append(lines, "", helpLine)

	return deckModalBgStyle.Render(strings.Join(lines, "\n"))
}

func (m deckModel) overlayModal(base, modal string) string {
	baseLines := strings.Split(base, "\n")
	modalLines := strings.Split(modal, "\n")

	// Center the modal
	modalWidth := 0
	for _, line := range modalLines {
		if width := ansi.StringWidth(line); width > modalWidth {
			modalWidth = width
		}
	}
	modalHeight := len(modalLines)

	startY := max((m.height-modalHeight)/2, 2)
	startX := max((m.width-modalWidth)/2, 0)

	// Overlay modal on base
	for i, modalLine := range modalLines {
		y := startY + i
		if y >= 0 && y < len(baseLines) {
			baseLine := baseLines[y]
			baseWidth := ansi.StringWidth(baseLine)
			if m.width > 0 && baseWidth < m.width {
				baseLine += strings.Repeat(" ", m.width-baseWidth)
				baseWidth = m.width
			}

			if startX >= baseWidth {
				continue
			}

			available := baseWidth - startX
			if available <= 0 {
				continue
			}

			line := modalLine
			if ansi.StringWidth(line) > available {
				line = ansi.Truncate(line, available, "")
			}

			before := ansi.Cut(baseLine, 0, startX)
			afterStart := startX + ansi.StringWidth(line)
			after := ""
			if afterStart < baseWidth {
				after = ansi.Cut(baseLine, afterStart, baseWidth)
			}

			baseLines[y] = before + line + after
		}
	}

	return strings.Join(baseLines, "\n")
}

func loadOverviewCmd(query deck.Querier, filters deck.Filters) bubbletea.Cmd {
	return func() bubbletea.Msg {
		overview, err := query.Overview(context.Background(), filters)
		return overviewLoadedMsg{overview: overview, err: err}
	}
}

func loadSessionCmd(query deck.Querier, sessionID string, keepUI bool) bubbletea.Cmd {
	return func() bubbletea.Msg {
		detail, err := query.SessionDetail(context.Background(), sessionID)
		return sessionLoadedMsg{detail: detail, err: err, keepUI: keepUI}
	}
}

func replayTick() bubbletea.Cmd {
	return bubbletea.Tick(300*time.Millisecond, func(t time.Time) bubbletea.Msg {
		return replayTickMsg(t)
	})
}

func refreshTick(interval time.Duration) bubbletea.Cmd {
	return bubbletea.Tick(interval, func(t time.Time) bubbletea.Msg {
		return refreshTickMsg(t)
	})
}

func (m deckModel) refreshCmd() bubbletea.Cmd {
	if m.view == viewOverview {
		return loadOverviewCmd(m.query, m.filters)
	}
	if m.view == viewSession && m.detail != nil {
		return loadSessionCmd(m.query, m.detail.Summary.ID, true)
	}
	return nil
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
	case period30d:
		return 30 * 24 * time.Hour
	case period3m:
		return 90 * 24 * time.Hour
	case period6m:
		return 180 * 24 * time.Hour
	default:
		return 30 * 24 * time.Hour
	}
}

func periodToLabel(p timePeriod) string {
	switch p {
	case period30d:
		return "30d"
	case period3m:
		return "3M"
	case period6m:
		return "6M"
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
	for tier, color := range claudeColors {
		if strings.Contains(modelLower, tier) {
			return lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: color, Dark: color}).Render(model)
		}
	}

	// Check for OpenAI models
	for modelName, color := range openaiColors {
		if strings.Contains(modelLower, modelName) || strings.Contains(modelLower, strings.ReplaceAll(modelName, "-", "")) {
			return lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: color, Dark: color}).Render(model)
		}
	}

	// Check for Google models
	for modelName, color := range googleColors {
		if strings.Contains(modelLower, modelName) || strings.Contains(modelLower, strings.ReplaceAll(modelName, "-", "")) {
			return lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: color, Dark: color}).Render(model)
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

func (m deckModel) renderConversationTimeline() []string {
	lines := []string{}

	if m.detail == nil || len(m.detail.Messages) == 0 {
		lines = append(lines, deckSectionStyle.Render("timeline"))
		lines = append(lines, deckMutedStyle.Render("no messages"))
		return lines
	}

	messages := m.sortedMessages()
	if len(messages) == 0 {
		lines = append(lines, deckSectionStyle.Render("timeline"))
		lines = append(lines, deckMutedStyle.Render("no messages"))
		return lines
	}

	// Header with sort info
	sortLabel := messageSortOrder[m.messageSort%len(messageSortOrder)]
	headerLeft := deckSectionStyle.Render("conversation timeline") + "  " +
		deckMutedStyle.Render(fmt.Sprintf("(sort: %s, %d messages)", sortLabel, len(messages)))
	lines = append(lines, headerLeft)

	// Build waveform visualization
	waveformLines := m.buildWaveform(messages)
	lines = append(lines, waveformLines...)

	return lines
}

func (m deckModel) buildWaveform(messages []deck.SessionMessage) []string {
	// Calculate available width for waveform
	availWidth := m.width
	if availWidth <= 0 {
		availWidth = 80
	}

	// Reserve space for labels and padding
	axisWidth := 8

	// Find max tokens for scaling
	var maxTokens int64
	for _, msg := range messages {
		if msg.TotalTokens > maxTokens {
			maxTokens = msg.TotalTokens
		}
	}

	if maxTokens == 0 {
		maxTokens = 1
	}

	// Waveform bars (multi-line) to represent token volume
	maxBarHeight := 5
	lines := make([]string, 0, maxBarHeight+3)
	barLines := make([]string, maxBarHeight)
	toolMarkers := []string{}
	msgNumbers := []string{}
	gap := " "

	for i, msg := range messages {
		// Calculate bar height (1-maxBarHeight)
		ratio := float64(msg.TotalTokens) / float64(maxTokens)
		barHeight := int(ratio * float64(maxBarHeight))
		barHeight = min(max(barHeight, 1), maxBarHeight)

		// Choose bar style based on role
		var barStyle lipgloss.Style

		if msg.Role == roleUser {
			barStyle = deckRoleUserStyle // Cyan for user
		} else {
			barStyle = deckRoleAsstStyle // Orange for assistant
		}

		isCurrent := i == m.messageCursor
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
		case len(msg.ToolCalls) > 0:
			icon := toolUsageIcon(msg.ToolCalls)
			toolMarkers = append(toolMarkers, lipgloss.NewStyle().Foreground(colorYellow).Render(icon)+gap)
		default:
			toolMarkers = append(toolMarkers, " "+gap)
		}

		// Message number (show every 5 or at current position)
		switch {
		case i%5 == 0 || i == m.messageCursor:
			msgNumbers = append(msgNumbers, deckMutedStyle.Render(strconv.Itoa(i+1))+gap)
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
	barWidth := lipgloss.Width(barLines[0])
	for i := range maxBarHeight {
		label := ""
		switch i {
		case 0:
			label = "tokens"
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
		legendPad := max(1, availWidth-axisWidth-barWidth-legendWidth)
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
	lines = append(lines, header)

	if m.detail == nil || len(m.detail.Messages) == 0 {
		lines = append(lines, deckMutedStyle.Render("no messages"))
		return padLines(lines, width, height)
	}

	if height < 3 {
		height = 3
	}

	messages := m.sortedMessages()
	if len(messages) == 0 {
		lines = append(lines, deckMutedStyle.Render("no messages"))
		return padLines(lines, width, height)
	}

	// Table header
	headerRow := fmt.Sprintf("%-3s %-8s %-9s %9s %8s %8s",
		"#", "role", "time", "tokens", sortKeyCost, "delta")
	lines = append(lines, deckMutedStyle.Render(headerRow))

	// Calculate visible range (show current message and surrounding context)
	maxVisible := max(height-2, 5) // Reserve space for header
	start, end, _ := stableVisibleRange(len(messages), m.messageCursor, maxVisible, max(m.messageCursor-(maxVisible/2), 0))

	// Render message rows
	for i := start; i < end; i++ {
		msg := messages[i]

		cursor := "   "
		if i == m.messageCursor {
			cursor = " > "
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

		row := fmt.Sprintf("%s%-3s %-8s %-9s %9s %8s %8s%s",
			cursor,
			msgNum,
			roleStyle.Render(roleText),
			timeStr,
			tokensStr,
			costStr,
			deltaStr,
			toolIndicator,
		)

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

	// Return all sessions without filtering
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

func costPerMinute(totalCost float64, duration time.Duration) float64 {
	minutes := duration.Minutes()
	if minutes <= 0 {
		return 0
	}
	return totalCost / minutes
}

func tokensPerMinute(tokens int64, duration time.Duration) int64 {
	minutes := duration.Minutes()
	if minutes <= 0 {
		return 0
	}
	return int64(float64(tokens) / minutes)
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
