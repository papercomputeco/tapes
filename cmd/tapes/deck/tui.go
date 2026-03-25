package deckcmder

import (
	"context"
	"strings"
	"time"

	"charm.land/bubbles/v2/help"
	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/papercomputeco/tapes/pkg/deck"
)

type deckView int

const (
	viewOverview deckView = iota
	viewSession
	viewModal
)

type timePeriod int

const (
	period24h timePeriod = iota
	period30d
)

const (
	horizontalPadding = 2
	verticalPadding   = 1
)

const (
	sortKeyCost              = "cost"
	roleUser                 = "user"
	roleAssistant            = "assistant"
	circleLarge              = "⬤"
	labelTokens              = "tokens"
	keyEnter                 = "enter"
	maxCostByModelEntries    = 5
	waveformWindowMultiplier = 10
)

const (
	sessionListChromeLines   = 3
	sessionListPositionLines = 2
)

type deckModel struct {
	query            deck.Querier
	filters          deck.Filters
	overview         *deck.Overview
	detail           *deck.SessionDetail
	view             deckView
	cursor           int
	scrollOffset     int
	messageCursor    int
	width            int
	height           int
	sortIndex        int
	statusIndex      int
	messageSort      int
	timePeriod       timePeriod
	modalCursor      int
	modalTab         modalTab
	replayActive     bool
	replayOnLoad     bool
	metricsReady     bool
	overviewStats    *deckOverviewStats
	refreshEvery     time.Duration
	spinner          spinner.Model
	keys             deckKeyMap
	help             help.Model
	searchInput      textinput.Model
	searchActive     bool
	sortedCache      *sortedMessagesCache
	sortedGroupCache *sortedGroupCache
}

type sortedMessagesCache struct {
	key      string
	id       string
	count    int
	messages []deck.SessionMessage
}

type sortedGroupCache struct {
	key    string
	id     string
	count  int
	groups []deck.SessionMessageGroup
}

type modalTab int

const (
	modalSort modalTab = iota
	modalFilter
)

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

type metricsReadyMsg struct {
	stats deckOverviewStats
}

type refreshTickMsg time.Time

// RunDeckTUI starts the deck TUI with the provided query implementation.
// This function is exported to allow sandbox and testing environments to inject mock data.
func RunDeckTUI(ctx context.Context, query deck.Querier, filters deck.Filters, refreshEvery time.Duration) error {
	model := newDeckModel(query, filters, nil, refreshEvery)

	if filters.Session != "" {
		detail, err := query.SessionDetail(ctx, filters.Session)
		if err != nil {
			return err
		}
		model.view = viewSession
		model.detail = detail
	}

	program := tea.NewProgram(model,
		tea.WithContext(ctx),
	)
	_, err := program.Run()
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
		switch {
		case filters.Since >= 30*24*time.Hour:
			period = period30d
		default:
			period = period24h
		}
	}

	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(colorGreen)

	ti := textinput.New()
	ti.Placeholder = "filter by label..."
	ti.CharLimit = 64
	ti.Prompt = "/ "
	ti.SetWidth(30)
	styles := ti.Styles()
	styles.Focused.Placeholder = lipgloss.NewStyle().Foreground(colorBrightBlack)
	styles.Focused.Text = lipgloss.NewStyle().Foreground(colorForeground)
	styles.Focused.Prompt = lipgloss.NewStyle().Foreground(colorRed)
	styles.Blurred.Placeholder = lipgloss.NewStyle().Foreground(colorBrightBlack)
	styles.Blurred.Text = lipgloss.NewStyle().Foreground(colorForeground)
	styles.Blurred.Prompt = lipgloss.NewStyle().Foreground(colorRed)
	ti.SetStyles(styles)

	h := help.New()
	h.Styles = help.DefaultStyles(isDarkTheme())

	return deckModel{
		query:            query,
		filters:          filters,
		overview:         overview,
		view:             viewOverview,
		sortIndex:        sortIndex,
		statusIndex:      statusIndex,
		messageSort:      0,
		timePeriod:       period,
		modalTab:         modalSort,
		refreshEvery:     refreshEvery,
		spinner:          s,
		keys:             defaultKeyMap(),
		help:             h,
		searchInput:      ti,
		sortedCache:      &sortedMessagesCache{},
		sortedGroupCache: &sortedGroupCache{},
	}
}

func (m deckModel) Init() tea.Cmd {
	cmds := []tea.Cmd{
		m.spinnerTickCmd(),
		loadOverviewCmd(m.query, m.filters),
	}
	if m.refreshEvery > 0 {
		cmds = append(cmds, refreshTick(m.refreshEvery))
	}
	return tea.Batch(cmds...)
}

func (m deckModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
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
		m.metricsReady = false
		m.overviewStats = nil

		metricsCmd := computeMetricsCmd(m.overview.Sessions)

		// Try to find the previously selected session in the filtered list
		// so the cursor stays valid when search is active.
		filtered := m.filteredSessions()
		if selectedSessionID != "" {
			for i, session := range filtered {
				if session.ID == selectedSessionID {
					m.cursor = i
					// Clamp scroll offset to keep cursor visible
					visibleRows := sessionListVisibleRows(len(filtered), m.sessionListHeight())
					_, _, m.scrollOffset = stableVisibleRange(
						len(filtered), m.cursor, visibleRows, m.scrollOffset,
					)
					return m, metricsCmd
				}
			}
		}

		// If session not found or no previous selection, clamp cursor and reset scroll.
		if len(filtered) == 0 {
			m.cursor = 0
		} else if m.cursor >= len(filtered) {
			m.cursor = clamp(m.cursor, len(filtered)-1)
		}
		m.scrollOffset = 0
		return m, metricsCmd
	case metricsReadyMsg:
		m.metricsReady = true
		m.overviewStats = &msg.stats
		return m, nil
	case sessionLoadedMsg:
		if msg.err != nil {
			return m, nil
		}
		m.detail = msg.detail
		m.resetSortedCache()
		m.view = viewSession
		if msg.keepUI {
			maxCursor := m.currentConversationLength() - 1
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
		if m.messageCursor >= m.currentConversationLength()-1 {
			m.replayActive = false
			return m, nil
		}
		m.messageCursor++
		return m, replayTick()
	case spinner.TickMsg:
		overviewLoading := m.overview == nil || !m.metricsReady
		if m.view == viewOverview && overviewLoading {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil
	case refreshTickMsg:
		if m.refreshEvery <= 0 {
			return m, nil
		}
		refreshCmd := m.refreshCmd()
		if refreshCmd == nil {
			return m, refreshTick(m.refreshEvery)
		}
		return m, tea.Batch(refreshTick(m.refreshEvery), refreshCmd)
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}

	return m, nil
}

func (m deckModel) View() tea.View {
	var base string
	switch m.view {
	case viewOverview:
		base = m.viewOverview()
	case viewSession:
		base = m.viewSession()
	case viewModal:
		base = m.viewOverview()
		v := tea.NewView(m.applyBackground(addPadding(m.overlayModal(base, m.viewModal()))))
		v.AltScreen = true
		v.BackgroundColor = colorBaseBg
		return v
	default:
		base = m.viewOverview()
	}
	v := tea.NewView(m.applyBackground(addPadding(base)))
	v.AltScreen = true
	v.BackgroundColor = colorBaseBg
	return v
}

func (m deckModel) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	// Handle search input mode
	if m.searchActive {
		switch msg.String() {
		case "escape":
			m.searchActive = false
			m.searchInput.SetValue("")
			m.searchInput.Blur()
			m.cursor = 0
			m.scrollOffset = 0
			return m, nil
		case keyEnter:
			m.searchActive = false
			m.searchInput.Blur()
			m.cursor = 0
			m.scrollOffset = 0
			return m, nil
		}
		var cmd tea.Cmd
		m.searchInput, cmd = m.searchInput.Update(msg)
		// Reset cursor when search term changes
		m.cursor = 0
		m.scrollOffset = 0
		return m, cmd
	}

	// Handle modal views
	if m.view == viewModal {
		return m.handleModalKey(msg)
	}

	switch msg.String() {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "j", "down":
		return m.moveCursor(1)
	case "k", "up":
		return m.moveCursor(-1)
	case "l", keyEnter:
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
	case "/":
		if m.view == viewOverview {
			m.searchActive = true
			return m, m.searchInput.Focus()
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
			if len(m.filteredSessions()) == 0 {
				return m, nil
			}
			m.replayOnLoad = true
			return m.enterSession()
		}
	}

	return m, nil
}

func (m deckModel) handleModalKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "q":
		return m, tea.Quit
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
	case keyEnter, "l":
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
			if m.overview != nil {
				deck.SortSessions(m.overview.Sessions, m.filters.Sort, m.filters.SortDir)
				m.cursor = 0
			}
			return m, nil
		case modalFilter:
			m.statusIndex = m.modalCursor
			m.filters.Status = statusFilters[m.statusIndex]
			m.view = viewOverview
			return m, loadOverviewCmd(m.query, m.filters)
		}
	}
	return m, nil
}

func (m deckModel) moveCursor(delta int) (tea.Model, tea.Cmd) {
	if m.view == viewOverview {
		sessions := m.filteredSessions()
		if len(sessions) == 0 {
			return m, nil
		}
		m.cursor = clamp(m.cursor+delta, len(sessions)-1)
		// Update scroll offset to keep cursor visible without jumping
		visibleRows := sessionListVisibleRows(len(sessions), m.sessionListHeight())
		_, _, m.scrollOffset = stableVisibleRange(
			len(sessions), m.cursor, visibleRows, m.scrollOffset,
		)
		return m, nil
	}

	if m.detail == nil || len(m.detail.Messages) == 0 {
		return m, nil
	}
	length := m.currentConversationLength()
	if length == 0 {
		m.messageCursor = 0
		return m, nil
	}
	m.messageCursor = clamp(m.messageCursor+delta, length-1)
	return m, nil
}

func (m deckModel) filteredSessions() []deck.SessionSummary {
	if m.overview == nil {
		return nil
	}
	term := strings.TrimSpace(m.searchInput.Value())
	if term == "" {
		return m.overview.Sessions
	}
	lower := strings.ToLower(term)
	var result []deck.SessionSummary
	for _, s := range m.overview.Sessions {
		if strings.Contains(strings.ToLower(s.Label), lower) {
			result = append(result, s)
		}
	}
	return result
}

func (m deckModel) enterSession() (tea.Model, tea.Cmd) {
	sessions := m.filteredSessions()
	if len(sessions) == 0 {
		return m, nil
	}

	session := sessions[m.cursor]
	return m, loadSessionCmd(m.query, session.ID, false)
}

func (m deckModel) cyclePeriod() (tea.Model, tea.Cmd) {
	m.timePeriod = (m.timePeriod + 1) % 2
	m.filters.Since = periodToDuration(m.timePeriod)
	return m, loadOverviewCmd(m.query, m.filters)
}

func (m deckModel) cycleMessageSort() (tea.Model, tea.Cmd) {
	m.messageSort = (m.messageSort + 1) % len(messageSortOrder)
	m.resetSortedCache()
	m.resetSortedGroupCache()
	length := m.currentConversationLength()
	if length == 0 {
		m.messageCursor = 0
		return m, nil
	}
	m.messageCursor = clamp(m.messageCursor, length-1)
	return m, nil
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
			"date":      "Date",
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

func loadOverviewCmd(query deck.Querier, filters deck.Filters) tea.Cmd {
	return func() tea.Msg {
		overview, err := query.Overview(context.Background(), filters)
		return overviewLoadedMsg{overview: overview, err: err}
	}
}

func computeMetricsCmd(sessions []deck.SessionSummary) tea.Cmd {
	return func() tea.Msg {
		stats := summarizeSessions(sessions)
		return metricsReadyMsg{stats: stats}
	}
}

func loadSessionCmd(query deck.Querier, sessionID string, keepUI bool) tea.Cmd {
	return func() tea.Msg {
		detail, err := query.SessionDetail(context.Background(), sessionID)
		return sessionLoadedMsg{detail: detail, err: err, keepUI: keepUI}
	}
}

func replayTick() tea.Cmd {
	return tea.Tick(300*time.Millisecond, func(t time.Time) tea.Msg {
		return replayTickMsg(t)
	})
}

func refreshTick(interval time.Duration) tea.Cmd {
	return tea.Tick(interval, func(t time.Time) tea.Msg {
		return refreshTickMsg(t)
	})
}

func (m deckModel) refreshCmd() tea.Cmd {
	if m.view == viewOverview {
		return loadOverviewCmd(m.query, m.filters)
	}
	if m.view == viewSession && m.detail != nil {
		return loadSessionCmd(m.query, m.detail.Summary.ID, true)
	}
	return nil
}

// spinnerTickCmd wraps the spinner's Tick() (which returns tea.Msg in v2) as a
// tea.Cmd so it can be batched with other commands.
func (m deckModel) spinnerTickCmd() tea.Cmd {
	return func() tea.Msg { return m.spinner.Tick() }
}
