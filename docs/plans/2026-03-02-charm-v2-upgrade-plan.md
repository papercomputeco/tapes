# Charm v2 Upgrade Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Upgrade bubbletea, lipgloss, bubbles to v2 (charm.land modules) and split tui.go into per-concern files.

**Architecture:** All Charm v2 libraries share the `charm.land` vanity domain. They must be upgraded atomically since they cross-depend. The file split happens first (pure restructure, no API changes) so the v2 migration diffs are clean.

**Tech Stack:** Go 1.25+, charm.land/bubbletea/v2 (v2.0.1), charm.land/lipgloss/v2 (v2.0.0), charm.land/bubbles/v2 (v2.0.0), glamour v0.10.0 (stays on v1)

---

## Task 1: Split tui.go into 7 files (no API changes)

Split the monolithic `cmd/tapes/deck/tui.go` (5305 lines) into separate files while keeping all v1 APIs intact. This is a pure restructure — no code changes.

**Files:**
- Modify: `cmd/tapes/deck/tui.go` — keep core model, Init, Update, View, key handling, commands
- Create: `cmd/tapes/deck/tui_theme.go` — theme detection, palettes, style variables
- Create: `cmd/tapes/deck/tui_keys.go` — key map types and bindings
- Create: `cmd/tapes/deck/tui_overview.go` — overview view rendering
- Create: `cmd/tapes/deck/tui_session.go` — session detail view rendering
- Create: `cmd/tapes/deck/tui_analytics.go` — analytics view rendering
- Create: `cmd/tapes/deck/tui_util.go` — shared formatting/layout helpers

All files use `package deckcmder`.

**Step 1: Create tui_theme.go**

Extract these sections from tui.go (lines 26-368):
- `themeOverride` var (line 26)
- `init()` function (lines 28-40)
- `detectColorProfile()` function (lines 44-61)
- `isDarkTheme()` function (lines 65-74)
- `deckPalette` struct and types (lines 192-209)
- All color/style package-level vars (lines 211-252)
- `darkPalette` and `lightPalette` vars (lines 254-330)
- `applyPalette()` function (lines 332-368)
- Concrete hex constants `baseBgDark`, `baseBgLight` (lines 100-103)

Imports needed: `"os"`, `"github.com/charmbracelet/lipgloss"`, `"github.com/muesli/termenv"`

**Step 2: Create tui_keys.go**

Extract these sections from tui.go (lines 370-411):
- `sortOrder`, `sortDirOptions`, `messageSortOrder`, `statusFilters` vars (lines 370-375)
- `deckKeyMap` struct (lines 377-388)
- `ShortHelp()` and `FullHelp()` methods (lines 390-396)
- `defaultKeyMap()` function (lines 398-411)

Imports needed: `"github.com/charmbracelet/bubbles/key"`

Also include `sortDirDesc` constant — find where it's defined and include it.

**Step 3: Create tui_overview.go**

Extract from tui.go (lines 1053-1829):
- `countWrappedLines()` (line 1053)
- `overviewChrome()` (line 1080)
- `sessionListHeight()` (line 1131)
- `viewOverview()` (line 1142)
- `viewMetrics()` (line 1155)
- `viewCostByModel()` (line 1341)
- `renderCostByModelChart()` (line 1372)
- `renderStatusPieChart()` (line 1425)
- `getModelColor()` (line 1497)
- `countByStatusInStats()` (line 1524)
- `viewInsights()` (line 1535)
- `viewSessionList()` (line 1567)

Imports needed: `"fmt"`, `"sort"`, `"strings"`, `"github.com/charmbracelet/lipgloss"`, `"github.com/charmbracelet/x/ansi"`, `"github.com/papercomputeco/tapes/pkg/deck"`

**Step 4: Create tui_session.go**

Extract from tui.go (lines 1830-2081):
- `viewSession()` (line 1830)
- `renderSessionMetrics()` (line 1885)
- `viewFooter()` (line 2073)
- `viewSessionFooter()` (line 2078)

Also extract the conversation rendering functions (lines 4099-4780):
- `renderConversationTimeline()` (line 4099)
- `waveformPoint` struct (line 4129)
- `sampleWaveformPoints()` (line 4139)
- `buildWaveform()` (line 4223)
- `renderConversationTable()` (line 4357)
- `renderMessageDetailPane()` (line 4546)
- `formatTokensCompact()` (line 4774)

Imports needed: `"fmt"`, `"strings"`, `"time"`, `"github.com/charmbracelet/lipgloss"`, `"github.com/charmbracelet/x/ansi"`, `"github.com/papercomputeco/tapes/pkg/deck"`

**Step 5: Create tui_analytics.go**

Extract from tui.go (lines 2082-3408):
- `viewAnalytics()` (line 2082)
- `buildAnalyticsContent()` (line 2126)
- `renderAnalyticsTabBar()` (line 2224)
- `renderAnalyticsSectionHeader()` (line 2250)
- `renderAnalyticsSummaryCards()` (line 2257)
- `renderAnalyticsHeatmap()` (line 2319)
- `renderAnalyticsHeatmapKeys()` (line 2480)
- `renderAnalyticsSelectedDay()` (line 2516)
- `renderAnalyticsDayDetail()` (line 2532)
- `renderAnalyticsDaySessions()` (line 2579)
- `selectAnalyticsDay()` (line 2641)
- `analyticsSelectableDays()` (line 2653)
- `analyticsDayExists()` (line 2669)
- `trimDateLabel()` (line 2678)
- `analyticsDayRange` type (line 2685)
- `parseAnalyticsDay()` (line 2690)
- `renderAnalyticsTools()` (line 2700)
- `renderAnalyticsHistogram()` (line 2802)
- `renderAnalyticsModelTable()` (line 2867)
- `renderAnalyticsProviders()` (line 2968)
- All facet-related types, constants, and functions (lines 3048-3397)
- `viewAnalyticsFooter()` (line 3397)

Imports needed: `"fmt"`, `"sort"`, `"strings"`, `"time"`, `"strconv"`, `"github.com/charmbracelet/lipgloss"`, `"github.com/charmbracelet/x/ansi"`, `"github.com/papercomputeco/tapes/pkg/deck"`

**Step 6: Create tui_util.go**

Extract shared helpers — everything after the modal and command functions:
- `sortedModelCosts()` (line 3649)
- `clamp()` (line 3665)
- `periodToDuration()` (line 3675)
- `periodToLabel()` (line 3688)
- `changeArrow()` (line 3701)
- `abs()` (line 3711)
- `costGradientIndex()` (line 3718)
- `formatCostIndicator()` (line 3740)
- `formatCostWithScale()` (line 3756)
- `renderCostWeightedBarbell()` (line 3768)
- `getCircleSize()` (line 3821)
- `getConnector()` (line 3839)
- `colorizeModel()` (line 3853)
- `formatStatusWithCircle()` (line 3881)
- `formatCost()` (line 3903)
- `formatTokens()` (line 3907)
- `formatDuration()` (line 3917)
- `formatDurationMinutes()` (line 3934)
- `formatPercent()` (line 3945)
- `truncateText()` (line 3949)
- `renderBar()` (line 3959)
- `renderHeaderLine()` (line 3968)
- `renderRule()` (line 3982)
- `addHorizontalPadding()` (line 3990)
- `addPadding()` (line 3995)
- `applyBackground()` method (line 4020)
- `renderCassetteTape()` (line 4039)
- `fitCell()` (line 4048)
- `avgTokenCount()` (line 4058)
- `statusStyleFor()` (line 4065)
- `splitPercent()` (line 4078)
- `renderTokenSplitBar()` (line 4086)
- `deckOverviewStats` struct and `summarizeSessions()` (lines 4781-4830)
- `selectedSessions()` (line 4831)
- `headerSessionCount()` (line 4841)
- `sortedMessages()` and caching (lines 4848-4990)
- All remaining utility functions (lines 4990-5305)

Imports needed: `"fmt"`, `"sort"`, `"strings"`, `"time"`, `"strconv"`, `"github.com/charmbracelet/lipgloss"`, `"github.com/charmbracelet/x/ansi"`, `"github.com/papercomputeco/tapes/pkg/deck"`

**Step 7: Trim tui.go to core**

After extraction, tui.go should only contain:
- Package declaration and imports
- View/period/modal/analytics constants (lines 76-118)
- `deckModel` struct and cache types (lines 120-173)
- Message types (lines 413-448)
- `RunDeckTUI()` (line 450)
- `newDeckModel()` (line 495)
- `Init()` (line 560)
- `Update()` (line 571)
- `View()` (line 723)
- `handleKey()` (line 741)
- `handleModalKey()` (line 888)
- `moveCursor()` (line 954)
- `filteredSessions()` (line 1000)
- `enterSession()` (line 1018)
- `cyclePeriod()` (line 1028)
- `cycleMessageSort()` (line 1040)
- `viewModal()` (line 3409)
- `overlayModal()` (line 3519)
- Command functions: `loadOverviewCmd`, `computeMetricsCmd`, `loadAnalyticsCmd`, `loadFacetAnalyticsCmd`, `loadAnalyticsDayCmd`, `loadSessionCmd`, `replayTick`, `refreshTick`, `refreshCmd` (lines 3574-3648)

**Step 8: Verify compilation and tests**

Run: `go build ./cmd/tapes/deck/...`
Expected: Clean compilation

Run: `make unit-test`
Expected: All tests pass

**Step 9: Run formatter**

Run: `make format`
Expected: Imports organized, code formatted

**Step 10: Commit**

```bash
git add cmd/tapes/deck/tui.go cmd/tapes/deck/tui_theme.go cmd/tapes/deck/tui_keys.go cmd/tapes/deck/tui_overview.go cmd/tapes/deck/tui_session.go cmd/tapes/deck/tui_analytics.go cmd/tapes/deck/tui_util.go
git commit -m "refactor: split tui.go into per-concern files

No functional changes. Prepares for Charm v2 migration by splitting
the monolithic 5300-line tui.go into 7 focused files.

Ref #136"
```

---

## Task 2: Update go.mod dependencies to Charm v2

**Files:**
- Modify: `go.mod`
- Regenerate: `go.sum`

**Step 1: Add new v2 dependencies**

```bash
go get charm.land/bubbletea/v2@latest
go get charm.land/lipgloss/v2@latest
go get charm.land/bubbles/v2@latest
```

**Step 2: Remove old v1 dependencies**

After all imports are updated (Tasks 3-5), run:

```bash
go mod tidy
```

**Step 3: Verify go.mod**

Run: `cat go.mod | grep charm`
Expected: New `charm.land/*` entries, old `github.com/charmbracelet/*` entries removed (except glamour which stays).

Note: This task will cause compilation failures until imports are updated in Tasks 3-5. That's expected.

---

## Task 3: Migrate cliui.go to lipgloss v2

**Files:**
- Modify: `pkg/cliui/cliui.go`

**Step 1: Update imports**

Replace:
```go
"github.com/charmbracelet/lipgloss"
```
With:
```go
"charm.land/lipgloss/v2"
```

Glamour stays as `github.com/charmbracelet/glamour` (no v2).

**Step 2: Verify no API changes needed**

The cliui.go usage is straightforward:
- `lipgloss.NewStyle()` — unchanged in v2
- `lipgloss.Color("82")` — unchanged in v2 (string ANSI colors still work)
- `.Foreground()`, `.Bold()`, `.Render()` — unchanged

The only potential issue: `lipgloss.NewStyle()` no longer requires a renderer. In v2, styles are plain value types. This is actually simpler — no changes needed.

**Step 3: Verify compilation**

Run: `go build ./pkg/cliui/...`
Expected: Clean compilation

**Step 4: Commit**

```bash
git add pkg/cliui/cliui.go
git commit -m "chore: migrate cliui to lipgloss v2

Update import path from github.com/charmbracelet/lipgloss to
charm.land/lipgloss/v2. No API changes needed — lipgloss v2
preserves the NewStyle/Color/Render API.

Ref #136"
```

---

## Task 4: Migrate chat.go to lipgloss v2

**Files:**
- Modify: `cmd/tapes/chat/chat.go`

**Step 1: Update imports**

Replace:
```go
"github.com/charmbracelet/lipgloss"
```
With:
```go
"charm.land/lipgloss/v2"
```

**Step 2: Verify no API changes needed**

chat.go only uses:
```go
lipgloss.NewStyle().Foreground(lipgloss.Color("82")).Bold(true).Render("you> ")
lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render("assistant> ")
```
These are unchanged in v2.

**Step 3: Verify compilation**

Run: `go build ./cmd/tapes/chat/...`
Expected: Clean compilation

**Step 4: Commit**

```bash
git add cmd/tapes/chat/chat.go
git commit -m "chore: migrate chat command to lipgloss v2

Ref #136"
```

---

## Task 5: Migrate TUI files to Charm v2

This is the largest task. Update all 7 TUI files from Task 1 to use v2 APIs.

**Files:**
- Modify: `cmd/tapes/deck/tui.go`
- Modify: `cmd/tapes/deck/tui_theme.go`
- Modify: `cmd/tapes/deck/tui_keys.go`
- Modify: `cmd/tapes/deck/tui_overview.go`
- Modify: `cmd/tapes/deck/tui_session.go`
- Modify: `cmd/tapes/deck/tui_analytics.go`
- Modify: `cmd/tapes/deck/tui_util.go`
- Modify: `cmd/tapes/deck/tui_test.go`

### Step 1: Update all imports across all TUI files

In every file, replace:
```go
bubbletea "github.com/charmbracelet/bubbletea"      → tea "charm.land/bubbletea/v2"
"github.com/charmbracelet/lipgloss"                  → "charm.land/lipgloss/v2"
"github.com/charmbracelet/bubbles/help"              → "charm.land/bubbles/v2/help"
"github.com/charmbracelet/bubbles/key"               → "charm.land/bubbles/v2/key"
"github.com/charmbracelet/bubbles/spinner"           → "charm.land/bubbles/v2/spinner"
"github.com/charmbracelet/bubbles/textinput"         → "charm.land/bubbles/v2/textinput"
```

Also rename the local alias from `bubbletea` to `tea` throughout (standard convention in v2).

The `"github.com/charmbracelet/x/ansi"` import stays — it's not part of the v2 vanity migration.

### Step 2: Migrate tui_theme.go — lipgloss v2 types

Replace `lipgloss.TerminalColor` with `color.Color` (from `image/color`):

```go
import "image/color"

type deckPalette struct {
    foreground         color.Color
    red                color.Color
    green              color.Color
    // ... all fields change from lipgloss.TerminalColor to color.Color
}

var (
    colorForeground  color.Color
    colorRed         color.Color
    // ... all package vars change type
)
```

Replace `lipgloss.CompleteColor{...}` with `lipgloss.CompleteColor(...)` or use `compat.CompleteColor`:

```go
// v1:
dimmed: lipgloss.CompleteColor{TrueColor: "#2A2A2B", ANSI256: "236", ANSI: "0"},

// v2 option — use the compat package:
import "charm.land/lipgloss/v2/compat"
dimmed: compat.CompleteColor{TrueColor: lipgloss.Color("#2A2A2B"), ANSI256: lipgloss.ANSIColor(236), ANSI: lipgloss.ANSIColor(0)},
```

Remove the renderer setup from `init()` — lipgloss v2 has no renderers:

```go
// v1 init():
renderer := lipgloss.NewRenderer(os.Stdout, termenv.WithProfile(profile))
renderer.SetColorProfile(profile)
lipgloss.SetDefaultRenderer(renderer)

// v2 init(): remove all three lines above. Just keep palette selection:
func init() {
    if isDarkTheme() {
        applyPalette(darkPalette)
    } else {
        applyPalette(lightPalette)
    }
}
```

Also remove the `termenv` import from `tui_theme.go` if it's only used for the renderer (check if `detectColorProfile()` and `isDarkTheme()` still need it — yes they do for `termenv.HasDarkBackground()` and `termenv.ColorProfile()`).

### Step 3: Migrate tui.go — bubbletea v2 core changes

**View() return type:**

```go
// v1:
func (m deckModel) View() string {
    // ...
    return m.applyBackground(addPadding(base))
}

// v2:
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
        return v
    case viewAnalytics:
        base = m.viewAnalytics()
    default:
        base = m.viewOverview()
    }
    v := tea.NewView(m.applyBackground(addPadding(base)))
    v.AltScreen = true
    return v
}
```

**Init() — remove spinner.Tick:**

```go
// v1:
func (m deckModel) Init() bubbletea.Cmd {
    cmds := []bubbletea.Cmd{
        m.spinner.Tick,
        loadOverviewCmd(m.query, m.filters),
    }

// v2: spinner auto-ticks, remove m.spinner.Tick from Init:
func (m deckModel) Init() tea.Cmd {
    cmds := []tea.Cmd{
        loadOverviewCmd(m.query, m.filters),
    }
```

**Update() — rename types:**

Replace throughout:
- `bubbletea.Msg` → `tea.Msg`
- `bubbletea.Cmd` → `tea.Cmd`
- `bubbletea.Model` → `tea.Model`
- `bubbletea.WindowSizeMsg` → `tea.WindowSizeMsg`
- `bubbletea.KeyMsg` → `tea.KeyPressMsg`
- `bubbletea.Quit` → `tea.Quit`
- `bubbletea.Batch(...)` → `tea.Batch(...)`
- `spinner.TickMsg` — check if this is still the same type name in v2

**handleKey() — KeyMsg migration:**

```go
// v1:
func (m deckModel) handleKey(msg bubbletea.KeyMsg) (bubbletea.Model, bubbletea.Cmd) {
    if m.searchActive {
        switch msg.Type {
        case bubbletea.KeyEscape:
        case bubbletea.KeyEnter:
        }
    }
    switch msg.String() {

// v2:
func (m deckModel) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
    if m.searchActive {
        switch msg.String() {
        case "escape":
        case "enter":
        }
    }
    switch msg.String() {
```

Note: In v2, `msg.String()` returns `"escape"` and `"enter"` as strings. The `Type` field with `KeyEscape`/`KeyEnter` constants is gone.

**RunDeckTUI() — remove WithAltScreen:**

```go
// v1:
program := bubbletea.NewProgram(model,
    bubbletea.WithContext(ctx),
    bubbletea.WithAltScreen(),
)

// v2: AltScreen is set declaratively in View()
program := tea.NewProgram(model,
    tea.WithContext(ctx),
)
```

**Command functions — rename types:**

All `bubbletea.Cmd` return types become `tea.Cmd`.

### Step 4: Migrate tui.go — bubbles v2 changes

**spinner in newDeckModel():**

```go
// v1:
s := spinner.New()
s.Spinner = spinner.Dot
s.Style = lipgloss.NewStyle().Foreground(colorGreen)

// v2: check if spinner.New() accepts options or still uses field assignment.
// Based on upgrade guide, constructor uses functional options:
s := spinner.New(
    spinner.WithSpinner(spinner.Dot),
    spinner.WithStyle(lipgloss.NewStyle().Foreground(colorGreen)),
)
```

**textinput in newDeckModel():**

```go
// v1:
ti := textinput.New()
ti.Placeholder = "filter by label..."
ti.CharLimit = 64
ti.Prompt = "/ "
ti.Width = 30
ti.PlaceholderStyle = lipgloss.NewStyle().Foreground(colorBrightBlack)
ti.TextStyle = lipgloss.NewStyle().Foreground(colorForeground)
ti.PromptStyle = lipgloss.NewStyle().Foreground(colorRed)

// v2: fields become setters
ti := textinput.New()
ti.SetPlaceholder("filter by label...")
ti.SetCharLimit(64)
ti.SetPrompt("/ ")
ti.SetWidth(30)
// Styles may need to use a Styles struct — check v2 docs
```

**help in newDeckModel():**

```go
// v1:
help: help.New(),

// v2: same constructor, but may need isDark for styles
h := help.New()
h.Styles = help.DefaultStyles(isDarkTheme())
```

**spinner.TickMsg in Update():**

Check if `spinner.TickMsg` still exists in v2. If spinners auto-tick, this case may need removal or the type name may change.

**textinput.Cursor.BlinkCmd() in handleKey():**

```go
// v1:
return m, m.searchInput.Cursor.BlinkCmd()

// v2: check if cursor API changed — it may be m.searchInput.CursorBlink() or similar
```

### Step 5: Migrate tui_overview.go, tui_session.go, tui_analytics.go, tui_util.go

These files primarily use lipgloss for rendering. Changes needed:
- Update import paths
- Replace `lipgloss.TerminalColor` parameter types with `color.Color`
- All `lipgloss.Color()`, `lipgloss.NewStyle()`, `.Render()` calls are unchanged

Specific changes to look for:
- Any function that accepts `lipgloss.TerminalColor` as a parameter (e.g., `renderFacetDistribution` takes `color lipgloss.TerminalColor`)
- Any type assertions or interfaces involving `lipgloss.TerminalColor`

### Step 6: Migrate tui_test.go

Update any bubbletea or lipgloss references in tests. The current test file doesn't directly import Charm packages, so this should only need recompilation verification.

### Step 7: Verify compilation

Run: `go build ./...`
Expected: Clean compilation

### Step 8: Run tests

Run: `make unit-test`
Expected: All tests pass

### Step 9: Run formatter

Run: `make format`
Expected: Clean formatting, organized imports

### Step 10: Commit

```bash
git add cmd/tapes/deck/ pkg/cliui/ go.mod go.sum
git commit -m "feat: upgrade to Charm v2 suite (bubbletea, lipgloss, bubbles)

Migrate from github.com/charmbracelet/* v1 to charm.land/* v2:
- bubbletea v2.0.1: declarative View(), KeyPressMsg, no WithAltScreen
- lipgloss v2.0.0: color.Color types, no renderers
- bubbles v2.0.0: getter/setter APIs, functional options
- glamour stays on v0.10.0 (no v2 release)

Closes #136"
```

---

## Task 6: Run go mod tidy and final verification

**Step 1: Tidy modules**

Run: `go mod tidy`
Expected: Removes old charmbracelet v1 dependencies (except glamour)

**Step 2: Full build**

Run: `go build ./...`
Expected: Clean

**Step 3: Full test suite**

Run: `make unit-test`
Expected: All pass

**Step 4: Lint check**

Run: `make check`
Expected: Clean

**Step 5: Final commit if needed**

```bash
git add go.mod go.sum
git commit -m "chore: tidy go.mod after Charm v2 migration

Ref #136"
```
