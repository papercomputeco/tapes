# Charm v2 Upgrade Design

**Issue:** #136 - chore: Upgrade to new Charm v2 suite
**Date:** 2026-03-02
**Branch:** chore/upgrade-charm-v2-suite

## Goal

Upgrade all Charm libraries (bubbletea, lipgloss, bubbles, glamour) from v1 to v2.
Split the monolithic `tui.go` (~5300 lines) into per-concern files while upgrading.

## Scope

### Files Affected

- `go.mod` / `go.sum` — dependency updates
- `cmd/tapes/deck/tui.go` — split into 7 files + v2 migration
- `cmd/tapes/deck/tui_test.go` — import updates
- `pkg/cliui/cliui.go` — lipgloss v2 + glamour v2
- `cmd/tapes/chat/chat.go` — lipgloss v2

### Import Path Changes

| v1 | v2 |
|----|-----|
| `github.com/charmbracelet/bubbletea` | `charm.land/bubbletea/v2` |
| `github.com/charmbracelet/lipgloss` | `charm.land/lipgloss/v2` |
| `github.com/charmbracelet/bubbles/*` | `charm.land/bubbles/v2/*` |
| `github.com/charmbracelet/glamour` | TBD (check if v2 exists) |

### Bubbletea v2 Changes

- `View() string` becomes `View() tea.View` using `tea.NewView()`
- `tea.WithAltScreen()` removed; set `v.AltScreen = true` in View
- `tea.KeyMsg` becomes `tea.KeyPressMsg`
  - `Type` -> `Code`, `Runes` -> `Text`, `Alt` -> `Mod`
- `tea.WindowSizeMsg` field names may change

### Lipgloss v2 Changes

- `lipgloss.TerminalColor` becomes `color.Color` from `image/color`
- `lipgloss.CompleteColor{}` replaced with explicit color handling
- Dark/light detection is manual via `tea.RequestBackgroundColor`
- Standalone usage needs `compat` package or `lipgloss.Println()`

### Bubbles v2 Changes

- Direct field access becomes getter/setter methods
- Constructors use functional options pattern
- `DefaultKeyMap` variable becomes `DefaultKeyMap()` function
- `help.DefaultStyles()` takes `isDark bool`
- `spinner.Tick` removed; spinner auto-starts

## File Split

Current: `cmd/tapes/deck/tui.go` (~5300 lines)

| File | Contents | ~Lines |
|------|----------|--------|
| `tui.go` | Model, Init, Update, View dispatch, key handling, commands, RunDeckTUI | ~800 |
| `tui_theme.go` | Color palettes, theme detection, style vars, `applyPalette()` | ~300 |
| `tui_keys.go` | `deckKeyMap`, key bindings, ShortHelp/FullHelp | ~50 |
| `tui_overview.go` | `viewOverview()`, metrics, session list, cost charts, status pie, insights | ~700 |
| `tui_session.go` | `viewSession()`, conversation timeline, waveform, message detail, conversation table | ~1200 |
| `tui_analytics.go` | `viewAnalytics()`, heatmap, histogram, model table, providers, facets, day detail | ~1400 |
| `tui_util.go` | Shared helpers: formatting, padding, layout, text wrapping, sorting | ~600 |

All files remain in package `deck`. No new packages or interface changes.

## Testing

- Existing `tui_test.go` updated with v2 imports
- `make test` validates compilation and tests
- `make format` for import organization
- Manual TUI verification with `tapes deck`
