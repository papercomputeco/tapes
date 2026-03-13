package deckcmder

import (
	"image/color"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/compat"
	"github.com/muesli/termenv"
)

// themeOverride is set by the CLI --theme flag before the TUI starts.
// Valid values: "", "dark", "light".
var themeOverride string

func init() {
	if isDarkTheme() {
		applyPalette(darkPalette)
	} else {
		applyPalette(lightPalette)
	}
}

// isDarkTheme returns true when the dark palette should be used.
// The --theme flag takes priority over terminal background detection.
func isDarkTheme() bool {
	switch themeOverride {
	case "dark":
		return true
	case "light":
		return false
	default:
		return termenv.HasDarkBackground()
	}
}

type deckPalette struct {
	foreground         color.Color
	red                color.Color
	green              color.Color
	yellow             color.Color
	blue               color.Color
	magenta            color.Color
	brightBlack        color.Color
	dimmed             color.Color
	highlightBg        color.Color
	panelBg            color.Color
	label              color.Color
	baseBg             color.Color
	costOrangeGradient []string
	claudeColors       map[string]string
	openaiColors       map[string]string
	googleColors       map[string]string
}

var (
	colorForeground  color.Color
	colorRed         color.Color
	colorGreen       color.Color
	colorYellow      color.Color
	colorBlue        color.Color
	colorMagenta     color.Color
	colorBrightBlack color.Color
	colorDimmed      color.Color
	colorHighlightBg color.Color
	colorPanelBg     color.Color
	colorLabel       color.Color
	colorBaseBg      color.Color

	costOrangeGradient []string

	deckTitleStyle       lipgloss.Style
	deckMutedStyle       lipgloss.Style
	deckAccentStyle      lipgloss.Style
	deckDimStyle         lipgloss.Style
	deckSectionStyle     lipgloss.Style
	deckDividerStyle     lipgloss.Style
	deckHighlightStyle   lipgloss.Style
	deckStatusOKStyle    lipgloss.Style
	deckStatusFailStyle  lipgloss.Style
	deckStatusWarnStyle  lipgloss.Style
	deckRoleUserStyle    lipgloss.Style
	deckRoleAsstStyle    lipgloss.Style
	deckModalBgStyle     lipgloss.Style
	deckTabBoxStyle      lipgloss.Style
	deckTabActiveStyle   lipgloss.Style
	deckTabInactiveStyle lipgloss.Style
	deckBackgroundStyle  lipgloss.Style
)

// Model color schemes by provider — dark and light variants per tier.
var (
	claudeColors map[string]string
	openaiColors map[string]string
	googleColors map[string]string
)

var darkPalette = deckPalette{
	foreground:  lipgloss.Color("#E6E4D9"),
	red:         lipgloss.Color("#FF6B4A"),
	green:       lipgloss.Color("#4DA667"),
	yellow:      lipgloss.Color("#F2B84B"),
	blue:        lipgloss.Color("#4EB1E9"),
	magenta:     lipgloss.Color("#B656B1"),
	brightBlack: lipgloss.Color("#4A4A4A"),
	dimmed:      compat.CompleteColor{TrueColor: lipgloss.Color("#2A2A2B"), ANSI256: lipgloss.ANSIColor(236), ANSI: lipgloss.ANSIColor(0)},
	highlightBg: compat.CompleteColor{TrueColor: lipgloss.Color("#252526"), ANSI256: lipgloss.ANSIColor(235), ANSI: lipgloss.ANSIColor(0)},
	panelBg:     compat.CompleteColor{TrueColor: lipgloss.Color("#212122"), ANSI256: lipgloss.ANSIColor(235), ANSI: lipgloss.ANSIColor(0)},
	label:       lipgloss.Color("#8A8079"),
	baseBg:      compat.CompleteColor{TrueColor: lipgloss.Color("#000000"), ANSI256: lipgloss.ANSIColor(16), ANSI: lipgloss.ANSIColor(0)},
	costOrangeGradient: []string{
		"#B6512B",
		"#D96840",
		"#FF7A45",
		"#FF8F4D",
		"#FFB25A",
	},
	claudeColors: map[string]string{
		"opus":   "#D97BC1",
		"sonnet": "#B656B1",
		"haiku":  "#8E3F8A",
	},
	openaiColors: map[string]string{
		"gpt-4o":      "#7DD9FF",
		"gpt-4":       "#4EB1E9",
		"gpt-4o-mini": "#3889B8",
		"gpt-3.5":     "#2A6588",
	},
	googleColors: map[string]string{
		"gemini-2.0":     "#7DD9FF",
		"gemini-1.5-pro": "#4EB1E9",
		"gemini-1.5":     "#3889B8",
		"gemma":          "#2A6588",
	},
}

var lightPalette = deckPalette{
	foreground:  lipgloss.Color("#1A1612"),
	red:         lipgloss.Color("#A8371F"),
	green:       lipgloss.Color("#16653A"),
	yellow:      lipgloss.Color("#996B0F"),
	blue:        lipgloss.Color("#155D91"),
	magenta:     lipgloss.Color("#7A2E7A"),
	brightBlack: lipgloss.Color("#4A4239"),
	dimmed:      compat.CompleteColor{TrueColor: lipgloss.Color("#8A7E72"), ANSI256: lipgloss.ANSIColor(245), ANSI: lipgloss.ANSIColor(8)},
	highlightBg: compat.CompleteColor{TrueColor: lipgloss.Color("#EFE6D8"), ANSI256: lipgloss.ANSIColor(255), ANSI: lipgloss.ANSIColor(7)},
	panelBg:     compat.CompleteColor{TrueColor: lipgloss.Color("#F5EFE6"), ANSI256: lipgloss.ANSIColor(255), ANSI: lipgloss.ANSIColor(7)},
	label:       lipgloss.Color("#3D352C"),
	baseBg:      compat.CompleteColor{TrueColor: lipgloss.Color("#E2E0DB"), ANSI256: lipgloss.ANSIColor(254), ANSI: lipgloss.ANSIColor(7)},
	costOrangeGradient: []string{
		"#9C3C1E",
		"#B64A28",
		"#CF5A33",
		"#E06A3F",
		"#F08B57",
	},
	claudeColors: map[string]string{
		"opus":   "#B24B9C",
		"sonnet": "#8F3B85",
		"haiku":  "#6E2D66",
	},
	openaiColors: map[string]string{
		"gpt-4o":      "#2F89C6",
		"gpt-4":       "#1B6EA8",
		"gpt-4o-mini": "#185A87",
		"gpt-3.5":     "#134466",
	},
	googleColors: map[string]string{
		"gemini-2.0":     "#2F89C6",
		"gemini-1.5-pro": "#1B6EA8",
		"gemini-1.5":     "#185A87",
		"gemma":          "#134466",
	},
}

func applyPalette(p deckPalette) {
	colorForeground = p.foreground
	colorRed = p.red
	colorGreen = p.green
	colorYellow = p.yellow
	colorBlue = p.blue
	colorMagenta = p.magenta
	colorBrightBlack = p.brightBlack
	colorDimmed = p.dimmed
	colorHighlightBg = p.highlightBg
	colorPanelBg = p.panelBg
	colorLabel = p.label
	colorBaseBg = p.baseBg
	costOrangeGradient = p.costOrangeGradient
	claudeColors = p.claudeColors
	openaiColors = p.openaiColors
	googleColors = p.googleColors

	deckTitleStyle = lipgloss.NewStyle().Bold(true).Foreground(colorYellow)
	deckMutedStyle = lipgloss.NewStyle().Foreground(colorBrightBlack)
	deckAccentStyle = lipgloss.NewStyle().Foreground(colorRed)
	deckDimStyle = lipgloss.NewStyle().Foreground(colorDimmed)
	deckSectionStyle = lipgloss.NewStyle().Bold(true).Foreground(colorForeground)
	deckDividerStyle = lipgloss.NewStyle().Foreground(colorDimmed)
	deckHighlightStyle = lipgloss.NewStyle().Background(colorHighlightBg)
	deckStatusOKStyle = lipgloss.NewStyle().Foreground(colorGreen)
	deckStatusFailStyle = lipgloss.NewStyle().Foreground(colorRed)
	deckStatusWarnStyle = lipgloss.NewStyle().Foreground(colorYellow)
	deckRoleUserStyle = lipgloss.NewStyle().Foreground(colorBlue)
	deckRoleAsstStyle = lipgloss.NewStyle().Foreground(colorRed)
	deckModalBgStyle = lipgloss.NewStyle().Background(colorPanelBg).Foreground(colorForeground).Padding(1, 2)
	deckTabBoxStyle = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(colorDimmed).Padding(0, 1)
	deckTabActiveStyle = lipgloss.NewStyle().Bold(true).Foreground(colorForeground)
	deckTabInactiveStyle = lipgloss.NewStyle().Foreground(colorBrightBlack)
	deckBackgroundStyle = lipgloss.NewStyle().Background(colorBaseBg)
}
