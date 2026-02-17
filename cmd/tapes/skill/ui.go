package skillcmder

import (
	"fmt"
	"io"
	"time"

	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/lipgloss"
)

var (
	successMark = lipgloss.NewStyle().Foreground(lipgloss.Color("82")).Render("✓")
	failMark    = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Render("✗")
	stepStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
)

// step prints a step-in-progress indicator, executes fn, then prints
// a success or failure checkmark. Returns the error from fn.
func step(w io.Writer, msg string, fn func() error) error {
	fmt.Fprintf(w, "  %s %s", stepStyle.Render("·"), msg)

	start := time.Now()
	err := fn()
	elapsed := time.Since(start)

	// Clear the line and reprint with result
	fmt.Fprintf(w, "\r  %s %s %s\n",
		mark(err),
		msg,
		stepStyle.Render(fmt.Sprintf("(%s)", formatDuration(elapsed))),
	)

	return err
}

func mark(err error) string {
	if err != nil {
		return failMark
	}
	return successMark
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// renderMarkdown renders markdown content for terminal display using glamour.
func renderMarkdown(content string) (string, error) {
	r, err := glamour.NewTermRenderer(
		glamour.WithAutoStyle(),
		glamour.WithWordWrap(80),
	)
	if err != nil {
		return content, err
	}

	rendered, err := r.Render(content)
	if err != nil {
		return content, err
	}

	return rendered, nil
}
