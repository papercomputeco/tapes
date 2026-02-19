// Package cliui provides reusable terminal UI helpers (spinners, step indicators,
// markdown rendering) for tapes CLI commands.
package cliui

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/lipgloss"
)

var (
	SuccessMark  = lipgloss.NewStyle().Foreground(lipgloss.Color("82")).Render("✓")
	FailMark     = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Render("✗")
	StepStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	spinnerStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("82"))
)

// spinnerFrames matches bubbletea's spinner.Dot pattern used in the deck TUI.
var spinnerFrames = []string{"⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"}

// Step prints an animated spinner while fn runs, then replaces it with
// a ✓ or ✗ checkmark and elapsed time.
func Step(w io.Writer, msg string, fn func() error) error {
	done := make(chan struct{})
	var mu sync.Mutex

	// Run spinner animation in background
	go func() {
		frame := 0
		ticker := time.NewTicker(80 * time.Millisecond)
		defer ticker.Stop()

		for {
			mu.Lock()
			fmt.Fprintf(w, "\r  %s %s",
				spinnerStyle.Render(spinnerFrames[frame%len(spinnerFrames)]),
				msg,
			)
			mu.Unlock()

			select {
			case <-done:
				return
			case <-ticker.C:
				frame++
			}
		}
	}()

	start := time.Now()
	err := fn()
	elapsed := time.Since(start)

	close(done)

	// Clear the spinner line and print final result
	mu.Lock()
	fmt.Fprintf(w, "\r  %s %s %s\n",
		Mark(err),
		msg,
		StepStyle.Render(fmt.Sprintf("(%s)", FormatDuration(elapsed))),
	)
	mu.Unlock()

	return err
}

// Mark returns a ✓ for nil errors or ✗ for non-nil errors.
func Mark(err error) string {
	if err != nil {
		return FailMark
	}
	return SuccessMark
}

// FormatDuration formats a duration for display (e.g. "12ms" or "3.2s").
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// RenderMarkdown renders markdown content for terminal display using glamour.
func RenderMarkdown(content string) (string, error) {
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
