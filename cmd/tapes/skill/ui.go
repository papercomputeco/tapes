package skillcmder

import (
	"io"

	"github.com/papercomputeco/tapes/pkg/cliui"
)

// step delegates to the shared cliui.Step spinner.
func step(w io.Writer, msg string, fn func() error) error {
	return cliui.Step(w, msg, fn)
}

// renderMarkdown delegates to the shared cliui.RenderMarkdown.
func renderMarkdown(content string) (string, error) {
	return cliui.RenderMarkdown(content)
}
