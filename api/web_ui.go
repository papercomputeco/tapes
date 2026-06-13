package api

import (
	_ "embed"

	"github.com/gofiber/fiber/v2"
)

// tapesWebUIHTML is intentionally a tiny, Prometheus-style UI served directly
// from the API binary. It browses the session/trace read surface with plain
// DOM rendering, so there is no frontend build and no external scripts.
//
//go:embed web_ui.html
var tapesWebUIHTML string

func (s *Server) handleWebUI(c *fiber.Ctx) error {
	return c.Type("html").SendString(tapesWebUIHTML)
}
