package api

import (
	_ "embed"

	"github.com/gofiber/fiber/v2"
)

// tapesWebUIHTML is intentionally a tiny, Prometheus-style UI served directly
// from the API binary. D3 is loaded from a CDN so there is no frontend build.
//
//go:embed web_ui.html
var tapesWebUIHTML string

func (s *Server) handleWebUI(c *fiber.Ctx) error {
	return c.Type("html").SendString(tapesWebUIHTML)
}
