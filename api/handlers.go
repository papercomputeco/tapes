package api

import "github.com/gofiber/fiber/v2"

// handlePing returns a simple health check response.
func (s *Server) handlePing(c *fiber.Ctx) error {
	return c.JSON("pong")
}
