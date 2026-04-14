package api

import "github.com/gofiber/fiber/v2"

// handlePing returns a simple health check response.
//
//	@Summary		Health check
//	@Description	Returns a simple JSON string confirming that the API server is reachable.
//	@Tags			health
//	@Produce		json
//	@Success		200	{string}	string	"pong"
//	@Router			/ping [get]
func (s *Server) handlePing(c *fiber.Ctx) error {
	return c.JSON("pong")
}
