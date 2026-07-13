package api

import (
	_ "embed"

	"github.com/gofiber/fiber/v2"
)

// openAPISpec is the versioned OpenAPI 3.0.3 contract, generated from the swag
// docs by `make openapi` (cmd/gen-openapi). It is the same document paper vendors
// to generate its Rust client, so the viewer and the client always agree.
//
//go:embed openapi.yaml
var openAPISpec []byte

// scalarHTML loads the Scalar API reference viewer from a CDN. Keeping the
// viewer JS out of our binary saves ~12 MB compared to embedding swagger-ui.
const scalarHTML = `<!doctype html>
<html>
  <head>
    <title>Tapes API</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script id="api-reference" data-url="/swagger/openapi.yaml"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.52.1"></script>
  </body>
</html>`

// mountSwagger registers the API reference routes. The viewer JS is fetched
// from a CDN at view time, so the only binary cost is this file plus the
// embedded spec.
func (s *Server) mountSwagger(app *fiber.App) {
	app.Get("/swagger/openapi.yaml", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, "application/yaml")
		return c.Send(openAPISpec)
	})

	// Backward-compat: the viewer served Swagger 2.0 JSON here before the
	// switch to the embedded 3.0.3 spec. Redirect stale bookmarks / tooling
	// to the new path instead of 404ing them.
	app.Get("/swagger/doc.json", func(c *fiber.Ctx) error {
		return c.Redirect("/swagger/openapi.yaml", fiber.StatusMovedPermanently)
	})

	app.Get("/swagger", func(c *fiber.Ctx) error {
		return c.Type("html").SendString(scalarHTML)
	})
}
