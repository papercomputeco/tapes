package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/swaggo/swag"

	// Register the generated OpenAPI spec with swag.ReadDoc.
	_ "github.com/papercomputeco/tapes/docs"
)

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
    <script id="api-reference" data-url="/swagger/doc.json"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.52.1"></script>
  </body>
</html>`

// mountSwagger registers the API reference routes. The viewer JS is fetched
// from a CDN at view time, so the only binary cost is this file plus the
// generated docs package.
func (s *Server) mountSwagger(app *fiber.App) {
	app.Get("/swagger/doc.json", func(c *fiber.Ctx) error {
		doc, err := swag.ReadDoc()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		}
		return c.Type("json").SendString(doc)
	})

	app.Get("/swagger", func(c *fiber.Ctx) error {
		return c.Type("html").SendString(scalarHTML)
	})
}
