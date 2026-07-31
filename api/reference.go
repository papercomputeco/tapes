package api

import (
	"github.com/gofiber/fiber/v2"
)

// scalarHTML loads the Scalar API reference viewer from a CDN. Keeping the
// viewer JS out of our binary saves ~12 MB compared to embedding swagger-ui.
//
// The viewer reads /openapi — the same document a client reads, compiled from
// the live routes. There is no second spec path for it to point at, so what an
// operator sees in the browser is what a generated client would be built from,
// cassettes included.
const scalarHTML = `<!doctype html>
<html>
  <head>
    <title>Tapes API</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script id="api-reference" data-url="/openapi"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.52.1"></script>
  </body>
</html>`

// mountReference registers the API reference viewer.
//
// It is one route: the HTML. The spec-serving paths this file used to own
// (/swagger/openapi.yaml, and a /swagger/doc.json redirect to it) are gone,
// because they served a checked-in file that no longer exists. Anything that
// fetched them wants /openapi.
//
// This path serves a page about the contract; it is not described by it. A
// route that documents the document is circular, and a generated client has no
// use for an operation whose response is HTML.
func (s *Server) mountReference(app *fiber.App) {
	app.Get("/swagger", func(c *fiber.Ctx) error {
		return c.Type("html").SendString(scalarHTML)
	})
}
