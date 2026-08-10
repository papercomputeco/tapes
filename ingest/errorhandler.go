package ingest

import (
	"errors"
	"log/slog"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// newBodyLimitErrorHandler returns the app-level error handler: it makes the
// body-limit 413 observable (one metric sample, one warn line, JSON envelope)
// and delegates every other error verbatim to fiber.DefaultErrorHandler.
func newBodyLimitErrorHandler(log *slog.Logger, metrics *Metrics) fiber.ErrorHandler {
	return func(c *fiber.Ctx, err error) error {
		// Match the sentinel, not the status: a handler-returned 413 is not a
		// body-limit rejection. Only ingest routes are counted — an oversized
		// POST to any other path keeps the default response and no sample.
		p := c.Path()
		ingestRoute := c.Method() == fiber.MethodPost &&
			(p == "/v1/ingest" || p == "/v1/ingest/transcript")
		if !errors.Is(err, fiber.ErrRequestEntityTooLarge) || !ingestRoute {
			return fiber.DefaultErrorHandler(c, err)
		}

		// The body was never parsed, so the provider is genuinely unknown;
		// zero bodyBytes keeps the accepted-size histogram untouched.
		metrics.ObserveWrite("", ResultRejectOversize, 0)
		log.Warn("ingest body over limit",
			"content_length", c.Request().Header.ContentLength(),
			"limit", MaxIngestBodyBytes,
			"path", c.Path(),
		)

		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(llm.ErrorResponse{
			Error: "request body exceeds the ingest size limit",
		})
	}
}
