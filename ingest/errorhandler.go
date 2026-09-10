package ingest

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/gofiber/fiber/v3"

	"github.com/papercomputeco/tapes/pkg/llm"
)

// newBodyLimitErrorHandler returns the app-level error handler: it makes the
// body-limit 413 observable (one metric sample, one warn line, JSON envelope)
// and preserves Fiber's standard responses for every other error.
func newBodyLimitErrorHandler(log *slog.Logger, metrics *Metrics) fiber.ErrorHandler {
	return func(c fiber.Ctx, err error) error {
		// Match the sentinel, not the status: a handler-returned 413 is not a
		// body-limit rejection. Only ingest routes are counted — an oversized
		// POST to any other path keeps the default response and no sample.
		p := c.Path()
		ingestRoute := c.Method() == fiber.MethodPost &&
			(p == "/v1/ingest" || p == "/v1/ingest/transcript")
		if !errors.Is(err, fiber.ErrRequestEntityTooLarge) || !ingestRoute {
			// Fiber v3 routes unmatched requests through a custom app-level error
			// handler as shared errors. Calling DefaultErrorHandler directly would
			// produce "Not Found" instead of the framework's normal
			// "Cannot METHOD /path" response, so preserve that response here.
			if !c.Matched() && (errors.Is(err, fiber.ErrNotFound) || errors.Is(err, fiber.ErrMethodNotAllowed)) {
				var fiberErr *fiber.Error
				if errors.As(err, &fiberErr) {
					return c.Status(fiberErr.Code).SendString(fmt.Sprintf("Cannot %s %s", c.Method(), c.Path()))
				}
			}
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
