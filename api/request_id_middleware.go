package api

import (
	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
)

const (
	requestIDHeader       = "X-Request-Id"
	maxRequestIDTextBytes = 36
)

func requestIDMiddleware(log *slog.Logger) fiber.Handler {
	return func(c *fiber.Ctx) error {
		requestID := canonicalRequestID(c.Get(requestIDHeader))
		requestLog := log.With("request_id", requestID)

		// Replace the untrusted header before any downstream middleware or
		// handler can read, forward, or log it.
		c.Request().Header.Set(requestIDHeader, requestID)
		c.Set(requestIDHeader, requestID)
		c.SetUserContext(tapeslogger.WithRequest(c.Context(), requestID, requestLog))

		return c.Next()
	}
}

func canonicalRequestID(candidate string) string {
	if len(candidate) <= maxRequestIDTextBytes {
		if parsed, err := uuid.Parse(candidate); err == nil &&
			parsed.Version() == uuid.Version(4) && parsed.String() == candidate {
			return candidate
		}
	}
	return uuid.NewString()
}
