package logger

import (
	"context"
	"log/slog"
)

type requestContext struct {
	requestID string
	logger    *slog.Logger
}

type requestContextKey struct{}

// WithRequest stores the canonical request ID and its scoped logger in ctx.
func WithRequest(ctx context.Context, requestID string, log *slog.Logger) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestContextKey{}, requestContext{
		requestID: requestID,
		logger:    log,
	})
}

// RequestIDFromContext returns the canonical request ID stored in ctx.
func RequestIDFromContext(ctx context.Context) string {
	request, ok := requestFromContext(ctx)
	if !ok {
		return ""
	}
	return request.requestID
}

// RequestLoggerFromContext returns the request-scoped logger stored in ctx.
func RequestLoggerFromContext(ctx context.Context) *slog.Logger {
	request, ok := requestFromContext(ctx)
	if !ok {
		return nil
	}
	return request.logger
}

func requestFromContext(ctx context.Context) (requestContext, bool) {
	if ctx == nil {
		return requestContext{}, false
	}
	request, ok := ctx.Value(requestContextKey{}).(requestContext)
	return request, ok
}
