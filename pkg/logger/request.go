package logger

import (
	"context"
	"log/slog"
)

type requestContext struct {
	requestID         string
	upstreamRequestID string
	logger            *slog.Logger
}

// WithRequestMetadata restores correlation copied across an asynchronous
// boundary and builds a logger whose fields keep Paper and provider IDs
// distinct.
func WithRequestMetadata(
	ctx context.Context,
	requestID string,
	upstreamRequestID string,
	log *slog.Logger,
) context.Context {
	log = WithRequestFields(log, requestID, upstreamRequestID)
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestContextKey{}, requestContext{
		requestID:         requestID,
		upstreamRequestID: upstreamRequestID,
		logger:            log,
	})
}

// WithRequestFields returns a child logger with separate Paper and provider
// request-ID fields. Empty values are omitted and never substitute for one
// another.
func WithRequestFields(log *slog.Logger, requestID, upstreamRequestID string) *slog.Logger {
	if log == nil {
		return nil
	}
	if requestID != "" {
		log = log.With("request_id", requestID)
	}
	if upstreamRequestID != "" {
		log = log.With("upstream_request_id", upstreamRequestID)
	}
	return log
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

// UpstreamRequestIDFromContext returns the provider-issued request ID stored
// in ctx. It is never substituted for a missing Paper request ID.
func UpstreamRequestIDFromContext(ctx context.Context) string {
	request, ok := requestFromContext(ctx)
	if !ok {
		return ""
	}
	return request.upstreamRequestID
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
