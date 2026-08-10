package logger_test

import (
	"bytes"
	"context"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/logger"
)

var _ = Describe("request logging context", func() {
	It("stores and retrieves one request ID and its scoped logger", func() {
		requestLog := logger.NewNoop()
		ctx := logger.WithRequest(context.Background(), "request-123", requestLog)

		Expect(logger.RequestIDFromContext(ctx)).To(Equal("request-123"))
		Expect(logger.RequestLoggerFromContext(ctx)).To(BeIdenticalTo(requestLog))
	})

	It("returns empty values when no request scope is present", func() {
		Expect(logger.RequestIDFromContext(context.Background())).To(BeEmpty())
		Expect(logger.RequestLoggerFromContext(context.Background())).To(BeNil())
	})

	It("keeps provider correlation separate from a missing Paper request ID", func() {
		var logs bytes.Buffer
		ctx := logger.WithRequestMetadata(
			context.Background(),
			"",
			"provider-request-7",
			slog.New(slog.NewJSONHandler(&logs, nil)),
		)

		Expect(logger.RequestIDFromContext(ctx)).To(BeEmpty())
		Expect(logger.UpstreamRequestIDFromContext(ctx)).To(Equal("provider-request-7"))
		logger.RequestLoggerFromContext(ctx).Info("provider-only correlation")
		Expect(logs.String()).To(ContainSubstring(`"upstream_request_id":"provider-request-7"`))
		Expect(logs.String()).NotTo(ContainSubstring(`"request_id"`))
	})
})
