package logger_test

import (
	"context"

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
})
