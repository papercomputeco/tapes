package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("isTransientDeriveRunError", func() {
	DescribeTable("classifies a session derive failure",
		func(err error, transient bool) {
			Expect(isTransientDeriveRunError(fmt.Errorf("derive: %w", err))).To(Equal(transient))
		},
		Entry("ctx canceled", context.Canceled, true),
		Entry("ctx deadline", context.DeadlineExceeded, true),
		Entry("session lock not taken", fmt.Errorf("%w a/b: boom", errDeriveSessionLock), true),
		Entry("connection dropped mid-read", io.ErrUnexpectedEOF, true),
		Entry("closed network connection", net.ErrClosed, true),
		Entry("network error", &net.OpError{Op: "read", Err: errors.New("reset")}, true),
		Entry("connection exception", &pgconn.PgError{Code: "08006"}, true),
		Entry("deadlock", &pgconn.PgError{Code: "40P01"}, true),
		Entry("out of resources", &pgconn.PgError{Code: "53300"}, true),
		Entry("admin shutdown", &pgconn.PgError{Code: "57P01"}, true),
		Entry("never reached the server", safeToRetryError{}, true),
		Entry("constraint violation", &pgconn.PgError{Code: "23505"}, false),
		Entry("malformed session content", errors.New("parse transcript: not a record array"), false),
	)
})

// safeToRetryError stands in for pgconn's unexported errors that report a
// request which never reached the server, such as a closed or busy connection.
type safeToRetryError struct{}

func (safeToRetryError) Error() string     { return "conn closed" }
func (safeToRetryError) SafeToRetry() bool { return true }
