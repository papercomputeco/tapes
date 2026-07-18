package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

func testPostgresDSN() (string, error) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		return "", errors.New("TEST_POSTGRES_DSN is not set; run postgres integration tests via `dagger call test` so the Dagger Postgres service is available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return "", fmt.Errorf("connect to test postgres: %w", err)
	}
	defer conn.Close(context.Background())

	if err := conn.Ping(ctx); err != nil {
		return "", fmt.Errorf("ping test postgres at TEST_POSTGRES_DSN: %w", err)
	}

	return dsn, nil
}

var _ = Describe("Driver", func() {
	Describe("NewDriver", func() {
		It("returns an error for invalid connection string", func() {
			_, err := postgres.NewDriver(context.Background(), "host=invalid port=9999 user=bad dbname=bad sslmode=disable connect_timeout=1")
			Expect(err).To(HaveOccurred())
			fmt.Fprintf(GinkgoWriter, "expected error: %v\n", err)
		})
	})
})
