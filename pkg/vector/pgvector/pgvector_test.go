package pgvector_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/vector"
	"github.com/papercomputeco/tapes/pkg/vector/pgvector"
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

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(GinkgoWriter, nil))
}

var _ = Describe("Driver", func() {
	Describe("Interface compliance", func() {
		It("should implement vector.Backend", func() {
			var _ vector.Driver = (*pgvector.Driver)(nil)
		})
	})

	Describe("NewDriver", func() {
		It("should return an error when connection string is empty", func() {
			_, err := pgvector.NewDriver(context.TODO(), &pgvector.Config{
				Dimensions: 128,
			}, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("connection string must be provided"))
		})

		It("should return an error when dimensions is zero", func() {
			_, err := pgvector.NewDriver(context.TODO(), &pgvector.Config{
				ConnString: "postgres://localhost:5432/test",
			}, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("dimensions cannot be 0"))
		})
	})

	Describe("ensureSchema dimension check", func() {
		const tableName = "pgvector_dim_check_test"

		var dsn string

		BeforeEach(func() {
			var err error
			dsn, err = testPostgresDSN()
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func(ctx SpecContext) {
			conn, err := pgx.Connect(ctx, dsn)
			Expect(err).NotTo(HaveOccurred())
			defer conn.Close(context.Background())
			_, err = conn.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{tableName}.Sanitize())
			Expect(err).NotTo(HaveOccurred())
		})

		It("refuses to reuse a table whose dimensions differ from the configuration", func(ctx SpecContext) {
			logger := newTestLogger()

			driver, err := pgvector.NewDriver(ctx, &pgvector.Config{
				ConnString: dsn,
				TableName:  tableName,
				Dimensions: 3,
			}, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(driver.Close()).To(Succeed())

			_, err = pgvector.NewDriver(ctx, &pgvector.Config{
				ConnString: dsn,
				TableName:  tableName,
				Dimensions: 4,
			}, logger)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("stores vector(3) embeddings but 4 dimensions are configured"))
		})

		It("reuses an existing table when the dimensions match", func(ctx SpecContext) {
			logger := newTestLogger()

			driver, err := pgvector.NewDriver(ctx, &pgvector.Config{
				ConnString: dsn,
				TableName:  tableName,
				Dimensions: 3,
			}, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(driver.Close()).To(Succeed())

			driver, err = pgvector.NewDriver(ctx, &pgvector.Config{
				ConnString: dsn,
				TableName:  tableName,
				Dimensions: 3,
			}, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(driver.Close()).To(Succeed())
		})
	})
})
