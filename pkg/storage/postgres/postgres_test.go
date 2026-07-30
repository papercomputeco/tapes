package postgres_test

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

var _ = Describe("Driver", func() {
	Describe("NewDriver", func() {
		It("returns an error for invalid connection string", func() {
			_, err := postgres.NewDriver(context.Background(), "host=invalid port=9999 user=bad dbname=bad sslmode=disable connect_timeout=1")
			Expect(err).To(HaveOccurred())
			fmt.Fprintf(GinkgoWriter, "expected error: %v\n", err)
		})
	})
})
