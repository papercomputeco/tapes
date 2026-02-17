package chroma_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/vector"
	"github.com/papercomputeco/tapes/pkg/vector/chroma"
)

var _ = Describe("Driver", func() {
	var logger *slog.Logger

	BeforeEach(func() {
		logger = tapeslogger.Nop()
	})

	Describe("NewDriver", func() {
		It("should return an error when URL is empty", func() {
			_, err := chroma.NewDriver(chroma.Config{URL: ""}, logger)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("chroma URL is required"))
		})

		It("should use default collection name when not specified", func() {
			// This test would require a running Chroma instance
			// Skipping for unit tests - should be covered in integration tests
			Skip("Requires running Chroma instance")
		})

		It("should succeed after retrying when Chroma becomes available", func() {
			var attempts atomic.Int32

			// The GET request for the collection and the POST to create it
			// are separate requests. Each retry attempt may hit both endpoints.
			// We track total requests and fail the first few to simulate Chroma
			// still starting up.
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempt := attempts.Add(1)

				// Fail the first 4 requests (2 retry cycles: GET+POST each),
				// succeed on the 5th (the GET of the 3rd retry cycle).
				if attempt <= 4 {
					http.Error(w, "service unavailable", http.StatusServiceUnavailable)
					return
				}

				// Return a valid collection response
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{
					"id":   "test-collection-id",
					"name": "tapes",
				})
			}))
			defer server.Close()

			driver, err := chroma.NewDriver(chroma.Config{
				URL:           server.URL,
				MaxRetries:    5,
				RetryDelay:    10 * time.Millisecond,
				MaxRetryDelay: 50 * time.Millisecond,
			}, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(driver).NotTo(BeNil())
			Expect(attempts.Load()).To(BeNumerically(">=", int32(5)))
		})

		It("should return an error after exhausting all retries", func() {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			}))
			defer server.Close()

			_, err := chroma.NewDriver(chroma.Config{
				URL:           server.URL,
				MaxRetries:    3,
				RetryDelay:    10 * time.Millisecond,
				MaxRetryDelay: 50 * time.Millisecond,
			}, logger)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("after 3 attempts"))
		})
	})

	Describe("Interface compliance", func() {
		It("should implement vector.Driver interface", func() {
			// Compile-time check that Driver implements vector.Driver
			var _ vector.Driver = (*chroma.Driver)(nil)
		})
	})
})
