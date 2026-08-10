package api

import (
	"bytes"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var _ = Describe("request ID middleware", func() {
	It("TestTapesAPIRequestIDMiddlewarePropagation", func() {
		validRequestID := uuid.NewString()
		uppercaseRequestID := strings.ToUpper(validRequestID)
		compactRequestID := strings.ReplaceAll(validRequestID, "-", "")
		scenarios := []struct {
			name     string
			provided string
			preserve bool
		}{
			{name: "valid", provided: validRequestID, preserve: true},
			{name: "missing"},
			{name: "malformed", provided: "not-a-uuid"},
			{name: "non-v4", provided: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
			{name: "uppercase", provided: uppercaseRequestID},
			{name: "compact", provided: compactRequestID},
			{name: "oversized", provided: strings.Repeat("a", 1024)},
		}

		generated := map[string]struct{}{}
		for _, scenario := range scenarios {
			var logs bytes.Buffer
			baseLogger := slog.New(slog.NewJSONHandler(&logs, nil))
			server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), baseLogger)
			Expect(err).NotTo(HaveOccurred())

			var contextRequestID string
			var contextLogger *slog.Logger
			server.app.Get("/request-id-probe", func(c *fiber.Ctx) error {
				contextRequestID = tapeslogger.RequestIDFromContext(c.UserContext())
				contextLogger = tapeslogger.RequestLoggerFromContext(c.UserContext())
				contextLogger.Info("handled probe")
				return c.SendStatus(http.StatusNoContent)
			})

			req, err := http.NewRequest(http.MethodGet, "/request-id-probe", nil)
			Expect(err).NotTo(HaveOccurred())
			if scenario.provided != "" {
				req.Header.Set("X-Request-Id", scenario.provided)
			}

			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred(), scenario.name)
			requestID := resp.Header.Get("X-Request-Id")
			Expect(requestID).NotTo(BeEmpty(), scenario.name)
			Expect(contextRequestID).To(Equal(requestID), scenario.name)
			Expect(contextLogger).NotTo(BeNil(), scenario.name)
			Expect(logs.String()).To(ContainSubstring(`"request_id":"`+requestID+`"`), scenario.name)

			parsed, err := uuid.Parse(requestID)
			Expect(err).NotTo(HaveOccurred(), scenario.name)
			Expect(parsed.Version()).To(Equal(uuid.Version(4)), scenario.name)
			if scenario.preserve {
				Expect(requestID).To(Equal(scenario.provided), scenario.name)
			} else {
				Expect(requestID).NotTo(Equal(scenario.provided), scenario.name)
				generated[requestID] = struct{}{}
			}
		}

		Expect(generated).To(HaveLen(6), "separate invalid or missing attempts need unique IDs")
	})
})
