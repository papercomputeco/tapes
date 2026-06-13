package api

import (
	"context"
	"io"
	"net/http"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var _ = Describe("minimal web UI", func() {
	var server *Server

	BeforeEach(func() {
		var err error
		server, err = NewServer(Config{ListenAddr: ":0", EnableWebUI: true}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
	})

	It("does not serve the UI unless explicitly enabled", func() {
		defaultServer, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := defaultServer.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusNotFound))
	})

	It("serves the session browser UI from / without a frontend build", func() {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("text/html"))

		raw, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		// The UI reads the session/trace surface only; the node-view
		// /v1/stems endpoints (and the D3 graph pane that consumed
		// them) are gone, as are all external script tags.
		Expect(string(raw)).To(ContainSubstring("/v1/sessions?limit="))
		Expect(string(raw)).To(ContainSubstring("/traces?payload=preview"))
		Expect(string(raw)).NotTo(ContainSubstring("/v1/stems"))
		Expect(string(raw)).NotTo(ContainSubstring("<script src="))
	})

	It("does not catch all unknown routes", func() {
		for _, path := range []string{"/graph", "/not-the-ui"} {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusNotFound))
		}
	})
})
