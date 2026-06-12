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

	It("serves the D3 UI from / without a frontend build", func() {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("text/html"))

		raw, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).To(ContainSubstring("d3@7.9.0"))
		Expect(string(raw)).To(ContainSubstring("integrity=\"sha256-8glLv2FBs1lyLE/kVOtsSw8OQswQzHr5IfwVj864ZTk=\""))
		Expect(string(raw)).To(ContainSubstring("/v1/stems/"))
		Expect(string(raw)).To(ContainSubstring("/v1/stems?limit="))
		Expect(string(raw)).NotTo(ContainSubstring("/v1/sessions/summary"))
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
