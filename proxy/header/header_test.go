package header

import (
	"net/http"
	"net/http/httptest"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SetUpstreamRequestHeaders", func() {
	var (
		app *fiber.App
		hh  *Handler
	)

	BeforeEach(func() {
		app = fiber.New()
		hh = NewHandler()
	})

	AfterEach(func() {
		app.Shutdown()
	})

	It("forwards standard headers to the upstream request", func() {
		var got http.Header

		app.Post("/test", func(c *fiber.Ctx) error {
			req, _ := http.NewRequest(http.MethodPost, "http://upstream/test", nil)
			hh.SetUpstreamRequestHeaders(c, req)
			got = req.Header
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		req.Header.Set("Authorization", "Bearer token123")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Api-Key", "secret")

		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		Expect(got.Get("Authorization")).To(Equal("Bearer token123"))
		Expect(got.Get("Content-Type")).To(Equal("application/json"))
		Expect(got.Get("X-Api-Key")).To(Equal("secret"))
	})

	It("strips the Connection header", func() {
		var got http.Header

		app.Post("/test", func(c *fiber.Ctx) error {
			req, _ := http.NewRequest(http.MethodPost, "http://upstream/test", nil)
			hh.SetUpstreamRequestHeaders(c, req)
			got = req.Header
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		req.Header.Set("Connection", "keep-alive")

		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		Expect(got.Get("Connection")).To(BeEmpty())
	})

	It("strips the Host header", func() {
		var got http.Header

		app.Post("/test", func(c *fiber.Ctx) error {
			req, _ := http.NewRequest(http.MethodPost, "http://upstream/test", nil)
			hh.SetUpstreamRequestHeaders(c, req)
			got = req.Header
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		req.Header.Set("Host", "client.example.com")

		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		Expect(got.Get("Host")).To(BeEmpty())
	})

	It("strips Accept-Encoding so Go's http.Transport negotiates its own", func() {
		var got http.Header

		app.Post("/test", func(c *fiber.Ctx) error {
			req, _ := http.NewRequest(http.MethodPost, "http://upstream/test", nil)
			hh.SetUpstreamRequestHeaders(c, req)
			got = req.Header
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		req.Header.Set("Accept-Encoding", "gzip, deflate, br")
		req.Header.Set("Authorization", "Bearer token123")

		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		Expect(got.Get("Accept-Encoding")).To(BeEmpty())
		// Other headers still forwarded
		Expect(got.Get("Authorization")).To(Equal("Bearer token123"))
	})
})

var _ = Describe("SetClientResponseHeaders", func() {
	var (
		app *fiber.App
		hh  *Handler
	)

	BeforeEach(func() {
		app = fiber.New()
		hh = NewHandler()
	})

	AfterEach(func() {
		app.Shutdown()
	})

	It("forwards standard upstream response headers to the client", func() {
		app.Get("/test", func(c *fiber.Ctx) error {
			resp := &http.Response{
				Header: http.Header{
					"Content-Type":   {"application/json"},
					"X-Request-Id":   {"abc-123"},
					"X-Custom-Value": {"hello"},
				},
			}
			hh.SetClientResponseHeaders(c, resp)
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		Expect(resp.Header.Get("Content-Type")).To(Equal("application/json"))
		Expect(resp.Header.Get("X-Request-Id")).To(Equal("abc-123"))
		Expect(resp.Header.Get("X-Custom-Value")).To(Equal("hello"))
	})

	It("strips the Connection header", func() {
		app.Get("/test", func(c *fiber.Ctx) error {
			resp := &http.Response{
				Header: http.Header{
					"Connection": {"keep-alive"},
				},
			}
			hh.SetClientResponseHeaders(c, resp)
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		Expect(resp.Header.Get("Connection")).To(BeEmpty())
	})

	It("strips the Transfer-Encoding header", func() {
		app.Get("/test", func(c *fiber.Ctx) error {
			resp := &http.Response{
				Header: http.Header{
					"Transfer-Encoding": {"chunked"},
				},
			}
			hh.SetClientResponseHeaders(c, resp)
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		Expect(resp.Header.Get("Transfer-Encoding")).To(BeEmpty())
	})

	It("strips Content-Encoding since the proxy body is always decompressed", func() {
		app.Get("/test", func(c *fiber.Ctx) error {
			resp := &http.Response{
				Header: http.Header{
					"Content-Encoding": {"gzip"},
					"X-Request-Id":     {"abc-123"},
				},
			}
			hh.SetClientResponseHeaders(c, resp)
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		Expect(resp.Header.Get("Content-Encoding")).To(BeEmpty())
		// Other headers still forwarded
		Expect(resp.Header.Get("X-Request-Id")).To(Equal("abc-123"))
	})

	It("strips Content-Length since Fiber recomputes it after compression", func() {
		app.Get("/test", func(c *fiber.Ctx) error {
			resp := &http.Response{
				Header: http.Header{
					"Content-Length": {"1234"},
					"X-Request-Id":   {"abc-123"},
				},
			}
			hh.SetClientResponseHeaders(c, resp)
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		// Content-Length should not carry the upstream value.
		// Fiber sets its own based on the actual response body.
		Expect(resp.Header.Get("Content-Length")).NotTo(Equal("1234"))
		// Other headers still forwarded
		Expect(resp.Header.Get("X-Request-Id")).To(Equal("abc-123"))
	})

	It("joins multi-value response headers with commas", func() {
		app.Get("/test", func(c *fiber.Ctx) error {
			resp := &http.Response{
				Header: http.Header{
					"X-Multi": {"value1", "value2"},
				},
			}
			hh.SetClientResponseHeaders(c, resp)
			return c.SendStatus(fiber.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()

		Expect(resp.Header.Get("X-Multi")).To(Equal("value1, value2"))
	})
})
