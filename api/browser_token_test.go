package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

const (
	testTokenSecret = "browser-token-test-secret"
	testTokenOrg    = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	testOtherOrg    = "9e107d9d-3721-4b58-a52c-9c2f1a3b4c5d"
	testOrigin      = "https://console.papercompute.com"
)

var _ = Describe("browser tokens", func() {
	Describe("mint and verify", func() {
		now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

		It("round-trips claims through a minted token", func() {
			token, err := mintBrowserToken([]byte(testTokenSecret), browserTokenClaims{
				OrgID:     testTokenOrg,
				Subject:   "user_123",
				ExpiresAt: now.Add(10 * time.Minute).Unix(),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(token).To(HavePrefix(browserTokenPrefix))

			claims, err := verifyBrowserToken([]byte(testTokenSecret), token, now)
			Expect(err).NotTo(HaveOccurred())
			Expect(claims.OrgID).To(Equal(testTokenOrg))
			Expect(claims.Subject).To(Equal("user_123"))
			Expect(claims.ExpiresAt).To(Equal(now.Add(10 * time.Minute).Unix()))
		})

		It("rejects an expired token", func() {
			token, err := mintBrowserToken([]byte(testTokenSecret), browserTokenClaims{
				OrgID:     testTokenOrg,
				ExpiresAt: now.Add(-time.Second).Unix(),
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = verifyBrowserToken([]byte(testTokenSecret), token, now)
			Expect(err).To(MatchError(errInvalidBrowserToken))
		})

		It("rejects a token signed with a different secret", func() {
			token, err := mintBrowserToken([]byte("other-secret"), browserTokenClaims{
				OrgID:     testTokenOrg,
				ExpiresAt: now.Add(10 * time.Minute).Unix(),
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = verifyBrowserToken([]byte(testTokenSecret), token, now)
			Expect(err).To(MatchError(errInvalidBrowserToken))
		})

		It("rejects a token whose payload was swapped after signing", func() {
			token, err := mintBrowserToken([]byte(testTokenSecret), browserTokenClaims{
				OrgID:     testTokenOrg,
				ExpiresAt: now.Add(10 * time.Minute).Unix(),
			})
			Expect(err).NotTo(HaveOccurred())
			forged, err := mintBrowserToken([]byte(testTokenSecret), browserTokenClaims{
				OrgID:     testOtherOrg,
				ExpiresAt: now.Add(10 * time.Minute).Unix(),
			})
			Expect(err).NotTo(HaveOccurred())

			// Graft the forged payload onto the original signature.
			forgedPayload := strings.Split(forged, ".")[1]
			originalSig := strings.Split(token, ".")[2]
			spliced := browserTokenPrefix + forgedPayload + "." + originalSig

			// The forged token verifies against its own signature, so only
			// the spliced combination must fail.
			_, err = verifyBrowserToken([]byte(testTokenSecret), spliced, now)
			Expect(err).To(MatchError(errInvalidBrowserToken))
		})

		It("rejects malformed tokens", func() {
			for _, tok := range []string{
				"",
				"tbt_v1.",
				"tbt_v1.missing-signature",
				"tbt_v1.!!!.!!!",
				"tbt_v2.payload.sig",
				"Bearer eyJhbGciOiJIUzI1NiJ9",
			} {
				_, err := verifyBrowserToken([]byte(testTokenSecret), tok, now)
				Expect(err).To(MatchError(errInvalidBrowserToken), "token %q", tok)
			}
		})
	})

	Describe("POST /v1/browser-tokens", func() {
		newTokenServer := func(config Config) *Server {
			config.ListenAddr = ":0"
			server, err := NewServer(config, inmemory.NewDriver(), tapeslogger.NewNoop())
			Expect(err).NotTo(HaveOccurred())
			return server
		}

		It("returns 501 when no signing secret is configured", func() {
			server := newTokenServer(Config{})

			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/browser-tokens", nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(fiber.StatusNotImplemented))
		})

		It("refuses to mint without a tenant on the request", func() {
			server := newTokenServer(Config{BrowserTokenSecret: testTokenSecret})

			for _, org := range []string{"", "not-a-uuid"} {
				req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/browser-tokens", nil)
				Expect(err).NotTo(HaveOccurred())
				if org != "" {
					req.Header.Set(orgIDHeader, org)
				}
				resp, err := server.app.Test(req)
				Expect(err).NotTo(HaveOccurred())
				resp.Body.Close()
				Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest), "org header %q", org)
			}
		})

		It("mints a unique token per issuance for identical claims", func() {
			server := newTokenServer(Config{BrowserTokenSecret: testTokenSecret})

			mint := func() string {
				req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/browser-tokens", nil)
				Expect(err).NotTo(HaveOccurred())
				req.Header.Set(orgIDHeader, testTokenOrg)
				resp, err := server.app.Test(req)
				Expect(err).NotTo(HaveOccurred())
				defer resp.Body.Close()
				Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
				var body BrowserTokenResponse
				raw, err := io.ReadAll(resp.Body)
				Expect(err).NotTo(HaveOccurred())
				Expect(json.Unmarshal(raw, &body)).To(Succeed())
				return body.Token
			}

			// The nonce makes back-to-back mints distinct even when org,
			// subject, and expiry second all match.
			Expect(mint()).NotTo(Equal(mint()))
		})

		It("mints a verifiable token bound to the request's org and subject", func() {
			server := newTokenServer(Config{BrowserTokenSecret: testTokenSecret})

			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/browser-tokens", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set(orgIDHeader, testTokenOrg)
			req.Header.Set(authSubjectHeader, "user_123")
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))

			var body BrowserTokenResponse
			raw, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(raw, &body)).To(Succeed())
			Expect(body.OrgID).To(Equal(testTokenOrg))
			Expect(body.ExpiresAt).NotTo(BeEmpty())

			claims, err := verifyBrowserToken([]byte(testTokenSecret), body.Token, time.Now())
			Expect(err).NotTo(HaveOccurred())
			Expect(claims.OrgID).To(Equal(testTokenOrg))
			Expect(claims.Subject).To(Equal("user_123"))
		})

		It("respects the configured TTL", func() {
			server := newTokenServer(Config{
				BrowserTokenSecret: testTokenSecret,
				BrowserTokenTTL:    time.Minute,
			})

			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/browser-tokens", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set(orgIDHeader, testTokenOrg)
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			var body BrowserTokenResponse
			raw, err := io.ReadAll(resp.Body)
			Expect(err).NotTo(HaveOccurred())
			Expect(json.Unmarshal(raw, &body)).To(Succeed())

			expiresAt, err := time.Parse(time.RFC3339, body.ExpiresAt)
			Expect(err).NotTo(HaveOccurred())
			Expect(expiresAt).To(BeTemporally("~", time.Now().Add(time.Minute), 10*time.Second))
		})
	})

	Describe("withBrowserToken middleware", func() {
		newStatsServer := func(config Config, driver storage.Driver) *Server {
			config.ListenAddr = ":0"
			server, err := NewServer(config, driver, tapeslogger.NewNoop())
			Expect(err).NotTo(HaveOccurred())
			return server
		}

		getStats := func(server *Server, mutate func(*http.Request)) *http.Response {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/stats", nil)
			Expect(err).NotTo(HaveOccurred())
			if mutate != nil {
				mutate(req)
			}
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			return resp
		}

		It("scopes the request to the token's org, overriding the org header", func() {
			drv := &statsStubDriver{Driver: inmemory.NewDriver()}
			server := newStatsServer(Config{BrowserTokenSecret: testTokenSecret}, drv)
			token, err := mintBrowserToken([]byte(testTokenSecret), browserTokenClaims{
				OrgID:     testTokenOrg,
				ExpiresAt: time.Now().Add(time.Minute).Unix(),
			})
			Expect(err).NotTo(HaveOccurred())

			resp := getStats(server, func(req *http.Request) {
				req.Header.Set(paperAuthHeader, token)
				// A spoofed org header must lose to the signed claim.
				req.Header.Set(orgIDHeader, testOtherOrg)
			})
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
			Expect(drv.lastOrg).To(Equal(testTokenOrg))
		})

		It("rejects an expired token with 401", func() {
			drv := &statsStubDriver{Driver: inmemory.NewDriver()}
			server := newStatsServer(Config{BrowserTokenSecret: testTokenSecret}, drv)
			token, err := mintBrowserToken([]byte(testTokenSecret), browserTokenClaims{
				OrgID:     testTokenOrg,
				ExpiresAt: time.Now().Add(-time.Minute).Unix(),
			})
			Expect(err).NotTo(HaveOccurred())

			resp := getStats(server, func(req *http.Request) {
				req.Header.Set(paperAuthHeader, token)
			})
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(fiber.StatusUnauthorized))
			Expect(drv.calls).To(Equal(0))
		})

		It("rejects a tapes-format token when no secret is configured", func() {
			drv := &statsStubDriver{Driver: inmemory.NewDriver()}
			server := newStatsServer(Config{}, drv)

			resp := getStats(server, func(req *http.Request) {
				req.Header.Set(paperAuthHeader, browserTokenPrefix+"payload.sig")
			})
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(fiber.StatusUnauthorized))
			Expect(drv.calls).To(Equal(0))
		})

		It("passes a non-tapes X-Paper-Auth value through with header org scoping", func() {
			drv := &statsStubDriver{Driver: inmemory.NewDriver()}
			server := newStatsServer(Config{BrowserTokenSecret: testTokenSecret}, drv)

			resp := getStats(server, func(req *http.Request) {
				req.Header.Set(paperAuthHeader, "Bearer eyJhbGciOiJIUzI1NiJ9.e30.sig")
				req.Header.Set(orgIDHeader, testOtherOrg)
			})
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
			Expect(drv.lastOrg).To(Equal(testOtherOrg))
		})
	})

	Describe("CORS", func() {
		newCORSServer := func(origins string) *Server {
			server, err := NewServer(Config{
				ListenAddr:         ":0",
				CORSAllowedOrigins: origins,
				BrowserTokenSecret: testTokenSecret,
			}, inmemory.NewDriver(), tapeslogger.NewNoop())
			Expect(err).NotTo(HaveOccurred())
			return server
		}

		It("answers a read preflight for an allowed origin", func() {
			server := newCORSServer(testOrigin)

			req, err := http.NewRequestWithContext(context.Background(), http.MethodOptions, "/v1/sessions", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Origin", testOrigin)
			req.Header.Set("Access-Control-Request-Method", http.MethodGet)
			req.Header.Set("Access-Control-Request-Headers", paperAuthHeader)
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.Header.Get("Access-Control-Allow-Origin")).To(Equal(testOrigin))
			Expect(resp.Header.Get("Access-Control-Allow-Methods")).NotTo(ContainSubstring("DELETE"))
			// X-Paper-Auth and nothing else — the read surface sends no
			// bodies, so Content-Type stays off the allowlist.
			Expect(resp.Header.Get("Access-Control-Allow-Headers")).To(Equal(paperAuthHeader))
		})

		It("does not allow an unlisted origin", func() {
			server := newCORSServer(testOrigin)

			req, err := http.NewRequestWithContext(context.Background(), http.MethodOptions, "/v1/sessions", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Origin", "https://evil.example.com")
			req.Header.Set("Access-Control-Request-Method", http.MethodGet)
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.Header.Get("Access-Control-Allow-Origin")).To(BeEmpty())
		})

		It("stamps allow-origin on an actual read response", func() {
			server := newCORSServer(testOrigin)

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/ping", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Origin", testOrigin)
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
			Expect(resp.Header.Get("Access-Control-Allow-Origin")).To(Equal(testOrigin))
		})

		It("emits no CORS headers when origins are not configured", func() {
			server := newCORSServer("")

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/ping", nil)
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Origin", testOrigin)
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			Expect(resp.Header.Get("Access-Control-Allow-Origin")).To(BeEmpty())
		})
	})
})
