package api

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
)

const (
	// paperAuthHeader is the auth header the platform edge terminates
	// today (a WorkOS JWT the gateway validates and strips). For direct
	// browser→tapes reads (PCC-945) the console attaches a tapes-minted
	// browser token on the same header instead; the browserTokenPrefix
	// keeps the two flows distinguishable so gateway-bound values pass
	// through untouched.
	paperAuthHeader = "X-Paper-Auth"

	// browserTokenPrefix versions the tapes browser token format. A token
	// is "tbt_v1.<base64url(claims)>.<base64url(hmac)>" where the HMAC is
	// computed over everything before the final dot, binding the version
	// prefix into the signature.
	browserTokenPrefix = "tbt_v1."

	// defaultBrowserTokenTTL bounds token lifetime when the server config
	// leaves BrowserTokenTTL zero. Short enough that a leaked token goes
	// stale quickly, long enough that the console mints once per session
	// view rather than per request.
	defaultBrowserTokenTTL = 10 * time.Minute
)

// browserTokenClaims is the payload signed into a browser read token. The
// org is the tenant every read is scoped to; the subject is the WorkOS user
// id carried the same way the gateway-stamped x-paper-auth-subject header is.
type browserTokenClaims struct {
	OrgID     string `json:"org_id"`
	Subject   string `json:"subject,omitempty"`
	ExpiresAt int64  `json:"exp"`
	// Nonce makes every issuance unique even for identical claims, so a
	// future revocation denylist could target one token without catching
	// every concurrently-minted sibling.
	Nonce string `json:"nti,omitempty"`
}

// mintBrowserToken signs claims into the compact token format. The signer
// itself is deterministic; issuance uniqueness comes from the Nonce the
// mint handler stamps into the claims.
func mintBrowserToken(secret []byte, claims browserTokenClaims) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("encoding browser token claims: %w", err)
	}
	signed := browserTokenPrefix + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// errInvalidBrowserToken is deliberately generic: verification failures all
// collapse to one message so a caller can't distinguish tampering from
// expiry probing.
var errInvalidBrowserToken = errors.New("invalid or expired browser token")

// verifyBrowserToken checks the signature and expiry of a token produced by
// mintBrowserToken and returns its claims.
func verifyBrowserToken(secret []byte, token string, now time.Time) (browserTokenClaims, error) {
	var claims browserTokenClaims
	rest, ok := strings.CutPrefix(token, browserTokenPrefix)
	if !ok {
		return claims, errInvalidBrowserToken
	}
	payloadB64, sigB64, ok := strings.Cut(rest, ".")
	if !ok {
		return claims, errInvalidBrowserToken
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return claims, errInvalidBrowserToken
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(browserTokenPrefix + payloadB64))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return claims, errInvalidBrowserToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return claims, errInvalidBrowserToken
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return claims, errInvalidBrowserToken
	}
	if claims.ExpiresAt <= now.Unix() {
		return claims, errInvalidBrowserToken
	}
	return claims, nil
}

// withBrowserToken verifies a tapes browser token presented on X-Paper-Auth
// and rebinds the request's tenant and subject to the token's verified
// claims. Registered after withOrgContext so the signed org overrides the
// blind-trusted X-Tapes-Org-Id header for browser callers; requests without
// a tapes-format token (absent header, or a gateway-bound JWT) pass through
// unchanged.
func (s *Server) withBrowserToken(c *fiber.Ctx) error {
	raw := strings.TrimSpace(c.Get(paperAuthHeader))
	if !strings.HasPrefix(raw, browserTokenPrefix) {
		return c.Next()
	}
	if s.config.BrowserTokenSecret == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(llm.ErrorResponse{Error: "browser tokens not configured"})
	}
	claims, err := verifyBrowserToken([]byte(s.config.BrowserTokenSecret), raw, time.Now())
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(llm.ErrorResponse{Error: err.Error()})
	}
	c.Locals(orgIDLocal, canonicalOrgID(claims.OrgID))
	// Handlers that read the subject header directly (skills) must see the
	// verified subject, never a browser-asserted value.
	c.Request().Header.Set(authSubjectHeader, claims.Subject)
	return c.Next()
}

// BrowserTokenResponse is the response for POST /v1/browser-tokens.
type BrowserTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
	OrgID     string `json:"org_id"`
}

// handleMintBrowserToken handles POST /v1/browser-tokens.
//
//	@Summary		Mint a short-lived browser read token
//	@ID				mintBrowserToken
//	@Description	Mints a short-lived token bound to the request's tenant and subject, suitable for a browser to present on X-Paper-Auth when calling the read endpoints directly (PCC-945). Minting is meant for trusted server-side callers on the same edge-stamped path as every other request; the token only re-asserts an identity that path already carries.
//	@Tags			auth
//	@Produce		json
//	@Success		200	{object}	BrowserTokenResponse
//	@Failure		400	{object}	llm.ErrorResponse	"No tenant on the request"
//	@Failure		501	{object}	llm.ErrorResponse	"Browser tokens not configured"
//	@Router			/v1/browser-tokens [post]
func (s *Server) handleMintBrowserToken(c *fiber.Ctx) error {
	if s.config.BrowserTokenSecret == "" {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "browser tokens not configured"})
	}
	// A mint with no tenant context can only be a misconfiguration (a
	// gateway that stopped stamping the org header). Reads fall back to
	// the nil-org bucket by design, but silently signing a credential
	// scoped to the wrong tenant bucket is worse than failing loud here —
	// the console degrades to its server-fn path on any mint failure.
	orgID := orgIDFromCtx(c)
	if orgID == nilOrgID {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "browser tokens require a tenant: no org on the request"})
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		s.logger.Error("mint browser token nonce", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to mint browser token"})
	}
	ttl := s.config.BrowserTokenTTL
	if ttl <= 0 {
		ttl = defaultBrowserTokenTTL
	}
	expiresAt := time.Now().Add(ttl)
	token, err := mintBrowserToken([]byte(s.config.BrowserTokenSecret), browserTokenClaims{
		OrgID:     orgID,
		Subject:   authSubjectFromCtx(c),
		ExpiresAt: expiresAt.Unix(),
		Nonce:     base64.RawURLEncoding.EncodeToString(nonce),
	})
	if err != nil {
		s.logger.Error("mint browser token", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to mint browser token"})
	}
	return c.JSON(BrowserTokenResponse{
		Token:     token,
		ExpiresAt: expiresAt.UTC().Format(time.RFC3339),
		OrgID:     orgID,
	})
}
