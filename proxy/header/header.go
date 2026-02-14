// Package header provides header filtering for the tapes proxy.
//
// This proxy sits between a client and an upstream LLM provider like so:
//
//	Client <--> Proxy <--> Upstream LLM Provider
//
// and headers are handled accordingly as each leg negotiates compression, hops,
// encoding, etc. independently.
package header

import (
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// Handler manages headers between proxy connections.
type Handler struct{}

// NewHandler creates a new header Handler.
func NewHandler() *Handler {
	return &Handler{}
}

// AgentNameHeader is the optional header used to tag agent requests.
const AgentNameHeader = "X-Tapes-Agent-Name"

// skipRequest is the set of request headers (client --> proxy --> upstream)
// that are not forwarded to the upstream LLM provider.
var skipRequest = map[string]struct{}{
	// Hop-by-hop headers: only meaningful for a single transport-level connection.
	"Connection": {},

	// The Host header is rewritten by Go's http.Transport to match the
	// upstream URL. Forwarding the client's Host would confuse virtual-hosted
	// upstreams.
	"Host": {},

	// Accept-Encoding is stripped so that Go's http.Transport adds its own
	// "Accept-Encoding: gzip" and transparently decompresses the upstream
	// response.
	"Accept-Encoding": {},

	// Internal agent routing header.
	AgentNameHeader: {},
}

// skipResponse is the set of upstream response headers (client <-- proxy <-- upstream)
// that are not copied back to the downstream client.
var skipResponse = map[string]struct{}{
	// Hop-by-hop headers: only meaningful for a single transport-level connection.
	"Connection": {},

	// Hop-by-hop headers: fasthttp manages chunked transfer encoding for the
	// client-facing response independently.
	"Transfer-Encoding": {},

	// The proxy always reads a decompressed body (Go's http.Transport strips
	// Content-Encoding after auto-decompression). Forwarding a stale
	// Content-Encoding would claim an encoding the body no longer has.
	// Fiber's compress middleware sets the correct Content-Encoding when it
	// re-compresses the response back down to the client.
	"Content-Encoding": {},

	// The upstream Content-Length reflects the (possibly compressed) upstream
	// body size. After decompression the length changes, and Fiber's compress
	// middleware may re-compress to a different size. Letting Fiber compute
	// the final Content-Length avoids sending an incorrect value.
	"Content-Length": {},
}

// SetUpstreamRequestHeaders copies request headers from the Fiber context to
// the outgoing http.Request, filtering headers that the proxy should not forward
// to the upstream API.
func (h *Handler) SetUpstreamRequestHeaders(c *fiber.Ctx, req *http.Request) {
	c.Request().Header.VisitAll(func(key, value []byte) {
		k := string(key)
		if _, skip := skipRequest[k]; !skip {
			req.Header.Set(k, string(value))
		}
	})
}

// SetClientResponseHeaders copies response headers from the upstream API
// http.Response to the Fiber context, filtering headers that the proxy should
// not forward back down to the client.
func (h *Handler) SetClientResponseHeaders(c *fiber.Ctx, resp *http.Response) {
	for k, v := range resp.Header {
		if _, skip := skipResponse[k]; !skip {
			c.Set(k, strings.Join(v, ", "))
		}
	}
}
