package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/tapesoapi/oasfiber"
)

// mountCassettes registers the cassette surface on the API server.
//
// This is the API server, not a gateway in front of it. A client that finds a
// cassette in the discovery document calls it on the same origin and the same
// port, through the same metrics, recovery, compression, and tenant middleware
// as every core endpoint — which is the point of publishing cassettes under
// /v1/cassettes rather than beside /v1.
//
// Registration order is load-bearing. Core's own endpoints inside the
// cassette namespace are registered before the proxy wildcards, because Fiber
// matches in order and core owns /v1/cassettes/<name>/openapi.json.
func (s *Server) mountCassettes(router *oasfiber.Router) {
	app := router.App()

	// /openapi serves the merged description; describing it inside that
	// description would be circular. Same for a single cassette's cached
	// document.
	app.Get("/openapi", s.handleCassetteAggregate)
	app.Get("/v1/cassettes/:name/openapi.json", s.handleCassetteSpec)

	router.Get("/v1/cassettes", s.handleCassetteDiscovery,
		oasfiber.Doc("listCassettes").
			Summary("Discover installed cassettes").
			Description("Lists the cassettes served by this API, their public route and OpenAPI "+
				"paths, and any configured cassette sources that could not be loaded.").
			Tag("cassettes").
			JSONResponse(200, "What is installed here, and what failed", s.schema(Discovery{})))

	// The proxy mounts are not published as operations. They are a namespace,
	// not an endpoint: what is reachable through them is whatever the installed
	// cassettes declare, and those operations appear in the merged document at
	// /openapi under their real paths. Publishing the mount itself would hand a
	// generated client a wildcard it cannot call.
	app.All("/v1/cassettes/:name", s.handleCassetteProxy)
	app.All("/v1/cassettes/:name/*", s.handleCassetteProxy)
}

// SetCassetteSources configures exact full OpenAPI document URLs on this
// server's lifetime-owned runner.
func (s *Server) SetCassetteSources(sources []string) {
	if runner, ok := s.cassetteSpecs.(*cassetterunner.Runner); ok {
		runner.SetSources(sources)
	}
}

// RefreshCassetteSpecs refreshes this server's cassette OpenAPI cache once.
func (s *Server) RefreshCassetteSpecs(ctx context.Context) []error {
	return s.cassetteSpecs.Refresh(ctx)
}

// StartCassetteSpecRefresh begins this server's source-resolution lifecycle.
// It retries quickly during startup so sidecars can become ready alongside
// tapes, then settles onto the configured refresh interval.
func (s *Server) StartCassetteSpecRefresh(ctx context.Context, interval time.Duration) {
	go s.runCassetteSpecRefresh(ctx, interval)
}

func (s *Server) runCassetteSpecRefresh(ctx context.Context, interval time.Duration) {
	const (
		startupWindow = 15 * time.Second
		startupRetry  = 500 * time.Millisecond
	)

	deadline := time.Now().Add(startupWindow)
	var errs []error
	for {
		errs = s.RefreshCassetteSpecs(ctx)
		if len(errs) == 0 || ctx.Err() != nil || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(startupRetry):
		}
	}
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.RefreshCassetteSpecs(ctx)
		}
	}
}

// handleCassetteDiscovery publishes what is installed here, and what failed.
//
// The response carries a strong ETag over the encoded document and honors
// If-None-Match, because discovery is the surface registry consumers poll:
// a cassette keeping an entity catalog fresh issues conditional GETs against
// this document's version and re-crawls only when it moves. The digest is
// computed over the exact bytes served, so the ETag changes exactly when the
// admitted set — cassettes, problems, claims-bearing manifests, entities —
// changes.
func (s *Server) handleCassetteDiscovery(c *fiber.Ctx) error {
	document := buildCassetteDiscovery(
		s.cassettes, string(currentContractVersion(s.contracts)), s.cassetteSpecs.Status)
	encoded, err := json.Marshal(document)
	if err != nil {
		s.logger.Error("encode cassette discovery", "error", err)
		return cassetteProblem(c, fiber.StatusInternalServerError, "encoding_failed",
			"could not encode the discovery document")
	}

	sum := sha256.Sum256(encoded)
	etag := `"sha256:` + hex.EncodeToString(sum[:]) + `"`
	c.Set(fiber.HeaderETag, etag)
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	if c.Get(fiber.HeaderIfNoneMatch) == etag {
		c.Status(fiber.StatusNotModified)

		return nil
	}

	return c.Send(encoded)
}

// handleCassetteSpec returns one cassette's document from core's cache.
//
// It is served from memory rather than proxied to the cassette on purpose: a
// client must be able to read the surface of a cassette that is currently down,
// which is exactly when it most needs to know what the surface was.
func (s *Server) handleCassetteSpec(c *fiber.Ctx) error {
	name, err := cassette.ParseName(c.Params("name"))
	if err != nil {
		return cassetteProblem(c, fiber.StatusNotFound, "unknown_cassette", err.Error())
	}
	if _, ok := s.cassettes.Get(name); !ok {
		return cassetteProblem(c, fiber.StatusNotFound, "unknown_cassette",
			fmt.Sprintf("cassette %q is not installed here", name))
	}

	unavailable := fmt.Sprintf("no OpenAPI document has been fetched from cassette %q", name)
	document, digest, ok := s.cassetteSpecs.Spec(name)
	if !ok {
		return cassetteProblem(c, fiber.StatusServiceUnavailable, "spec_unavailable", unavailable)
	}

	// The digest is the manifest-independent identity of what core is serving:
	// it is computed over the republished document, so a client that caches on
	// this ETag revalidates exactly when the paths it generated from change.
	etag := `"` + string(digest) + `"`
	c.Set(fiber.HeaderETag, etag)
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	if c.Get(fiber.HeaderIfNoneMatch) == etag {
		c.Status(fiber.StatusNotModified)

		return nil
	}

	return c.Send(document)
}

// handleCassetteAggregate returns one description of this whole origin: core's
// own API surface plus every installed cassette's, merged.
//
// The core half comes from the live parser every route registered itself into,
// which is the only description of core that cannot be stale: it is the
// registrations themselves, not a file someone regenerates after changing them.
//
// Compiled per request because the cassette half changes per request — a
// cassette mounted a second ago belongs in the answer. Compile does no I/O and
// is deterministic, so the cost is CPU over a tree already in memory, and two
// requests a millisecond apart return byte-identical documents.
func (s *Server) handleCassetteAggregate(c *fiber.Ctx) error {
	document, err := s.cassetteSpecs.Document(c.UserContext(), s.openapi)
	if err != nil {
		return cassetteProblem(c, fiber.StatusInternalServerError, "aggregate_failed", err.Error())
	}

	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)

	return c.Send(document)
}

// handleCassetteProxy forwards a request under /v1/cassettes/<name> to the
// cassette that owns it.
func (s *Server) handleCassetteProxy(c *fiber.Ctx) error {
	instance, forwarded, ok := s.cassettes.Lookup(c.Path())
	if !ok {
		return cassetteProblem(c, fiber.StatusNotFound, "unknown_cassette",
			"no cassette serves "+c.Path())
	}

	return s.proxyToCassette(c, instance, forwarded)
}

// proxyToCassette reverse-proxies one request to a cassette's own listener.
//
// forwarded is the path on that listener: the registry has already swapped the
// canonical public prefix for the prefix the cassette actually serves. The
// cassette never learns that /v1/cassettes exists.
//
// The response is streamed, not buffered: a cassette may hold a response open
// indefinitely — a server-sent event stream is the reason this matters — and a
// proxy that waits for the end delivers nothing at all for those. Request
// bodies are still read whole, which is what a cassette call is: the streaming
// direction is the answer, not the ask.
func (s *Server) proxyToCassette(c *fiber.Ctx, instance *cassetterunner.Instance, forwarded string) error {
	target, err := url.Parse(instance.URL)
	if err != nil {
		return cassetteProblem(c, fiber.StatusBadGateway, "bad_target",
			fmt.Sprintf("cassette %q has an unusable endpoint: %v", instance.Name, err))
	}

	// The proxy outlives this handler: it is still writing the response body
	// after the handler returns. So the request it holds is a detached copy,
	// cancelled by this function's own machinery rather than by the request's
	// context — a context fasthttp recycles underneath a live stream would
	// cancel it mid-conversation. The cancel has two triggers: fasthttp closing
	// the body stream (a finished response, or a disconnect it noticed while
	// writing), and the connection watch below, which is what notices a client
	// that left a stream too quiet to fail a write.
	upstream, cancel := context.WithCancel(context.Background())
	inbound, err := detachRequest(upstream, c)
	if err != nil {
		cancel()

		return cassetteProblem(c, fiber.StatusBadGateway, "bad_request",
			fmt.Sprintf("could not forward this request to cassette %q: %v", instance.Name, err))
	}

	proxy := &httputil.ReverseProxy{
		Transport: s.cassetteClient.Transport,
		Rewrite: func(proxied *httputil.ProxyRequest) {
			cassetterunner.RewriteProxyRequest(proxied, target, forwarded, instance.Name)
		},
		// A cassette being down is an expected state, not a core failure, so it
		// gets a 502 that names the cassette rather than a bare gateway error
		// an operator would have to correlate with logs.
		ErrorHandler: func(writer http.ResponseWriter, request *http.Request, err error) {
			s.logger.Warn("cassette request failed",
				"cassette", string(instance.Name), "url", instance.URL, "path", request.URL.Path, "error", err)

			writer.Header().Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
			writer.WriteHeader(http.StatusBadGateway)
			if encodeErr := json.NewEncoder(writer).Encode(map[string]string{
				"error": "cassette_unavailable",
				"message": fmt.Sprintf("cassette %q at %s did not respond: %v",
					instance.Name, instance.URL, err),
			}); encodeErr != nil {
				s.logger.Warn("writing cassette proxy error response",
					"cassette", string(instance.Name), "error", encodeErr)
			}
		},
	}

	reader, writer := io.Pipe()
	relay := newCassetteResponseWriter(writer)
	go func() {
		// Order matters on the way out: settle the status first so a proxy that
		// unwound without writing one cannot leave await blocked, then close the
		// pipe so fasthttp sees the end of the body. The cancel is last-resort
		// hygiene here — the proxy is finished with the upstream either way.
		defer writer.Close()
		defer relay.finish()
		defer cancel()

		proxy.ServeHTTP(relay, inbound)
	}()

	status, header := relay.await()
	c.Status(status)
	size := -1
	for key, values := range header {
		switch http.CanonicalHeaderKey(key) {
		// fasthttp frames the body itself from the size handed to
		// SetBodyStream, so echoing the upstream's framing would either
		// contradict it or duplicate it.
		case fiber.HeaderContentLength:
			if declared, err := strconv.Atoi(values[0]); err == nil && declared >= 0 {
				size = declared
			}
		case fiber.HeaderTransferEncoding:
		default:
			for _, value := range values {
				c.Response().Header.Add(key, value)
			}
		}
	}

	// A cassette that declared its length keeps it, so an ordinary JSON call is
	// framed exactly as it was before this was a stream. Everything else goes
	// out chunked, which is the only framing available for a body whose end is
	// not yet known.
	c.Context().Response.SetBodyStream(&cassetteBodyStream{PipeReader: reader, cancel: cancel}, size)

	// fasthttp pulls the body: while a stream is idle it is blocked reading the
	// pipe, where a client hanging up makes no sound — the disconnect would
	// surface only on the next event, and an abandoned quiet stream would hold
	// its cassette connection until then. For an event stream the connection
	// carries no further requests, so it is dedicated to this response and
	// watched directly; the watch is what cancels the upstream with no event's
	// help. Ordinary sized responses finish promptly and need neither.
	if acceptsEventStream(c) {
		c.Context().SetConnectionClose()
		go watchClientGone(c.Context().Conn(), cancel)
	}

	return nil
}

// cassetteProblem writes a machine-readable error. The code is stable and the
// message is not, which is the split a client needs to branch on a failure
// without parsing prose.
func cassetteProblem(c *fiber.Ctx, status int, code, message string) error {
	return c.Status(status).JSON(map[string]string{"error": code, "message": message})
}
