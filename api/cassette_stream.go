package api

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/gofiber/fiber/v2"
)

// cassetteResponseWriter is the http.ResponseWriter a cassette's reverse proxy
// writes into. It exists to bridge two models of writing a response that do not
// otherwise meet: net/http hands a handler a writer and lets it write over the
// life of the response, while fasthttp wants the response handed to it — as
// bytes or as a stream — by the time the handler returns.
//
// gofiber's adaptor bridges them by buffering: it runs the handler to
// completion, collects everything written, and only then gives fasthttp a body.
// For the JSON request and response most cassette calls are, that is invisible.
// For a cassette that streams it is fatal — an endpoint that stays open for the
// length of a conversation never "completes", so the client receives nothing at
// all and whatever sits in front of core eventually times the request out.
//
// The bridge here is an io.Pipe instead. The proxy goroutine writes response
// bytes into the pipe and fasthttp reads them out as the body stream, flushing
// a chunk to the socket per read. Backpressure is the pipe's own: a write blocks
// until fasthttp has taken the previous one, so a slow client slows the cassette
// rather than accumulating in core's heap.
type cassetteResponseWriter struct {
	header http.Header
	writer *io.PipeWriter

	// ready is closed once the status line and headers are settled, which is
	// the moment the Fiber handler can begin its half of the response. A
	// response has exactly one status, so the close is guarded by once —
	// which also makes finish safe to call unconditionally.
	ready  chan struct{}
	once   sync.Once
	status int
}

func newCassetteResponseWriter(writer *io.PipeWriter) *cassetteResponseWriter {
	return &cassetteResponseWriter{
		header: make(http.Header),
		writer: writer,
		ready:  make(chan struct{}),
		status: http.StatusOK,
	}
}

func (w *cassetteResponseWriter) Header() http.Header { return w.header }

func (w *cassetteResponseWriter) WriteHeader(status int) {
	w.once.Do(func() {
		w.status = status
		close(w.ready)
	})
}

// Write settles the headers on first use, because a handler that writes a body
// without naming a status has named 200 — the same rule net/http applies.
func (w *cassetteResponseWriter) Write(body []byte) (int, error) {
	w.WriteHeader(http.StatusOK)

	return w.writer.Write(body)
}

// Flush is what makes httputil.ReverseProxy treat this writer as streamable; a
// writer with no Flush is one it may accumulate into. There is nothing to push
// here — the pipe holds no buffer of its own, so by the time Write returns
// fasthttp already has the bytes — but the method must exist to be seen.
func (w *cassetteResponseWriter) Flush() {}

// finish releases a waiter that would otherwise block forever on a response
// that never produced a status. The proxy's ErrorHandler always writes one, so
// reaching this with ready still open means the handler unwound some other way;
// 502 is the honest answer, since core has nothing from the cassette to relay.
func (w *cassetteResponseWriter) finish() {
	w.WriteHeader(http.StatusBadGateway)
}

// await blocks until the response's status and headers are known.
func (w *cassetteResponseWriter) await() (int, http.Header) {
	<-w.ready

	return w.status, w.header
}

// cassetteBodyStream is the reader fasthttp drains for the response body. Its
// one addition over the pipe itself is the cancel: fasthttp closes the body
// stream when it is finished with the response — read to its end, or abandoned
// by a disconnect a failed write revealed — and that close would otherwise
// stop nothing upstream, because the proxy holding the cassette open answers
// to a context, not to this pipe. Cancelling both is what lets every teardown
// path release the cassette connection and the proxy goroutine with it.
type cassetteBodyStream struct {
	*io.PipeReader
	cancel context.CancelFunc
}

func (s *cassetteBodyStream) Close() error {
	s.cancel()

	return s.PipeReader.Close()
}

// watchClientGone reads a streaming response's connection for the one thing
// the client can still say on it: goodbye. fasthttp pulls body bytes, so while
// a stream is idle nothing in the write path can notice a peer that left — but
// a connection dedicated to an event stream (the caller marks it
// Connection: close for exactly this reason) carries no further request bytes,
// so a Read returning at all means the client is gone or the response is over.
// Either way the cancel is right: it releases an upstream the proxy no longer
// has a reader for, and after a normal completion it lands where the proxy has
// already returned and cancels nothing.
func watchClientGone(conn net.Conn, cancel context.CancelFunc) {
	defer cancel()

	if conn == nil {
		return
	}

	_, _ = conn.Read(make([]byte, 1))
}

// detachRequest copies a Fiber request into a net/http one that stays valid
// after the handler returns.
//
// fasthttpadaptor.ConvertRequest does this conversion and does it faster, but
// it does it by aliasing: the method, the header keys and values and the body
// it produces all point into buffers fasthttp reuses the moment the handler is
// done — its own doc comment says the result must not outlive the handler. That
// is precisely what this request does, because the proxy holding it is still
// streaming a response back. Every field is therefore copied, not referenced.
func detachRequest(ctx context.Context, c *fiber.Ctx) (*http.Request, error) {
	body := make([]byte, len(c.Body()))
	copy(body, c.Body())

	request, err := http.NewRequestWithContext(ctx,
		string(c.Context().Method()), string(c.Context().RequestURI()), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	c.Request().Header.VisitAll(func(key, value []byte) {
		request.Header.Add(string(key), string(value))
	})

	// What SetXForwarded reads to tell the cassette who the client asked and
	// over what, which is the only identity a cassette gets about the origin.
	request.Host = string(c.Context().Host())
	request.RemoteAddr = c.Context().RemoteAddr().String()
	request.TLS = c.Context().TLSConnectionState()

	return request, nil
}
