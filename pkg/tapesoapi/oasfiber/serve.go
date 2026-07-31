package oasfiber

import (
	"context"
	"strings"
	"sync"

	"github.com/gofiber/fiber/v2"

	oas "github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// Server serves a parser's compiled document over HTTP, caching by fingerprint.
//
// Compilation performs no I/O, so serving it on a request path is safe — but it
// is not free, and the document only changes when a fragment is added. The
// cache recompiles on demand and reuses the bytes until something invalidates
// them, which is what lets /openapi stay current as cassettes resolve without
// recompiling per request.
type Server struct {
	parser  *oas.Parser
	options []oas.CompileOption

	mutex    sync.RWMutex
	cached   *oas.CompiledDoc
	cachedAt uint64
	revision uint64
}

// NewServer returns a document server over a parser.
func NewServer(parser *oas.Parser, options ...oas.CompileOption) *Server {
	return &Server{parser: parser, options: options}
}

// Invalidate marks the cached document stale, so the next request recompiles.
//
// A caller that adds fragments after startup — the cassette runner, when a spec
// resolves — calls this rather than the server polling for changes it has no
// way to observe.
func (s *Server) Invalidate() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.revision++
}

// Document returns the current compiled document, recompiling if stale.
func (s *Server) Document(ctx context.Context) (*oas.CompiledDoc, error) {
	s.mutex.RLock()
	if s.cached != nil && s.cachedAt == s.revision {
		document := s.cached
		s.mutex.RUnlock()

		return document, nil
	}
	revision := s.revision
	s.mutex.RUnlock()

	compiled, err := s.parser.Compile(ctx, s.options...)
	if err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	// A concurrent Invalidate during the compile means these bytes are already
	// stale; cache them against the revision they were compiled from so the
	// next request recompiles rather than serving them forever.
	s.cached = compiled
	s.cachedAt = revision

	return compiled, nil
}

// JSON returns a Fiber handler serving the document as JSON with ETag support.
func (s *Server) JSON() fiber.Handler {
	return s.handler(fiber.MIMEApplicationJSON, func(document *oas.CompiledDoc) ([]byte, error) {
		return document.JSON(), nil
	})
}

// YAML returns a Fiber handler serving the document as YAML with ETag support.
func (s *Server) YAML() fiber.Handler {
	return s.handler("application/yaml", func(document *oas.CompiledDoc) ([]byte, error) {
		return document.YAML()
	})
}

func (s *Server) handler(contentType string, encode func(*oas.CompiledDoc) ([]byte, error)) fiber.Handler {
	return func(c *fiber.Ctx) error {
		document, err := s.Document(c.UserContext())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error":   "openapi_compile_failed",
				"message": err.Error(),
			})
		}
		body, err := encode(document)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error":   "openapi_encode_failed",
				"message": err.Error(),
			})
		}

		// The fingerprint covers the rendered document, so a client that
		// revalidates on it re-fetches exactly when the surface it generated
		// from changed. The encoding is part of the tag because JSON and YAML
		// of the same document are different bytes.
		etag := `"` + document.Fingerprint() + "+" + strings.TrimPrefix(contentType, "application/") + `"`
		c.Set(fiber.HeaderETag, etag)
		c.Set(fiber.HeaderContentType, contentType)
		if c.Get(fiber.HeaderIfNoneMatch) == etag {
			return c.SendStatus(fiber.StatusNotModified)
		}

		return c.Send(body)
	}
}

// Mount registers the document routes and, optionally, a reference viewer.
func (s *Server) Mount(app *fiber.App, jsonPath, yamlPath string) {
	if jsonPath != "" {
		app.Get(jsonPath, s.JSON())
	}
	if yamlPath != "" {
		app.Get(yamlPath, s.YAML())
	}
}
