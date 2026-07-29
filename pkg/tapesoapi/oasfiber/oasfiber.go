// Package oasfiber populates a tapesoapi parser as Fiber routes are
// registered.
//
// The hook point is route *registration*, not request handling. A fiber.Handler
// only runs when a request arrives — too late to describe a surface, and the
// handler does not know its own route pattern anyway. So this package wraps the
// router surface instead: every Get/Post/Group call registers the route on the
// real *fiber.App exactly as Fiber would, and contributes a fragment to the
// parser at the same moment.
//
//	parser := tapesoapi.NewParser(tapesoapi.WithInfo(info))
//	app := fiber.New()
//	api := oasfiber.Wrap(app, parser)
//
//	api.Get("/v1/sessions/:id", s.handleGetSession,
//		oasfiber.Doc("getSession").
//			Summary("Fetch one session").
//			Tag("sessions").
//			JSONResponse(200, "the session", parser.Schema(SessionDetailResponse{})))
//
// The parser is therefore live: Compile at any point describes every route
// registered so far, which is what lets /openapi serve core's own surface
// merged with the cassette documents fetched at runtime.
//
// The core package has no dependency on Fiber. This adapter is the only thing
// that does, and it works through the same Source interface an adapter for any
// other router would.
package oasfiber

import (
	"fmt"
	"path"
	"runtime"
	"strings"

	"github.com/gofiber/fiber/v2"

	oas "github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// Undocumented is what to do about a route registered without a Doc.
type Undocumented int

// The undocumented-route policies.
const (
	// Stub publishes the path and method with a placeholder operation, so the
	// compiled document is complete even where it is uninformative. It is the
	// default because a spec that silently omits served routes is the failure
	// mode this package exists to prevent.
	Stub Undocumented = iota

	// Skip omits the route from the document entirely.
	Skip

	// Fail makes registering an undocumented route a registration error,
	// collected and returned by [Router.Err]. This is how a team enforces
	// "every endpoint is documented".
	Fail
)

// Option configures a Router.
type Option func(*routerOptions)

type routerOptions struct {
	undocumented Undocumented
	prefix       string
	tags         []string
	skipPaths    map[string]struct{}
}

// WithUndocumented sets the policy for routes registered without a Doc.
func WithUndocumented(policy Undocumented) Option {
	return func(o *routerOptions) { o.undocumented = policy }
}

// WithPathPrefix mounts every route this router registers under a prefix in the
// document. Fiber groups already carry their own prefix; this is for the case
// where the app is served behind a path the router does not see.
func WithPathPrefix(prefix string) Option {
	return func(o *routerOptions) { o.prefix = strings.TrimSuffix(prefix, "/") }
}

// WithTags applies tags to every operation this router registers, which a
// per-route Doc can add to.
func WithTags(tags ...string) Option {
	return func(o *routerOptions) { o.tags = append(o.tags, tags...) }
}

// WithSkipPaths omits specific routes from the document by their OpenAPI path.
//
// It is for the endpoints that are served but are not API surface: a metrics
// exposition, a UI, and the handlers that serve the document itself — which
// cannot describe themselves without circularity.
func WithSkipPaths(paths ...string) Option {
	return func(o *routerOptions) {
		if o.skipPaths == nil {
			o.skipPaths = map[string]struct{}{}
		}
		for _, p := range paths {
			o.skipPaths[p] = struct{}{}
		}
	}
}

// Router registers routes on a Fiber app and describes them to a parser.
//
// It mirrors the part of Fiber's router surface an application uses, so
// adopting it is a one-line change at construction rather than a rewrite of
// every registration.
type Router struct {
	app     *fiber.App
	router  fiber.Router
	parser  *oas.Parser
	options routerOptions
	errs    *[]error
}

// Wrap returns a Router that registers on app and describes to parser.
func Wrap(app *fiber.App, parser *oas.Parser, options ...Option) *Router {
	resolved := routerOptions{undocumented: Stub}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	errs := new([]error)

	return &Router{app: app, router: app, parser: parser, options: resolved, errs: errs}
}

// App returns the wrapped Fiber app, for the registrations that do not go
// through this router.
func (r *Router) App() *fiber.App { return r.app }

// Parser returns the parser this router contributes to.
func (r *Router) Parser() *oas.Parser { return r.parser }

// Err returns the registration errors collected so far, joined.
//
// Registration is deliberately non-failing: a route that cannot be described is
// still a route that must be served, and returning an error from Get would put
// error handling on every line of a router setup. The errors are collected here
// instead, for the caller to check once after wiring is complete.
func (r *Router) Err() error {
	if len(*r.errs) == 0 {
		return nil
	}
	messages := make([]string, 0, len(*r.errs))
	for _, err := range *r.errs {
		messages = append(messages, err.Error())
	}

	return fmt.Errorf("openapi route registration:\n  - %s", strings.Join(messages, "\n  - "))
}

// Group returns a Router that registers under a prefix, mirroring
// fiber.App.Group. Groups compose: the prefix carries into the document.
func (r *Router) Group(prefix string, handlers ...fiber.Handler) *Router {
	return &Router{
		app:    r.app,
		router: r.router.Group(prefix, handlers...),
		parser: r.parser,
		errs:   r.errs,
		options: routerOptions{
			undocumented: r.options.undocumented,
			prefix:       r.options.prefix + strings.TrimSuffix(prefix, "/"),
			tags:         append([]string(nil), r.options.tags...),
			skipPaths:    r.options.skipPaths,
		},
	}
}

// Use registers middleware. Middleware describes no operation, so nothing is
// contributed to the parser.
func (r *Router) Use(args ...any) *Router {
	r.router.Use(args...)

	return r
}

// The per-method registrations. Each captures its own caller rather than
// delegating to a shared helper that would capture this file: the whole value of
// the location is that it points at the line a reader has to go edit.

// Get registers a GET route.
func (r *Router) Get(path string, handler fiber.Handler, doc ...*DocBuilder) *Router {
	return r.add(fiber.MethodGet, path, firstDoc(doc), callerLocation(), handler)
}

// Post registers a POST route.
func (r *Router) Post(path string, handler fiber.Handler, doc ...*DocBuilder) *Router {
	return r.add(fiber.MethodPost, path, firstDoc(doc), callerLocation(), handler)
}

// Put registers a PUT route.
func (r *Router) Put(path string, handler fiber.Handler, doc ...*DocBuilder) *Router {
	return r.add(fiber.MethodPut, path, firstDoc(doc), callerLocation(), handler)
}

// Patch registers a PATCH route.
func (r *Router) Patch(path string, handler fiber.Handler, doc ...*DocBuilder) *Router {
	return r.add(fiber.MethodPatch, path, firstDoc(doc), callerLocation(), handler)
}

// Delete registers a DELETE route.
func (r *Router) Delete(path string, handler fiber.Handler, doc ...*DocBuilder) *Router {
	return r.add(fiber.MethodDelete, path, firstDoc(doc), callerLocation(), handler)
}

// Head registers a HEAD route.
func (r *Router) Head(path string, handler fiber.Handler, doc ...*DocBuilder) *Router {
	return r.add(fiber.MethodHead, path, firstDoc(doc), callerLocation(), handler)
}

// Options registers an OPTIONS route.
func (r *Router) Options(path string, handler fiber.Handler, doc ...*DocBuilder) *Router {
	return r.add(fiber.MethodOptions, path, firstDoc(doc), callerLocation(), handler)
}

// All registers a handler for every method Fiber knows.
//
// The document gets only the methods the docs describe, because app.All mounts
// verbs the handler may not implement, and publishing those would hand a client
// generator operations that 405.
func (r *Router) All(path string, handler fiber.Handler, docs ...*DocBuilder) *Router {
	location := callerLocation()
	r.router.All(path, handler)
	for _, doc := range docs {
		if doc == nil {
			continue
		}
		r.describe(doc.method, path, doc, location)
	}

	return r
}

// Add registers one route with an explicit doc, for the callers that need more
// than one handler on a route.
func (r *Router) Add(method, path string, doc *DocBuilder, handlers ...fiber.Handler) *Router {
	return r.add(method, path, doc, callerLocation(), handlers...)
}

// add is the one place a route is both registered and described.
func (r *Router) add(
	method, path string, doc *DocBuilder, location string, handlers ...fiber.Handler,
) *Router {
	r.router.Add(method, path, handlers...)
	r.describe(method, path, doc, location)

	return r
}

// Static mounts a static file handler. Static assets are not API surface, so
// nothing is contributed.
func (r *Router) Static(prefix, root string, config ...fiber.Static) *Router {
	r.router.Static(prefix, root, config...)

	return r
}

func firstDoc(docs []*DocBuilder) *DocBuilder {
	for _, doc := range docs {
		if doc != nil {
			return doc
		}
	}

	return nil
}

// describe contributes the fragment for one registered route.
func (r *Router) describe(method, routePath string, doc *DocBuilder, location string) {
	openAPIPath, ok := r.openAPIPath(routePath)
	if !ok {
		// A wildcard route has no OpenAPI path; the operations reachable
		// through it are published by whatever serves them.
		return
	}
	if _, skip := r.options.skipPaths[openAPIPath]; skip {
		return
	}

	provenance := oas.Provenance{
		Kind:   oas.KindRoute,
		Name:   strings.ToUpper(method) + " " + openAPIPath,
		Detail: location,
	}

	if doc == nil {
		switch r.options.undocumented {
		case Skip:
			return
		case Fail:
			r.fail(fmt.Errorf("%s is registered without a Doc (%s)", provenance.Name, location))

			return
		case Stub:
		default:
			// An unrecognized setting stubs, like the zero value: a route that is
			// served should appear in the contract, however thinly.
		}
		doc = Doc(oas.SynthesizeOperationID(method, openAPIPath)).
			Summary("Undocumented operation").
			EmptyResponse(200, "undocumented response")
	}

	operation := doc.build(r.options.tags)
	if err := r.parser.AddOperation(method, openAPIPath, operation, provenance); err != nil {
		r.fail(fmt.Errorf("%s: %w", provenance.Name, err))
	}
}

func (r *Router) fail(err error) { *r.errs = append(*r.errs, err) }

// openAPIPath converts a Fiber route pattern into an OpenAPI path.
//
// Fiber's `:id` becomes `{id}`. A wildcard (`*` or `+`) has no OpenAPI
// equivalent — the spec has no way to say "any suffix" — so those routes report
// not-ok and are left out rather than published under a path no client can
// construct.
func (r *Router) openAPIPath(routePath string) (string, bool) {
	joined := routePath
	if r.options.prefix != "" {
		joined = path.Join(r.options.prefix, routePath)
	}
	segments := strings.Split(joined, "/")
	for i, segment := range segments {
		switch {
		case strings.HasPrefix(segment, "*"), strings.HasPrefix(segment, "+"):
			return "", false
		case strings.HasPrefix(segment, ":"):
			name := strings.TrimPrefix(segment, ":")
			// Fiber marks an optional param with a trailing `?`. OpenAPI path
			// parameters are always required, so the optional form is published
			// as the present-parameter route; the absent form is a different
			// path that Fiber matches and the document does not describe.
			name = strings.TrimSuffix(name, "?")
			// A param constraint (`:id<int>`) is routing detail, not identity.
			if idx := strings.IndexByte(name, '<'); idx >= 0 {
				name = name[:idx]
			}
			segments[i] = "{" + name + "}"
		}
	}
	out := strings.Join(segments, "/")
	if out != "/" {
		out = strings.TrimSuffix(out, "/")
	}
	if out == "" {
		out = "/"
	}

	return out, true
}

// callerLocation captures the file:line of the registration call, so a conflict
// error can name the line to go edit.
//
// The skip is fixed at 2 — this frame, the Get/Post/Add method, then the
// application line that called it — which is why every caller is one of those
// methods and none of them passes a depth. A helper that forwarded on its
// caller's behalf would report itself, and that is the one location nobody needs.
func callerLocation() string {
	const skipToRegistrationSite = 2

	_, file, line, ok := runtime.Caller(skipToRegistrationSite)
	if !ok {
		return ""
	}

	return fmt.Sprintf("%s:%d", trimToModule(file), line)
}

// trimToModule shortens an absolute build path to a repo-relative one, so the
// location a compiled contract reports does not leak the machine that built it.
func trimToModule(file string) string {
	if idx := strings.LastIndex(file, "/tapes/"); idx >= 0 {
		return file[idx+len("/tapes/"):]
	}

	return path.Base(path.Dir(file)) + "/" + path.Base(file)
}

// The id a stub gets comes from oas.SynthesizeOperationID rather than from a
// rule of this adapter's own. An aggregate names anonymous ingested operations
// with the same function, and two rules would let one route be called one thing
// in core's contract and another in an aggregate that republishes it.
