package oasfiber_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	oas "github.com/papercomputeco/tapes/pkg/tapesoapi"
	"github.com/papercomputeco/tapes/pkg/tapesoapi/oasfiber"
)

func TestOASFiber(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "oasfiber Suite")
}

func noop(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusNoContent) }

var _ = Describe("Wrap", func() {
	var (
		app    *fiber.App
		parser *oas.Parser
		router *oasfiber.Router
	)

	BeforeEach(func() {
		app = fiber.New(fiber.Config{DisableStartupMessage: true})
		parser = oas.NewParser(oas.WithInfo(oas.Info{Title: "Test", Version: "1.0.0"}))
		router = oasfiber.Wrap(app, parser)
	})

	compile := func(options ...oas.CompileOption) map[string]any {
		GinkgoHelper()
		compiled, err := parser.Compile(context.Background(), options...)
		Expect(err).NotTo(HaveOccurred())

		var tree map[string]any
		Expect(json.Unmarshal(compiled.JSON(), &tree)).To(Succeed())

		return tree
	}

	paths := func(tree map[string]any) map[string]any {
		GinkgoHelper()
		out, ok := tree["paths"].(map[string]any)
		Expect(ok).To(BeTrue())

		return out
	}

	It("registers the route on the app and describes it in one call", func() {
		router.Get("/v1/things", noop, oasfiber.Doc("listThings").Summary("List things"))
		Expect(router.Err()).NotTo(HaveOccurred())

		// The route is really registered: this is a wrapper, not a substitute.
		response, err := app.Test(httpGet("/v1/things"), -1)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode).To(Equal(fiber.StatusNoContent))

		// And described, from the same call.
		Expect(paths(compile(oas.WithLint()))).To(HaveKey("/v1/things"))
	})

	It("translates fiber path params into OpenAPI templates", func() {
		router.Get("/v1/traces/:trace_id/spans/:span_id", noop, oasfiber.Doc("getSpan"))
		Expect(router.Err()).NotTo(HaveOccurred())

		tree := compile(oas.WithLint())
		Expect(paths(tree)).To(HaveKey("/v1/traces/{trace_id}/spans/{span_id}"))

		// Every template parameter must be described for the document to
		// validate, and the route pattern already proves each one exists.
		operation := paths(tree)["/v1/traces/{trace_id}/spans/{span_id}"].(map[string]any)["get"].(map[string]any)
		declared := operation["parameters"].([]any)
		names := make([]string, 0, len(declared))
		for _, parameter := range declared {
			names = append(names, parameter.(map[string]any)["name"].(string))
		}
		Expect(names).To(ConsistOf("trace_id", "span_id"))
	})

	It("carries a group's prefix into the document", func() {
		admin := router.Group("/v1/admin")
		admin.Post("/rotate", noop, oasfiber.Doc("rotate"))
		Expect(router.Err()).NotTo(HaveOccurred())

		Expect(paths(compile(oas.WithLint()))).To(HaveKey("/v1/admin/rotate"))
	})

	It("describes only the verbs an All mount actually implements", func() {
		router.All("/v1/mcp", noop,
			oasfiber.DocFor("POST", "invokeMcp"),
			oasfiber.DocFor("GET", "openMcpStream"))
		Expect(router.Err()).NotTo(HaveOccurred())

		// app.All mounts every verb fiber knows, including ones the handler
		// does not implement. Publishing those would hand a generated client
		// operations that cannot work.
		item := paths(compile(oas.WithLint()))["/v1/mcp"].(map[string]any)
		Expect(item).To(HaveKey("post"))
		Expect(item).To(HaveKey("get"))
		Expect(item).NotTo(HaveKey("delete"))
	})

	It("leaves a wildcard route out of the document", func() {
		router.All("/v1/cassettes/:name/*", noop, oasfiber.DocFor("GET", "proxy"))
		Expect(router.Err()).NotTo(HaveOccurred())

		// OpenAPI has no way to say "any suffix", so publishing the mount would
		// mean publishing a path no client can construct.
		Expect(paths(compile(oas.WithLint()))).To(BeEmpty())
	})

	Describe("the undocumented policy", func() {
		It("stubs an undocumented route by default", func() {
			router.Get("/v1/bare", noop)
			Expect(router.Err()).NotTo(HaveOccurred())

			// A spec that silently omits served routes is the failure this
			// package exists to prevent, so the default is uninformative rather
			// than absent.
			Expect(paths(compile())).To(HaveKey("/v1/bare"))
		})

		It("omits one when told to skip", func() {
			skipping := oasfiber.Wrap(app, parser, oasfiber.WithUndocumented(oasfiber.Skip))
			skipping.Get("/v1/internal", noop)

			Expect(paths(compile(oas.WithLint()))).NotTo(HaveKey("/v1/internal"))
		})

		It("reports one as a registration error when told to fail", func() {
			failing := oasfiber.Wrap(app, parser, oasfiber.WithUndocumented(oasfiber.Fail))
			failing.Get("/v1/undocumented", noop)

			// This is how "every endpoint is documented" stops being a
			// convention someone has to remember.
			Expect(failing.Err()).To(MatchError(ContainSubstring("registered without a Doc")))
			// The error names the registration site, not just the route.
			Expect(failing.Err().Error()).To(ContainSubstring("oasfiber_test.go:"))
		})
	})

	It("omits the paths it is told to skip", func() {
		skipping := oasfiber.Wrap(app, parser,
			oasfiber.WithSkipPaths("/metrics"))
		skipping.Get("/metrics", noop, oasfiber.Doc("metrics"))
		skipping.Get("/v1/real", noop, oasfiber.Doc("real"))

		tree := paths(compile(oas.WithLint()))
		// A handler that serves the document cannot describe itself without
		// circularity, and a metrics exposition is not client surface.
		Expect(tree).NotTo(HaveKey("/metrics"))
		Expect(tree).To(HaveKey("/v1/real"))
	})

	It("applies router-level tags to every operation it registers", func() {
		tagged := oasfiber.Wrap(app, parser, oasfiber.WithTags("v1"))
		tagged.Get("/v1/tagged", noop, oasfiber.Doc("tagged").Tag("things"))

		operation := paths(compile(oas.WithLint()))["/v1/tagged"].(map[string]any)["get"].(map[string]any)
		Expect(operation["tags"]).To(ConsistOf("things", "v1"))
	})

	It("names the registration site in a conflict", func() {
		router.Get("/v1/dup", noop, oasfiber.Doc("first"))
		router.Get("/v1/dup", noop, oasfiber.Doc("second"))

		_, err := parser.Compile(context.Background())
		// Provenance is what turns "this path is defined twice" from a puzzle
		// into a fix.
		Expect(err).To(MatchError(ContainSubstring("oasfiber_test.go:")))
	})
})

var _ = Describe("Scan", func() {
	It("stubs a route registered behind the wrapper's back", func() {
		app := fiber.New(fiber.Config{DisableStartupMessage: true})
		parser := oas.NewParser(oas.WithInfo(oas.Info{Title: "Test", Version: "1.0.0"}))

		router := oasfiber.Wrap(app, parser)
		router.Get("/v1/described", noop,
			oasfiber.Doc("described").EmptyResponse(204, "no content"))

		// A third-party plugin, a mounted sub-app, or a handler registered
		// straight on the *fiber.App never passes through the wrapper — which
		// would reintroduce the undescribed-route defect through the side door.
		app.Get("/v1/undescribed", noop)

		stubbed, err := oasfiber.Scan(app, parser)
		Expect(err).NotTo(HaveOccurred())
		Expect(stubbed).To(ConsistOf("GET /v1/undescribed"))

		compiled, err := parser.Compile(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(compiled.Paths()).To(ContainElements("/v1/described", "/v1/undescribed"))
	})

	It("can report without contributing", func() {
		app := fiber.New(fiber.Config{DisableStartupMessage: true})
		parser := oas.NewParser(oas.WithInfo(oas.Info{Title: "Test", Version: "1.0.0"}))
		app.Get("/v1/undescribed", noop)

		stubbed, err := oasfiber.Scan(app, parser, oasfiber.ScanReportOnly())
		Expect(err).NotTo(HaveOccurred())
		Expect(stubbed).To(ConsistOf("GET /v1/undescribed"))

		// Report-only is how a test asserts "nothing was registered behind the
		// wrapper's back" without changing the document it is asserting about.
		Expect(parser.Fragments()).To(BeEmpty())
	})

	It("does not stub the HEAD fiber registers alongside every GET", func() {
		app := fiber.New(fiber.Config{DisableStartupMessage: true})
		parser := oas.NewParser()
		app.Get("/v1/thing", noop)

		stubbed, err := oasfiber.Scan(app, parser)
		Expect(err).NotTo(HaveOccurred())
		Expect(stubbed).To(ConsistOf("GET /v1/thing"))
	})
})

var _ = Describe("Server", func() {
	It("serves the compiled document and revalidates on its fingerprint", func() {
		app := fiber.New(fiber.Config{DisableStartupMessage: true})
		parser := oas.NewParser(oas.WithInfo(oas.Info{Title: "Test", Version: "1.0.0"}))
		router := oasfiber.Wrap(app, parser)
		router.Get("/v1/thing", noop, oasfiber.Doc("thing"))

		documents := oasfiber.NewServer(parser, oas.WithLint())
		documents.Mount(app, "/openapi.json", "/openapi.yaml")

		response, err := app.Test(httpGet("/openapi.json"), -1)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.StatusCode).To(Equal(http.StatusOK))
		body, err := io.ReadAll(response.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(body).To(ContainSubstring(`"/v1/thing"`))

		etag := response.Header.Get(fiber.HeaderETag)
		Expect(etag).NotTo(BeEmpty())

		// The fingerprint covers the rendered document, so a client that
		// revalidates on it re-fetches exactly when its surface changed.
		conditional := httpGet("/openapi.json")
		conditional.Header.Set(fiber.HeaderIfNoneMatch, etag)
		cached, err := app.Test(conditional, -1)
		Expect(err).NotTo(HaveOccurred())
		Expect(cached.StatusCode).To(Equal(http.StatusNotModified))
	})

	It("recompiles after the surface is invalidated", func() {
		parser := oas.NewParser(oas.WithInfo(oas.Info{Title: "Test", Version: "1.0.0"}))
		documents := oasfiber.NewServer(parser, oas.WithLint())

		first, err := documents.Document(context.Background())
		Expect(err).NotTo(HaveOccurred())

		Expect(parser.AddOperation("GET", "/late",
			oas.NewOperation("late").EmptyResponse(204, "no content").Build(),
			oas.Provenance{Kind: oas.KindRoute, Name: "GET /late"})).To(Succeed())

		// Nothing lets the cache observe a fragment being added, so the caller
		// that added it says so.
		documents.Invalidate()

		second, err := documents.Document(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(second.Fingerprint()).NotTo(Equal(first.Fingerprint()))
	})
})

func httpGet(target string) *http.Request {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, target, nil)
	if err != nil {
		panic(err)
	}

	return request
}
