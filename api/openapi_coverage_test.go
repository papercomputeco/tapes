package api

// Coverage gate: every route the server registers must appear in the published
// OpenAPI contract, and the contract must not describe routes that don't exist.
//
// This exists because an entire surface (the skills endpoints) shipped without
// a single swag annotation and nothing noticed. `tapes dev check-openapi` does
// not catch it — that validates the *types* of fields on one endpoint's served
// payload and says so explicitly ("a contract type check, not a structural
// completeness check"). Route coverage was checked by nobody.
//
// The route list comes from fiber's own table rather than from grepping the
// registration source, so a route added through any path — a helper, a loop, a
// future group — is still seen here.

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/yaml"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// undocumentedRoutes are paths deliberately absent from the contract, each with
// the reason. Adding an entry is a decision to leave something unpublished, not
// a way to silence this test — anything a client would call belongs in the
// contract instead.
var undocumentedRoutes = map[string]string{
	"/":                     "optional web UI; HTML, not an API surface",
	"/metrics":              "Prometheus exposition — scraped by convention, not called by clients",
	"/swagger":              "the spec viewer itself",
	"/swagger/doc.json":     "serves the spec; describing it in the spec is circular",
	"/swagger/openapi.yaml": "serves the spec; describing it in the spec is circular",
}

// conditionalRoutes are registered only under some configurations, so their
// absence from a default server is expected and says nothing about staleness.
// They are still covered by the checks above whenever they ARE mounted.
var conditionalRoutes = map[string]string{
	"/": "mounted only when Config.EnableWebUI is set",
}

// allRegisteredPaths are mounted with app.All, which registers every HTTP verb
// fiber knows. Only the verbs the handler actually implements are documented,
// so the extra registrations are not contract gaps. Listed explicitly so a NEW
// app.All mount cannot inherit the exemption silently.
var allRegisteredPaths = map[string]string{
	"/v1/mcp": "app.All mount; the MCP transport implements GET, POST, and DELETE",
}

// openAPIPaths is the parsed `paths` section of the embedded contract.
type openAPIPaths struct {
	Paths map[string]map[string]any `json:"paths"`
}

// fiberToOpenAPIPath converts fiber's `:id` params to OpenAPI's `{id}`.
func fiberToOpenAPIPath(p string) string {
	segs := strings.Split(p, "/")
	for i, seg := range segs {
		if name, ok := strings.CutPrefix(seg, ":"); ok {
			segs[i] = "{" + name + "}"
		}
	}
	out := strings.Join(segs, "/")
	if out != "/" {
		out = strings.TrimSuffix(out, "/")
	}
	return out
}

var _ = Describe("OpenAPI route coverage", func() {
	var (
		registered map[string]map[string]bool // path -> method set
		documented map[string]map[string]bool
	)

	BeforeEach(func() {
		server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		registered = map[string]map[string]bool{}
		for _, r := range server.app.GetRoutes(true) {
			method := strings.ToUpper(r.Method)
			// fiber auto-registers HEAD alongside every GET; it is never a
			// separately documented operation.
			if method == "HEAD" {
				continue
			}
			p := fiberToOpenAPIPath(r.Path)
			if registered[p] == nil {
				registered[p] = map[string]bool{}
			}
			registered[p][method] = true
		}

		var spec openAPIPaths
		Expect(yaml.Unmarshal(OpenAPISpec(), &spec)).To(Succeed())
		documented = map[string]map[string]bool{}
		for p, ops := range spec.Paths {
			documented[p] = map[string]bool{}
			for method := range ops {
				documented[p][strings.ToUpper(method)] = true
			}
		}
	})

	It("documents every registered route", func() {
		var missing []string
		for path, methods := range registered {
			if _, exempt := undocumentedRoutes[path]; exempt {
				continue
			}
			if _, isAll := allRegisteredPaths[path]; isAll {
				// Only require the path to be described; see allRegisteredPaths.
				if len(documented[path]) == 0 {
					missing = append(missing, "(any) "+path)
				}
				continue
			}
			for method := range methods {
				if !documented[path][method] {
					missing = append(missing, method+" "+path)
				}
			}
		}
		Expect(missing).To(BeEmpty(),
			"routes are served but absent from api/openapi.yaml.\n"+
				"Add swag annotations to the handler and run `make openapi`.\n"+
				"If a route is deliberately unpublished, add it to undocumentedRoutes with the reason.\n"+
				"Missing: %v", missing)
	})

	It("does not document routes that are not served", func() {
		var phantom []string
		for path, methods := range documented {
			for method := range methods {
				if registered[path] == nil {
					phantom = append(phantom, method+" "+path)
					continue
				}
				if _, isAll := allRegisteredPaths[path]; isAll {
					continue
				}
				if !registered[path][method] {
					phantom = append(phantom, method+" "+path)
				}
			}
		}
		Expect(phantom).To(BeEmpty(),
			"api/openapi.yaml describes operations the server does not serve — "+
				"a client generated from it would call routes that 404.\nPhantom: %v", phantom)
	})

	It("keeps the exemption lists honest", func() {
		// An exemption for a route that no longer exists is stale and hides the
		// next real gap on that path.
		for path := range undocumentedRoutes {
			if _, conditional := conditionalRoutes[path]; conditional {
				continue
			}
			Expect(registered).To(HaveKey(path),
				"undocumentedRoutes names %q, which is no longer registered; remove the entry", path)
		}
		for path := range allRegisteredPaths {
			Expect(registered).To(HaveKey(path),
				"allRegisteredPaths names %q, which is no longer registered; remove the entry", path)
		}
	})
})
