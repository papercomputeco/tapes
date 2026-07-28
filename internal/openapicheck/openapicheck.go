// Package openapicheck compares a server's registered routes against the
// OpenAPI contract it publishes.
//
// tapes publishes two contracts (the read API and the ingest write surface) and
// may publish more. The rule is the same for each — everything served is
// described, nothing described is unserved — so the rule lives here once rather
// than as a copy per server that can drift into disagreeing about what counts
// as covered.
//
// This exists because an entire surface once shipped with no annotations and
// nothing noticed: `tapes dev check-openapi` validates field types on one
// endpoint's payload and says so explicitly, leaving route coverage unchecked.
package openapicheck

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gofiber/fiber/v2"
	"sigs.k8s.io/yaml"
)

// Route is one registered (method, path) pair, with the path in OpenAPI form.
type Route struct {
	Method string
	Path   string
}

// Exemptions carves out the routes a contract legitimately omits. Every entry
// carries a reason: an unexplained exemption is indistinguishable from an
// oversight the next reader will preserve out of caution.
type Exemptions struct {
	// Undocumented are paths deliberately absent from the contract — infra
	// endpoints, a UI, the spec-serving routes themselves.
	Undocumented map[string]string

	// AllMounted are paths registered with fiber's app.All, which mounts every
	// verb fiber knows including ones the handler does not implement. For
	// these, the contract need only describe the path; requiring every mounted
	// verb would publish operations that do not work.
	AllMounted map[string]string

	// Conditional are paths mounted only under some configurations. Their
	// absence from a default server is expected and is not staleness.
	Conditional map[string]string
}

// Result lists what disagrees. All three empty means the contract and the
// server describe the same surface.
type Result struct {
	// MissingFromSpec is served but undocumented — a client generated from the
	// contract cannot reach it.
	MissingFromSpec []string

	// PhantomInSpec is documented but unserved — a generated client calls it
	// and gets a 404. The same defect pointed the other way.
	PhantomInSpec []string

	// StaleExemptions name routes that no longer exist. Left in place they
	// become cover for the next real gap on that path.
	StaleExemptions []string
}

// OK reports whether the contract and the server agree.
func (r Result) OK() bool {
	return len(r.MissingFromSpec) == 0 && len(r.PhantomInSpec) == 0 && len(r.StaleExemptions) == 0
}

// FromFiberRoutes converts a fiber route table into Routes, normalising `:id`
// params to OpenAPI `{id}` and dropping the HEAD entries fiber registers
// alongside every GET (never a separately documented operation).
func FromFiberRoutes(app *fiber.App) []Route {
	var out []Route
	for _, r := range app.GetRoutes(true) {
		method := strings.ToUpper(r.Method)
		if method == "HEAD" {
			continue
		}
		out = append(out, Route{Method: method, Path: ToOpenAPIPath(r.Path)})
	}
	return out
}

// ToOpenAPIPath rewrites fiber's `:id` path params as OpenAPI's `{id}`.
func ToOpenAPIPath(p string) string {
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

// specPaths is the slice of an OpenAPI document this check needs.
type specPaths struct {
	Paths map[string]map[string]any `json:"paths"`
}

// Check compares routes against the contract in specYAML.
func Check(routes []Route, specYAML []byte, ex Exemptions) (Result, error) {
	var doc specPaths
	if err := yaml.Unmarshal(specYAML, &doc); err != nil {
		return Result{}, fmt.Errorf("parse openapi document: %w", err)
	}

	documented := map[string]map[string]bool{}
	for path, ops := range doc.Paths {
		documented[path] = map[string]bool{}
		for method := range ops {
			documented[path][strings.ToUpper(method)] = true
		}
	}

	registered := map[string]map[string]bool{}
	for _, r := range routes {
		if registered[r.Path] == nil {
			registered[r.Path] = map[string]bool{}
		}
		registered[r.Path][r.Method] = true
	}

	var res Result

	for path, methods := range registered {
		if _, exempt := ex.Undocumented[path]; exempt {
			continue
		}
		if _, isAll := ex.AllMounted[path]; isAll {
			if len(documented[path]) == 0 {
				res.MissingFromSpec = append(res.MissingFromSpec, "(any method) "+path)
			}
			continue
		}
		for method := range methods {
			if !documented[path][method] {
				res.MissingFromSpec = append(res.MissingFromSpec, method+" "+path)
			}
		}
	}

	for path, methods := range documented {
		if _, isAll := ex.AllMounted[path]; isAll {
			continue
		}
		for method := range methods {
			if !registered[path][method] {
				res.PhantomInSpec = append(res.PhantomInSpec, method+" "+path)
			}
		}
	}

	for path := range ex.Undocumented {
		if _, conditional := ex.Conditional[path]; conditional {
			continue
		}
		if registered[path] == nil {
			res.StaleExemptions = append(res.StaleExemptions, "Undocumented: "+path)
		}
	}
	for path := range ex.AllMounted {
		if registered[path] == nil {
			res.StaleExemptions = append(res.StaleExemptions, "AllMounted: "+path)
		}
	}

	sort.Strings(res.MissingFromSpec)
	sort.Strings(res.PhantomInSpec)
	sort.Strings(res.StaleExemptions)
	return res, nil
}

// Explain renders a Result as operator-facing guidance. Empty when everything
// agrees.
func (r Result) Explain(specPath string) string {
	if r.OK() {
		return ""
	}
	var b strings.Builder
	if len(r.MissingFromSpec) > 0 {
		fmt.Fprintf(&b, "\nServed but absent from %s:\n", specPath)
		for _, s := range r.MissingFromSpec {
			fmt.Fprintf(&b, "  - %s\n", s)
		}
		b.WriteString("Annotate the handler (swag) and run `make openapi`.\n" +
			"If it is deliberately unpublished, add it to Exemptions.Undocumented with the reason.\n")
	}
	if len(r.PhantomInSpec) > 0 {
		fmt.Fprintf(&b, "\nDescribed in %s but not served — a generated client would 404:\n", specPath)
		for _, s := range r.PhantomInSpec {
			fmt.Fprintf(&b, "  - %s\n", s)
		}
	}
	if len(r.StaleExemptions) > 0 {
		b.WriteString("\nExemptions naming routes that no longer exist; remove them:\n")
		for _, s := range r.StaleExemptions {
			fmt.Fprintf(&b, "  - %s\n", s)
		}
	}
	return b.String()
}
