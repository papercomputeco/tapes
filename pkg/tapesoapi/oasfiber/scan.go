package oasfiber

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gofiber/fiber/v2"

	oas "github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// ScanOption configures a Scan.
type ScanOption func(*scanOptions)

type scanOptions struct {
	skipPaths map[string]struct{}
	tags      []string
	report    bool
}

// ScanSkipPaths omits specific OpenAPI paths from the scan.
func ScanSkipPaths(paths ...string) ScanOption {
	return func(o *scanOptions) {
		if o.skipPaths == nil {
			o.skipPaths = map[string]struct{}{}
		}
		for _, p := range paths {
			o.skipPaths[p] = struct{}{}
		}
	}
}

// ScanTags tags every stub the scan contributes.
func ScanTags(tags ...string) ScanOption {
	return func(o *scanOptions) { o.tags = append(o.tags, tags...) }
}

// ScanReportOnly returns what would be stubbed instead of contributing it,
// which is how a test asserts "nothing was registered behind the wrapper's
// back" without changing the document.
func ScanReportOnly() ScanOption {
	return func(o *scanOptions) { o.report = true }
}

// Scan walks an app's route table and stubs anything the parser has not seen.
//
// It exists so adopting this package can be incremental rather than
// all-or-nothing. Third-party Fiber plugins, mounted sub-apps, and handlers
// registered directly on the *fiber.App never pass through [Router], and
// without a reconciliation pass they would be served but undescribed — the
// exact defect the wrapper exists to prevent, reintroduced through the side
// door.
//
// Call it once, after wiring is complete. It returns the paths it stubbed.
func Scan(app *fiber.App, parser *oas.Parser, options ...ScanOption) ([]string, error) {
	resolved := scanOptions{}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}

	described := describedRoutes(parser)

	var stubbed []string
	for _, route := range app.GetRoutes(true) {
		method := strings.ToUpper(route.Method)
		// Fiber registers a HEAD alongside every GET. It is never a separately
		// documented operation, and publishing it would double the surface.
		if method == fiber.MethodHead {
			continue
		}
		openAPIPath, ok := toOpenAPIPath(route.Path)
		if !ok {
			continue
		}
		if _, skip := resolved.skipPaths[openAPIPath]; skip {
			continue
		}
		if _, known := described[method+" "+openAPIPath]; known {
			continue
		}

		stubbed = append(stubbed, method+" "+openAPIPath)
		if resolved.report {
			continue
		}

		doc := Doc(oas.SynthesizeOperationID(method, openAPIPath)).
			Summary("Undocumented operation").
			Description("Registered outside the documented router and stubbed by a reconciliation scan.").
			EmptyResponse(200, "undocumented response")
		if len(resolved.tags) > 0 {
			doc.Tag(resolved.tags...)
		}
		operation := doc.build(nil)
		provenance := oas.Provenance{
			Kind:   oas.KindRoute,
			Name:   method + " " + openAPIPath,
			Detail: "reconciliation scan",
		}
		if err := parser.AddOperation(method, openAPIPath, operation, provenance); err != nil {
			return nil, fmt.Errorf("scan %s %s: %w", method, openAPIPath, err)
		}
	}
	sort.Strings(stubbed)

	return stubbed, nil
}

// describedRoutes indexes what the parser already holds, so a scan stubs only
// the gaps.
func describedRoutes(parser *oas.Parser) map[string]struct{} {
	out := map[string]struct{}{}
	for _, fragment := range parser.Fragments() {
		for path, item := range fragment.Paths {
			for _, method := range item.Methods() {
				out[method+" "+path] = struct{}{}
			}
		}
	}

	return out
}

// toOpenAPIPath converts a Fiber route pattern, reporting not-ok for the
// wildcard routes OpenAPI cannot express.
func toOpenAPIPath(routePath string) (string, bool) {
	router := &Router{}

	return router.openAPIPath(routePath)
}
