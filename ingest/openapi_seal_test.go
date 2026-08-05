package ingest_test

// Seal gate for the ingest write surface's published contract. The rule and
// its rendering live in internal/openapicheck; this file supplies only what is
// specific to this server — how to compile it and where its seal is kept.
//
// This surface matters more than the read API's for the same reason it is
// harder to notice breaking: its clients are capture adapters in other
// repositories (tapes-extproc, tapesctl, paperd), none of which are built by
// this repo's CI. A field that changes name here compiles fine, passes the
// coverage gate, and surfaces as turns that stop being captured.

import (
	"os"
	"path/filepath"
	"runtime"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/internal/openapicheck"
	"github.com/papercomputeco/tapes/pkg/tapesoapi/gosource"
)

var _ = Describe("OpenAPI contract seal", func() {
	It("matches the fingerprint recorded in ingest/CONTRACT", func(ctx SpecContext) {
		// nil docs is the whole point: this compiles the prose-stripped
		// document, so the seal moves for a route or schema change and stays
		// put for a doc-comment edit.
		compiled, err := ingest.CompileOpenAPI(ctx, nil)
		Expect(err).NotTo(HaveOccurred())

		contents, err := os.ReadFile("CONTRACT")
		Expect(err).NotTo(HaveOccurred(),
			"ingest/CONTRACT is missing; it seals the envelope every capture adapter writes")

		res := openapicheck.CheckSeal("ingest", "ingest/CONTRACT", contents, compiled)
		Expect(res.OK()).To(BeTrue(), res.Explain())
	})

	It("compiles the same fingerprint twice", func(ctx SpecContext) {
		// A fingerprint that varied between runs would make the seal a flake
		// and get it deleted. Map iteration order is the way that happens, and
		// it is not visible from a single compile.
		first, err := ingest.CompileOpenAPI(ctx, nil)
		Expect(err).NotTo(HaveOccurred())
		second, err := ingest.CompileOpenAPI(ctx, nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(second.Fingerprint()).To(Equal(first.Fingerprint()))
	})

	It("seals the stripped document, not the documented one", func(ctx SpecContext) {
		// Guards the plausible wrong fix. `make contracts` and `tapes dev
		// openapi` both default to prose INCLUDED, so someone bumping this
		// seal has a documented fingerprint close at hand; pasting that one
		// would silently retarget the gate at a value that moves whenever a
		// comment is edited. Asserting the two differ keeps that mistake from
		// passing.
		docs, err := gosource.Load(moduleRoot(),
			gosource.SkipDirs("oapi-reference", "build", "migrations"))
		Expect(err).NotTo(HaveOccurred())

		documented, err := ingest.CompileOpenAPI(ctx, docs)
		Expect(err).NotTo(HaveOccurred())
		stripped, err := ingest.CompileOpenAPI(ctx, nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(documented.Fingerprint()).NotTo(Equal(stripped.Fingerprint()),
			"folding in doc comments changed nothing, so this spec is no longer proving "+
				"the seal is the stripped document; check that gosource still reads this module")

		contents, err := os.ReadFile("CONTRACT")
		Expect(err).NotTo(HaveOccurred())
		sealed, err := openapicheck.ParseSeal(contents)
		Expect(err).NotTo(HaveOccurred())

		Expect(sealed).NotTo(Equal(documented.Fingerprint()),
			"ingest/CONTRACT holds the documented fingerprint. It must hold the prose-stripped "+
				"one, or every doc-comment edit becomes a contract change: recompile with "+
				"`tapes dev openapi ingest --docs-root ''` and seal that value instead")
	})
})

// moduleRoot locates the checkout so the doc reader can walk it, without
// depending on the working directory a test binary happens to be run from.
func moduleRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller failed")
	}

	return filepath.Join(filepath.Dir(file), "..")
}
