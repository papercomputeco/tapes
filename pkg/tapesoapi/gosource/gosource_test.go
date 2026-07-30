package gosource_test

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi/gosource"
)

func TestGoSource(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gosource Suite")
}

// module writes a throwaway module to a temp dir. A fixture on disk rather than
// a string fed to the parser, because the thing under test is the walk: which
// directories are visited, and what import path each one is indexed under.
func module(files map[string]string) string {
	GinkgoHelper()

	root := GinkgoT().TempDir()
	files["go.mod"] = "module example.com/demo\n\ngo 1.26\n"
	for name, content := range files {
		path := filepath.Join(root, name)
		Expect(os.MkdirAll(filepath.Dir(path), 0o755)).To(Succeed())
		Expect(os.WriteFile(path, []byte(content), 0o644)).To(Succeed())
	}

	return root
}

var _ = Describe("Load", func() {
	It("indexes type and field comments under the type's import path", func() {
		root := module(map[string]string{
			"api/types.go": `package api

// Session is one captured harness session.
type Session struct {
	// ID is the session's opaque identifier.
	ID string ` + "`json:\"id\"`" + `

	Undocumented string
}
`,
		})

		docs, err := gosource.Load(root)
		Expect(err).NotTo(HaveOccurred())

		// The key is the PkgPath reflection reports, which is what lets a
		// reflected type find its own comments with no mapping to maintain.
		Expect(docs.TypeDoc("example.com/demo/api", "Session")).
			To(Equal("Session is one captured harness session."))
		Expect(docs.FieldDoc("example.com/demo/api", "Session", "ID")).
			To(Equal("ID is the session's opaque identifier."))
		Expect(docs.FieldDoc("example.com/demo/api", "Session", "Undocumented")).To(BeEmpty())
	})

	It("reads a comment on a grouped type declaration", func() {
		root := module(map[string]string{
			"api/types.go": `package api

type (
	// Grouped is declared inside a parenthesised block.
	Grouped struct{}
)
`,
		})

		docs, err := gosource.Load(root)
		Expect(err).NotTo(HaveOccurred())
		// Both spellings are the same comment to a reader, so both are indexed.
		Expect(docs.TypeDoc("example.com/demo/api", "Grouped")).
			To(Equal("Grouped is declared inside a parenthesised block."))
	})

	It("reads a trailing line comment on a field", func() {
		root := module(map[string]string{
			"api/types.go": `package api

type Row struct {
	Count int ` + "`json:\"count\"`" + ` // Count is how many.
}
`,
		})

		docs, err := gosource.Load(root)
		Expect(err).NotTo(HaveOccurred())
		Expect(docs.FieldDoc("example.com/demo/api", "Row", "Count")).To(Equal("Count is how many."))
	})

	It("excludes tooling directives", func() {
		root := module(map[string]string{
			"api/types.go": `package api

//go:generate stringer -type=Kind

// Kind is a classification.
type Kind struct{}
`,
		})

		docs, err := gosource.Load(root)
		Expect(err).NotTo(HaveOccurred())
		// A directive is an instruction to tooling, not documentation, and
		// publishing it in an API contract tells a client nothing.
		Expect(docs.TypeDoc("example.com/demo/api", "Kind")).To(Equal("Kind is a classification."))
	})

	It("skips test files, dot directories, and named directories", func() {
		root := module(map[string]string{
			"api/types_test.go":   "package api\n\n// FromTest should not be indexed.\ntype FromTest struct{}\n",
			".hidden/types.go":    "package hidden\n\n// FromHidden should not be indexed.\ntype FromHidden struct{}\n",
			"vendor/dep/types.go": "package dep\n\n// FromVendor should not be indexed.\ntype FromVendor struct{}\n",
			"skipme/types.go":     "package skipme\n\n// FromSkipped should not be indexed.\ntype FromSkipped struct{}\n",
		})

		docs, err := gosource.Load(root, gosource.SkipDirs("skipme"))
		Expect(err).NotTo(HaveOccurred())

		Expect(docs.TypeDoc("example.com/demo/api", "FromTest")).To(BeEmpty())
		Expect(docs.TypeDoc("example.com/demo/.hidden", "FromHidden")).To(BeEmpty())
		Expect(docs.TypeDoc("example.com/demo/vendor/dep", "FromVendor")).To(BeEmpty())
		Expect(docs.TypeDoc("example.com/demo/skipme", "FromSkipped")).To(BeEmpty())
	})

	It("does not index a nested module under this module's path", func() {
		root := module(map[string]string{
			"example/go.mod":  "module example.com/nested\n\ngo 1.26\n",
			"example/main.go": "package main\n\n// Nested belongs to another module.\ntype Nested struct{}\n",
		})

		docs, err := gosource.Load(root)
		Expect(err).NotTo(HaveOccurred())
		// A nested module has different import paths; indexing it under this
		// module's would attach the wrong comments to a same-named type.
		Expect(docs.TypeDoc("example.com/demo/example", "Nested")).To(BeEmpty())
	})

	It("survives a file that does not parse", func() {
		root := module(map[string]string{
			"api/good.go":   "package api\n\n// Good is fine.\ntype Good struct{}\n",
			"api/broken.go": "package api\n\nthis is not go\n",
		})

		docs, err := gosource.Load(root)
		// A file that will not parse is a compile error the build reports far
		// better than this scan can; losing its comments is not worth failing
		// contract generation over.
		Expect(err).NotTo(HaveOccurred())
		Expect(docs.TypeDoc("example.com/demo/api", "Good")).To(Equal("Good is fine."))
	})

	It("fails when there is no module to anchor import paths to", func() {
		root := GinkgoT().TempDir()

		_, err := gosource.Load(root)
		Expect(err).To(MatchError(ContainSubstring("go.mod")))
	})
})
