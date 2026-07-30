package tapesoapi_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
	v30 "github.com/papercomputeco/tapes/pkg/tapesoapi/v3.0"
)

var _ = Describe("merging", func() {
	// Both fixtures define `GET /items` and `schemas/Item` differently, and
	// `schemas/Shared` identically. That combination is the whole test surface:
	// a merge has to conflict on the first two and stay quiet about the third.
	twoDocuments := func(policy tapesoapi.ConflictPolicy, options ...tapesoapi.DocOption) *tapesoapi.Parser {
		GinkgoHelper()

		parser := tapesoapi.NewParser(
			tapesoapi.WithInfo(tapesoapi.Info{Title: "Aggregate", Version: "1.0.0"}),
			tapesoapi.WithConflictPolicy(policy),
		)
		add := func(fixture string, name string) {
			Expect(parser.AddDocument(ctx(), v30.Read(fixture),
				append([]tapesoapi.DocOption{
					tapesoapi.WithProvenance(tapesoapi.Provenance{
						Kind: tapesoapi.KindDocument, Name: name,
					}),
					tapesoapi.WithoutInfo(),
				}, options...)...)).To(Succeed())
		}
		add(v30.ConflictLeft, "left.yaml")
		add(v30.ConflictRight, "right.yaml")

		return parser
	}

	Describe("the default policy", func() {
		It("reports every conflict at once, naming both sources", func() {
			_, err := twoDocuments(tapesoapi.PolicyError).Compile(ctx())

			var conflict *tapesoapi.ConflictError
			Expect(errors.As(err, &conflict)).To(BeTrue(), "got %v", err)

			// Collect-all rather than fail-fast: someone aggregating a fleet
			// wants the whole list so they can fix it in one pass.
			Expect(conflict.Conflicts).To(HaveLen(2))

			kinds := map[string]string{}
			for _, entry := range conflict.Conflicts {
				kinds[entry.Kind] = entry.Key
			}
			Expect(kinds).To(HaveKeyWithValue("path", "GET /items"))
			Expect(kinds).To(HaveKeyWithValue("component", "schemas/Item"))

			// Provenance is load-bearing, not logging: an error that does not
			// name the two places to go look is a puzzle rather than a fix.
			Expect(err.Error()).To(ContainSubstring("left.yaml"))
			Expect(err.Error()).To(ContainSubstring("right.yaml"))
		})

		It("does not conflict on a component both documents define identically", func() {
			_, err := twoDocuments(tapesoapi.PolicyError).Compile(ctx())

			var conflict *tapesoapi.ConflictError
			Expect(errors.As(err, &conflict)).To(BeTrue())
			for _, entry := range conflict.Conflicts {
				// Two packages registering an identical type is the normal
				// case, and calling it a conflict would make shared types
				// unusable across documents.
				Expect(entry.Key).NotTo(Equal("schemas/Shared"))
			}
		})
	})

	DescribeTable("a picking policy keeps one side and says so",
		func(policy tapesoapi.ConflictPolicy, wantOperationID string) {
			parser := twoDocuments(policy)

			compiled, err := parser.Compile(ctx(), tapesoapi.WithLint())
			Expect(err).NotTo(HaveOccurred())

			tree := compileTree(parser, tapesoapi.WithLint())
			Expect(at(tree, "paths", "/items", "get", "operationId")).To(Equal(wantOperationID))

			// Nothing is resolved silently. A picked winner that left no trace
			// would make an aggregate quietly disagree with its inputs.
			Expect(compiled.Warnings()).NotTo(BeEmpty())
			Expect(compiled.Warnings()).To(ContainElement(ContainSubstring("GET /items")))
		},
		// left.yaml sorts before right.yaml by provenance, so "first" and
		// "last" are stated in merge order, not in the order they were added.
		Entry("first wins", tapesoapi.PolicyFirstWins, "listItemsLeft"),
		Entry("last wins", tapesoapi.PolicyLastWins, "listItemsRight"),
	)

	It("keeps both sides when a namespace pre-empts the component collision", func() {
		parser := tapesoapi.NewParser(
			tapesoapi.WithInfo(tapesoapi.Info{Title: "Aggregate", Version: "1.0.0"}))

		for _, entry := range []struct{ fixture, namespace, prefix string }{
			{v30.ConflictLeft, "left_", "/left"},
			{v30.ConflictRight, "right_", "/right"},
		} {
			Expect(parser.AddDocument(ctx(), v30.Read(entry.fixture),
				tapesoapi.WithProvenance(tapesoapi.Provenance{
					Kind: tapesoapi.KindDocument, Name: entry.namespace,
				}),
				tapesoapi.WithoutInfo(),
				tapesoapi.WithComponentNamespace(entry.namespace),
				tapesoapi.WithPathPrefix(entry.prefix),
			)).To(Succeed())
		}

		// Namespacing and prefixing pre-empt collisions rather than resolving
		// them — which is the only outcome where nothing is lost.
		tree := compileTree(parser)
		Expect(object(tree, "paths")).To(HaveKeys("/left/items", "/right/items"))
		Expect(object(tree, "components", "schemas")).To(HaveKeys("left_Item", "right_Item"))
	})

	It("merges different methods on one path without complaint", func() {
		parser := tapesoapi.NewParser(
			tapesoapi.WithInfo(tapesoapi.Info{Title: "Aggregate", Version: "1.0.0"}))

		// This is the normal case for a router: GET and DELETE on one path
		// arrive as two separate registrations from two separate call sites.
		for _, entry := range []struct{ method, id string }{
			{"GET", "getThing"}, {"DELETE", "deleteThing"},
		} {
			Expect(parser.AddOperation(entry.method, "/things/{id}",
				tapesoapi.NewOperation(entry.id).EmptyResponse(204, "no content").Build(),
				tapesoapi.Provenance{
					Kind: tapesoapi.KindRoute, Name: entry.method + " /things/{id}",
				})).To(Succeed())
		}

		item := object(compileTree(parser), "paths", "/things/{id}")
		Expect(item).To(HaveKeys("get", "delete"))
	})

	Describe("document metadata", func() {
		It("unions servers and tags with stable ordering", func() {
			parser := tapesoapi.NewParser(
				tapesoapi.WithInfo(tapesoapi.Info{Title: "Aggregate", Version: "1.0.0"}),
				tapesoapi.WithServer("https://api.example.com", "production"))
			Expect(parser.AddDocument(ctx(), v30.Read(v30.Petstore),
				tapesoapi.WithoutInfo())).To(Succeed())

			tree := compileTree(parser)
			servers, ok := tree["servers"].([]any)
			Expect(ok).To(BeTrue())
			// The parser-level server describes the aggregate, so it leads.
			Expect(servers[0].(map[string]any)["url"]).To(Equal("https://api.example.com"))
			Expect(servers).To(HaveLen(2))
		})

		It("lets a parser-level Info override every document's", func() {
			parser := twoDocuments(tapesoapi.PolicyLastWins)

			// Two documents each describing themselves would otherwise be an
			// Info conflict; the aggregate is this API, and they describe parts
			// of it.
			Expect(at(compileTree(parser, tapesoapi.WithLint()), "info", "title")).
				To(Equal("Aggregate"))
		})
	})
})
