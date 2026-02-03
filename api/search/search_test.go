package search_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/api/search"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	testutils "github.com/papercomputeco/tapes/pkg/utils/test"
	"github.com/papercomputeco/tapes/pkg/vector"
)

func TestSearch(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Search Suite")
}

var _ = Describe("Search", func() {
	var (
		driver       *inmemory.Driver
		vectorDriver *testutils.MockVectorDriver
		embedder     *testutils.MockEmbedder
		logger       *zap.Logger
		ctx          context.Context
		searcher     *search.Searcher
	)

	BeforeEach(func() {
		logger, _ = zap.NewDevelopment()
		driver = inmemory.NewDriver()
		vectorDriver = testutils.NewMockVectorDriver()
		embedder = testutils.NewMockEmbedder()
		ctx = context.Background()
		searcher = search.NewSearcher(ctx, embedder, vectorDriver, driver, logger)
	})

	Describe("Search function", func() {
		It("returns empty results when vector store has no matches", func() {
			output, err := searcher.Search("hello", 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(output.Query).To(Equal("hello"))
			Expect(output.Count).To(Equal(0))
			Expect(output.Results).To(BeEmpty())
		})

		It("returns search results with full branches", func() {
			node1 := merkle.NewNode(testutils.NewTestBucket("user", "Hello"), nil)
			node2 := merkle.NewNode(testutils.NewTestBucket("assistant", "Hi there"), node1)

			_, err := driver.Put(ctx, node1)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, node2)
			Expect(err).NotTo(HaveOccurred())

			vectorDriver.Results = []vector.QueryResult{
				{
					Document: vector.Document{
						ID:   node2.Hash,
						Hash: node2.Hash,
					},
					Score: 0.95,
				},
			}

			output, err := searcher.Search("greeting", 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(output.Query).To(Equal("greeting"))
			Expect(output.Count).To(Equal(1))
			Expect(output.Results).To(HaveLen(1))
			Expect(output.Results[0].Hash).To(Equal(node2.Hash))
			Expect(output.Results[0].Score).To(Equal(float32(0.95)))
			Expect(output.Results[0].Branch).To(HaveLen(2))
		})

		It("defaults topK to 5 when zero", func() {
			output, err := searcher.Search("test", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).NotTo(BeNil())
		})

		It("returns an error when embedding fails", func() {
			embedder.FailOn = "fail-query"
			_, err := searcher.Search("fail-query", 5)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to embed query"))
		})

		It("returns an error when vector query fails", func() {
			vectorDriver.FailQuery = true
			_, err := searcher.Search("test", 5)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to query vector store"))
		})

		It("skips results where DAG loading fails", func() {
			// Add a result that references a hash not in the store
			vectorDriver.Results = []vector.QueryResult{
				{
					Document: vector.Document{
						ID:   "nonexistent-hash",
						Hash: "nonexistent-hash",
					},
					Score: 0.9,
				},
			}

			output, err := searcher.Search("test", 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(output.Count).To(Equal(0))
		})
	})

	Describe("BuildResult", func() {
		It("builds a result from a single node", func() {
			node := merkle.NewNode(testutils.NewTestBucket("user", "Hello world"), nil)
			_, err := driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())

			result := vector.QueryResult{
				Document: vector.Document{
					ID:   node.Hash,
					Hash: node.Hash,
				},
				Score: 0.95,
			}

			dag, err := merkle.LoadDag(ctx, driver, node.Hash)
			Expect(err).NotTo(HaveOccurred())

			searchResult := searcher.BuildResult(result, dag)

			Expect(searchResult.Hash).To(Equal(node.Hash))
			Expect(searchResult.Score).To(Equal(float32(0.95)))
			Expect(searchResult.Role).To(Equal("user"))
			Expect(searchResult.Preview).To(Equal("Hello world"))
			Expect(searchResult.Turns).To(Equal(1))
			Expect(searchResult.Branch).To(HaveLen(1))
		})

		It("builds a result from a conversation chain", func() {
			node1 := merkle.NewNode(testutils.NewTestBucket("user", "Hello"), nil)
			node2 := merkle.NewNode(testutils.NewTestBucket("assistant", "Hi there"), node1)
			node3 := merkle.NewNode(testutils.NewTestBucket("user", "How are you?"), node2)

			_, err := driver.Put(ctx, node1)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, node2)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, node3)
			Expect(err).NotTo(HaveOccurred())

			result := vector.QueryResult{
				Document: vector.Document{
					ID:   node3.Hash,
					Hash: node3.Hash,
				},
				Score: 0.85,
			}

			dag, err := merkle.LoadDag(ctx, driver, node3.Hash)
			Expect(err).NotTo(HaveOccurred())

			searchResult := searcher.BuildResult(result, dag)

			Expect(searchResult.Hash).To(Equal(node3.Hash))
			Expect(searchResult.Turns).To(Equal(3))
			Expect(searchResult.Role).To(Equal("user"))
			Expect(searchResult.Preview).To(Equal("How are you?"))

			// Dag branch should be in chronological order (root to leaves)
			Expect(searchResult.Branch).To(HaveLen(3))
			Expect(searchResult.Branch[0].Text).To(Equal("Hello"))
			Expect(searchResult.Branch[1].Text).To(Equal("Hi there"))
			Expect(searchResult.Branch[2].Text).To(Equal("How are you?"))
			Expect(searchResult.Branch[2].Matched).To(BeTrue())
			Expect(searchResult.Branch[0].Matched).To(BeFalse())
			Expect(searchResult.Branch[1].Matched).To(BeFalse())
		})

		It("handles empty DAG gracefully", func() {
			result := vector.QueryResult{
				Document: vector.Document{
					ID:   "empty-hash",
					Hash: "empty-hash",
				},
				Score: 0.5,
			}

			dag := merkle.NewDag()
			searchResult := searcher.BuildResult(result, dag)

			Expect(searchResult.Hash).To(Equal("empty-hash"))
			Expect(searchResult.Turns).To(Equal(0))
			Expect(searchResult.Role).To(BeEmpty())
			Expect(searchResult.Preview).To(BeEmpty())
			Expect(searchResult.Branch).To(BeEmpty())
		})
	})
})
