package mcp

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
	testutils "github.com/papercomputeco/tapes/pkg/utils/test"
	"github.com/papercomputeco/tapes/pkg/vector"
)

var _ = Describe("Search tool", func() {
	var (
		driver *inmemory.InMemoryDriver
		ctx    context.Context
	)

	BeforeEach(func() {
		driver = inmemory.NewInMemoryDriver()

		var err error
		Expect(err).NotTo(HaveOccurred())
		ctx = context.TODO()
	})

	Describe("buildSearchResult", func() {
		It("builds a result from a single node", func() {
			node := merkle.NewNode(testutils.NewTestBucket("user", "Hello world"), nil)
			Expect(driver.Put(ctx, node)).To(Succeed())

			result := vector.QueryResult{
				Document: vector.Document{
					ID:   node.Hash,
					Hash: node.Hash,
				},
				Score: 0.95,
			}

			dag, err := merkle.LoadDag(ctx, driver, node.Hash)
			Expect(err).NotTo(HaveOccurred())

			searchResult := buildSearchResult(result, dag)

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

			Expect(driver.Put(ctx, node1)).To(Succeed())
			Expect(driver.Put(ctx, node2)).To(Succeed())
			Expect(driver.Put(ctx, node3)).To(Succeed())

			result := vector.QueryResult{
				Document: vector.Document{
					ID:   node3.Hash,
					Hash: node3.Hash,
				},
				Score: 0.85,
			}

			dag, err := merkle.LoadDag(ctx, driver, node3.Hash)
			Expect(err).NotTo(HaveOccurred())

			searchResult := buildSearchResult(result, dag)

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
			searchResult := buildSearchResult(result, dag)

			Expect(searchResult.Hash).To(Equal("empty-hash"))
			Expect(searchResult.Turns).To(Equal(0))
			Expect(searchResult.Role).To(BeEmpty())
			Expect(searchResult.Preview).To(BeEmpty())
			Expect(searchResult.Branch).To(BeEmpty())
		})
	})
})
