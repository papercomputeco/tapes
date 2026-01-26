package merkle_test

import (
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

// sqliteTestBucket creates a simple bucket for testing with the given text content
func sqliteTestBucket(text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     "user",
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}

var _ = Describe("SQLiteStorer", func() {
	var (
		storer *merkle.SQLiteStorer
		ctx    context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		storer, err = merkle.NewSQLiteStorer(":memory:")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if storer != nil {
			storer.Close()
		}
	})

	Describe("NewSQLiteStorer", func() {
		It("creates a storer with in-memory database", func() {
			Expect(storer).NotTo(BeNil())
		})

		It("creates a storer with file database", func() {
			tmpDir := GinkgoT().TempDir()
			dbPath := filepath.Join(tmpDir, "test.db")

			s, err := merkle.NewSQLiteStorer(dbPath)
			Expect(err).NotTo(HaveOccurred())
			defer s.Close()

			// Verify file was created
			_, err = os.Stat(dbPath)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Put and Get", func() {
		It("stores and retrieves a node", func() {
			node := merkle.NewNode(sqliteTestBucket("test content"), nil)

			err := storer.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := storer.Get(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.Hash).To(Equal(node.Hash))
			Expect(retrieved.Bucket).To(Equal(node.Bucket))
			Expect(retrieved.ParentHash).To(BeNil())
		})

		It("stores and retrieves a node with parent", func() {
			parent := merkle.NewNode(sqliteTestBucket("parent"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), parent)

			err := storer.Put(ctx, parent)
			Expect(err).NotTo(HaveOccurred())

			err = storer.Put(ctx, child)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := storer.Get(ctx, child.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.ParentHash).NotTo(BeNil())
			Expect(*retrieved.ParentHash).To(Equal(parent.Hash))
		})

		It("returns ErrNotFound for non-existent hash", func() {
			_, err := storer.Get(ctx, "nonexistent")
			Expect(err).To(HaveOccurred())

			var notFoundErr merkle.ErrNotFound
			Expect(err).To(BeAssignableToTypeOf(notFoundErr))
		})

		It("is idempotent for duplicate puts", func() {
			node := merkle.NewNode(sqliteTestBucket("test"), nil)

			err := storer.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())

			err = storer.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())

			nodes, _ := storer.List(ctx)
			Expect(nodes).To(HaveLen(1))
		})

		It("rejects nil nodes", func() {
			err := storer.Put(ctx, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nil node"))
		})
	})

	Describe("Has", func() {
		It("returns true for existing node", func() {
			node := merkle.NewNode(sqliteTestBucket("test"), nil)
			storer.Put(ctx, node)

			exists, err := storer.Has(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
		})

		It("returns false for non-existent hash", func() {
			exists, err := storer.Has(ctx, "nonexistent")
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
		})
	})

	Describe("GetByParent", func() {
		It("returns children of a parent", func() {
			parent := merkle.NewNode(sqliteTestBucket("parent"), nil)
			child1 := merkle.NewNode(sqliteTestBucket("child1"), parent)
			child2 := merkle.NewNode(sqliteTestBucket("child2"), parent)

			storer.Put(ctx, parent)
			storer.Put(ctx, child1)
			storer.Put(ctx, child2)

			children, err := storer.GetByParent(ctx, &parent.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(children).To(HaveLen(2))
		})

		It("returns root nodes when parentHash is nil", func() {
			root1 := merkle.NewNode(sqliteTestBucket("root1"), nil)
			root2 := merkle.NewNode(sqliteTestBucket("root2"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), root1)

			storer.Put(ctx, root1)
			storer.Put(ctx, root2)
			storer.Put(ctx, child)

			roots, err := storer.GetByParent(ctx, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(roots).To(HaveLen(2))
		})
	})

	Describe("List", func() {
		It("returns all nodes", func() {
			node1 := merkle.NewNode(sqliteTestBucket("node1"), nil)
			node2 := merkle.NewNode(sqliteTestBucket("node2"), node1)
			node3 := merkle.NewNode(sqliteTestBucket("node3"), node2)

			storer.Put(ctx, node1)
			storer.Put(ctx, node2)
			storer.Put(ctx, node3)

			nodes, err := storer.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(3))
		})

		It("returns empty slice for empty store", func() {
			nodes, err := storer.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(BeEmpty())
		})
	})

	Describe("Roots", func() {
		It("returns all root nodes", func() {
			root1 := merkle.NewNode(sqliteTestBucket("root1"), nil)
			root2 := merkle.NewNode(sqliteTestBucket("root2"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), root1)

			storer.Put(ctx, root1)
			storer.Put(ctx, root2)
			storer.Put(ctx, child)

			roots, err := storer.Roots(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(roots).To(HaveLen(2))
		})
	})

	Describe("Leaves", func() {
		It("returns all leaf nodes", func() {
			root := merkle.NewNode(sqliteTestBucket("root"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), root)
			leaf := merkle.NewNode(sqliteTestBucket("leaf"), child)

			storer.Put(ctx, root)
			storer.Put(ctx, child)
			storer.Put(ctx, leaf)

			leaves, err := storer.Leaves(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(leaves).To(HaveLen(1))
			Expect(leaves[0].Hash).To(Equal(leaf.Hash))
		})
	})

	Describe("Ancestry", func() {
		It("returns path from node to root", func() {
			rootBucket := sqliteTestBucket("root")
			childBucket := sqliteTestBucket("child")
			grandchildBucket := sqliteTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child := merkle.NewNode(childBucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child)

			storer.Put(ctx, root)
			storer.Put(ctx, child)
			storer.Put(ctx, grandchild)

			ancestry, err := storer.Ancestry(ctx, grandchild.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(ancestry).To(HaveLen(3))
			Expect(ancestry[0].Bucket).To(Equal(grandchildBucket))
			Expect(ancestry[1].Bucket).To(Equal(childBucket))
			Expect(ancestry[2].Bucket).To(Equal(rootBucket))
		})
	})

	Describe("Descendants", func() {
		It("returns path from root to node", func() {
			rootBucket := sqliteTestBucket("root")
			childBucket := sqliteTestBucket("child")
			grandchildBucket := sqliteTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child := merkle.NewNode(childBucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child)

			storer.Put(ctx, root)
			storer.Put(ctx, child)
			storer.Put(ctx, grandchild)

			ancestry, err := storer.Descendants(ctx, grandchild.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(ancestry).To(HaveLen(3))
			Expect(ancestry[0].Bucket).To(Equal(rootBucket))
			Expect(ancestry[1].Bucket).To(Equal(childBucket))
			Expect(ancestry[2].Bucket).To(Equal(grandchildBucket))
		})
	})

	Describe("Depth", func() {
		It("returns 0 for root node", func() {
			root := merkle.NewNode(sqliteTestBucket("root"), nil)
			storer.Put(ctx, root)

			depth, err := storer.Depth(ctx, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(depth).To(Equal(0))
		})

		It("returns correct depth for nested nodes", func() {
			root := merkle.NewNode(sqliteTestBucket("root"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), root)
			grandchild := merkle.NewNode(sqliteTestBucket("grandchild"), child)

			storer.Put(ctx, root)
			storer.Put(ctx, child)
			storer.Put(ctx, grandchild)

			depth, err := storer.Depth(ctx, grandchild.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(depth).To(Equal(2))
		})
	})

	Describe("Complex content", func() {
		It("stores and retrieves bucket with usage metrics", func() {
			bucket := merkle.Bucket{
				Type:       "message",
				Role:       "assistant",
				Content:    []llm.ContentBlock{{Type: "text", Text: "Hello, world!"}},
				Model:      "gpt-4",
				Provider:   "openai",
				StopReason: "stop",
				Usage: &llm.Usage{
					PromptTokens:     10,
					CompletionTokens: 5,
					TotalTokens:      15,
				},
			}
			node := merkle.NewNode(bucket, nil)

			err := storer.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := storer.Get(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())

			Expect(retrieved.Bucket.Role).To(Equal("assistant"))
			Expect(retrieved.Bucket.Model).To(Equal("gpt-4"))
			Expect(retrieved.Bucket.Usage).NotTo(BeNil())
			Expect(retrieved.Bucket.Usage.TotalTokens).To(Equal(15))
		})
	})

	Describe("Content-addressable deduplication", func() {
		It("deduplicates identical nodes", func() {
			// Same content, same parent (nil) = same hash = stored once
			bucket := sqliteTestBucket("identical")
			node1 := merkle.NewNode(bucket, nil)
			node2 := merkle.NewNode(bucket, nil)

			Expect(node1.Hash).To(Equal(node2.Hash))

			storer.Put(ctx, node1)
			storer.Put(ctx, node2)

			nodes, _ := storer.List(ctx)
			Expect(nodes).To(HaveLen(1))
		})

		It("creates branches for different content with same parent", func() {
			parent := merkle.NewNode(sqliteTestBucket("parent"), nil)
			branch1 := merkle.NewNode(sqliteTestBucket("branch1"), parent)
			branch2 := merkle.NewNode(sqliteTestBucket("branch2"), parent)

			storer.Put(ctx, parent)
			storer.Put(ctx, branch1)
			storer.Put(ctx, branch2)

			// Parent should have 2 children (branches)
			children, _ := storer.GetByParent(ctx, &parent.Hash)
			Expect(children).To(HaveLen(2))

			// Both are leaves
			leaves, _ := storer.Leaves(ctx)
			Expect(leaves).To(HaveLen(2))
		})
	})
})
