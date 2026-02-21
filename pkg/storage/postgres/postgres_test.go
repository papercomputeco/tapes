package postgres_test

import (
	"context"
	"fmt"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/postgres"
)

// postgresTestBucket creates a simple bucket for testing with the given text content
func postgresTestBucket(text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     "user",
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}

// connStr returns the PostgreSQL connection string from environment or skips the test.
func connStr() string {
	dsn := os.Getenv("TAPES_TEST_POSTGRES_DSN")
	if dsn == "" {
		Skip("TAPES_TEST_POSTGRES_DSN not set, skipping PostgreSQL tests")
	}
	return dsn
}

var _ = Describe("Driver", func() {
	var (
		driver *postgres.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		dsn := connStr()

		var err error
		driver, err = postgres.NewDriver(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())

		// Clean all nodes before each test for isolation.
		_, err = driver.Client.Node.Delete().Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	Describe("NewDriver", func() {
		It("creates a driver with valid connection string", func() {
			dsn := connStr()
			d, err := postgres.NewDriver(context.Background(), dsn)
			Expect(err).NotTo(HaveOccurred())
			defer d.Close()
		})

		It("returns an error for invalid connection string", func() {
			_, err := postgres.NewDriver(context.Background(), "host=invalid port=9999 user=bad dbname=bad sslmode=disable connect_timeout=1")
			Expect(err).To(HaveOccurred())
			fmt.Fprintf(GinkgoWriter, "expected error: %v\n", err)
		})
	})

	Describe("Put and Get", func() {
		It("stores and retrieves a node", func() {
			node := merkle.NewNode(postgresTestBucket("test content"), nil)

			_, err := driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := driver.Get(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.Hash).To(Equal(node.Hash))
			Expect(retrieved.Bucket).To(Equal(node.Bucket))
			Expect(retrieved.ParentHash).To(BeNil())
		})

		It("stores and retrieves a node with parent", func() {
			parent := merkle.NewNode(postgresTestBucket("parent"), nil)
			child := merkle.NewNode(postgresTestBucket("child"), parent)

			_, err := driver.Put(ctx, parent)
			Expect(err).NotTo(HaveOccurred())

			_, err = driver.Put(ctx, child)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := driver.Get(ctx, child.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.ParentHash).NotTo(BeNil())
			Expect(*retrieved.ParentHash).To(Equal(parent.Hash))
		})

		It("returns NotFoundError for non-existent hash", func() {
			_, err := driver.Get(ctx, "nonexistent")
			Expect(err).To(HaveOccurred())

			var notFoundErr storage.NotFoundError
			Expect(err).To(BeAssignableToTypeOf(notFoundErr))
		})

		It("is idempotent for duplicate puts", func() {
			node := merkle.NewNode(postgresTestBucket("test"), nil)

			isNew, err := driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())
			Expect(isNew).To(BeTrue())

			isNew, err = driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())
			Expect(isNew).To(BeFalse())

			nodes, _ := driver.List(ctx)
			Expect(nodes).To(HaveLen(1))
		})

		It("rejects nil nodes", func() {
			_, err := driver.Put(ctx, nil)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nil node"))
		})
	})

	Describe("Has", func() {
		It("returns true for existing node", func() {
			node := merkle.NewNode(postgresTestBucket("test"), nil)
			driver.Put(ctx, node)

			exists, err := driver.Has(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
		})

		It("returns false for non-existent hash", func() {
			exists, err := driver.Has(ctx, "nonexistent")
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
		})
	})

	Describe("GetByParent", func() {
		It("returns children of a parent", func() {
			parent := merkle.NewNode(postgresTestBucket("parent"), nil)
			child1 := merkle.NewNode(postgresTestBucket("child1"), parent)
			child2 := merkle.NewNode(postgresTestBucket("child2"), parent)

			driver.Put(ctx, parent)
			driver.Put(ctx, child1)
			driver.Put(ctx, child2)

			children, err := driver.GetByParent(ctx, &parent.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(children).To(HaveLen(2))
		})

		It("returns root nodes when parentHash is nil", func() {
			root1 := merkle.NewNode(postgresTestBucket("root1"), nil)
			root2 := merkle.NewNode(postgresTestBucket("root2"), nil)
			child := merkle.NewNode(postgresTestBucket("child"), root1)

			driver.Put(ctx, root1)
			driver.Put(ctx, root2)
			driver.Put(ctx, child)

			roots, err := driver.GetByParent(ctx, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(roots).To(HaveLen(2))
		})
	})

	Describe("List", func() {
		It("returns all nodes", func() {
			node1 := merkle.NewNode(postgresTestBucket("node1"), nil)
			node2 := merkle.NewNode(postgresTestBucket("node2"), node1)
			node3 := merkle.NewNode(postgresTestBucket("node3"), node2)

			driver.Put(ctx, node1)
			driver.Put(ctx, node2)
			driver.Put(ctx, node3)

			nodes, err := driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(3))
		})

		It("returns empty slice for empty store", func() {
			nodes, err := driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(BeEmpty())
		})
	})

	Describe("Roots", func() {
		It("returns all root nodes", func() {
			root1 := merkle.NewNode(postgresTestBucket("root1"), nil)
			root2 := merkle.NewNode(postgresTestBucket("root2"), nil)
			child := merkle.NewNode(postgresTestBucket("child"), root1)

			driver.Put(ctx, root1)
			driver.Put(ctx, root2)
			driver.Put(ctx, child)

			roots, err := driver.Roots(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(roots).To(HaveLen(2))
		})
	})

	Describe("Leaves", func() {
		It("returns all leaf nodes", func() {
			root := merkle.NewNode(postgresTestBucket("root"), nil)
			child := merkle.NewNode(postgresTestBucket("child"), root)
			leaf := merkle.NewNode(postgresTestBucket("leaf"), child)

			driver.Put(ctx, root)
			driver.Put(ctx, child)
			driver.Put(ctx, leaf)

			leaves, err := driver.Leaves(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(leaves).To(HaveLen(1))
			Expect(leaves[0].Hash).To(Equal(leaf.Hash))
		})
	})

	Describe("Ancestry", func() {
		It("returns path from node to root", func() {
			rootBucket := postgresTestBucket("root")
			childBucket := postgresTestBucket("child")
			grandchildBucket := postgresTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child := merkle.NewNode(childBucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child)

			driver.Put(ctx, root)
			driver.Put(ctx, child)
			driver.Put(ctx, grandchild)

			ancestry, err := driver.Ancestry(ctx, grandchild.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(ancestry).To(HaveLen(3))
			Expect(ancestry[0].Bucket).To(Equal(grandchildBucket))
			Expect(ancestry[1].Bucket).To(Equal(childBucket))
			Expect(ancestry[2].Bucket).To(Equal(rootBucket))
		})
	})

	Describe("LoadDag (merkle.LoadDag with driver as BranchLoader)", func() {
		It("returns full branch for a leaf node", func() {
			rootBucket := postgresTestBucket("root")
			childBucket := postgresTestBucket("child")
			grandchildBucket := postgresTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child := merkle.NewNode(childBucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child)

			driver.Put(ctx, root)
			driver.Put(ctx, child)
			driver.Put(ctx, grandchild)

			dag, err := merkle.LoadDag(ctx, driver, grandchild.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(3))

			Expect(dag.Root.Bucket).To(Equal(rootBucket))
			Expect(dag.Get(child.Hash).Bucket).To(Equal(childBucket))
			Expect(dag.Get(grandchild.Hash).Bucket).To(Equal(grandchildBucket))
		})

		It("returns full branch for a root node with descendants", func() {
			rootBucket := postgresTestBucket("root")
			childBucket := postgresTestBucket("child")
			grandchildBucket := postgresTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child := merkle.NewNode(childBucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child)

			driver.Put(ctx, root)
			driver.Put(ctx, child)
			driver.Put(ctx, grandchild)

			dag, err := merkle.LoadDag(ctx, driver, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(3))

			Expect(dag.Root.Bucket).To(Equal(rootBucket))
			Expect(dag.Get(child.Hash).Bucket).To(Equal(childBucket))
			Expect(dag.Get(grandchild.Hash).Bucket).To(Equal(grandchildBucket))
		})

		It("returns all branches when there are multiple children", func() {
			rootBucket := postgresTestBucket("root")
			child1Bucket := postgresTestBucket("child1")
			child2Bucket := postgresTestBucket("child2")
			grandchildBucket := postgresTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child1 := merkle.NewNode(child1Bucket, root)
			child2 := merkle.NewNode(child2Bucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child1)

			driver.Put(ctx, root)
			driver.Put(ctx, child1)
			driver.Put(ctx, child2)
			driver.Put(ctx, grandchild)

			dag, err := merkle.LoadDag(ctx, driver, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(4))

			Expect(dag.Root.Bucket).To(Equal(rootBucket))
			Expect(dag.Get(root.Hash)).NotTo(BeNil())
			Expect(dag.Get(child1.Hash)).NotTo(BeNil())
			Expect(dag.Get(child2.Hash)).NotTo(BeNil())
			Expect(dag.Get(grandchild.Hash)).NotTo(BeNil())
		})

		It("returns only ancestors and one branch when queried from a leaf", func() {
			rootBucket := postgresTestBucket("root")
			child1Bucket := postgresTestBucket("child1")
			child2Bucket := postgresTestBucket("child2")

			root := merkle.NewNode(rootBucket, nil)
			child1 := merkle.NewNode(child1Bucket, root)
			child2 := merkle.NewNode(child2Bucket, root)

			driver.Put(ctx, root)
			driver.Put(ctx, child1)
			driver.Put(ctx, child2)

			dag, err := merkle.LoadDag(ctx, driver, child1.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(2))

			Expect(dag.Root.Bucket).To(Equal(rootBucket))
			Expect(dag.Get(child1.Hash).Bucket).To(Equal(child1Bucket))
			Expect(dag.Get(child2.Hash)).To(BeNil())
		})
	})

	Describe("Depth", func() {
		It("returns 0 for root node", func() {
			root := merkle.NewNode(postgresTestBucket("root"), nil)
			driver.Put(ctx, root)

			depth, err := driver.Depth(ctx, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(depth).To(Equal(0))
		})

		It("returns correct depth for nested nodes", func() {
			root := merkle.NewNode(postgresTestBucket("root"), nil)
			child := merkle.NewNode(postgresTestBucket("child"), root)
			grandchild := merkle.NewNode(postgresTestBucket("grandchild"), child)

			driver.Put(ctx, root)
			driver.Put(ctx, child)
			driver.Put(ctx, grandchild)

			depth, err := driver.Depth(ctx, grandchild.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(depth).To(Equal(2))
		})
	})

	Describe("Complex content", func() {
		It("stores and retrieves node with usage metadata", func() {
			bucket := merkle.Bucket{
				Type:     "message",
				Role:     "assistant",
				Content:  []llm.ContentBlock{{Type: "text", Text: "Hello, world!"}},
				Model:    "gpt-4",
				Provider: "openai",
			}
			node := merkle.NewNode(bucket, nil, merkle.NodeMeta{
				StopReason: "stop",
				Usage: &llm.Usage{
					PromptTokens:     10,
					CompletionTokens: 5,
					TotalTokens:      15,
				},
			})

			_, err := driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := driver.Get(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())

			Expect(retrieved.Bucket.Role).To(Equal("assistant"))
			Expect(retrieved.Bucket.Model).To(Equal("gpt-4"))
			Expect(retrieved.StopReason).To(Equal("stop"))
			Expect(retrieved.Usage).NotTo(BeNil())
			Expect(retrieved.Usage.TotalTokens).To(Equal(15))
		})
	})

	Describe("Content-addressable deduplication", func() {
		It("deduplicates identical nodes", func() {
			bucket := postgresTestBucket("identical")
			node1 := merkle.NewNode(bucket, nil)
			node2 := merkle.NewNode(bucket, nil)

			Expect(node1.Hash).To(Equal(node2.Hash))

			driver.Put(ctx, node1)
			driver.Put(ctx, node2)

			nodes, _ := driver.List(ctx)
			Expect(nodes).To(HaveLen(1))
		})

		It("creates branches for different content with same parent", func() {
			parent := merkle.NewNode(postgresTestBucket("parent"), nil)
			branch1 := merkle.NewNode(postgresTestBucket("branch1"), parent)
			branch2 := merkle.NewNode(postgresTestBucket("branch2"), parent)

			driver.Put(ctx, parent)
			driver.Put(ctx, branch1)
			driver.Put(ctx, branch2)

			children, _ := driver.GetByParent(ctx, &parent.Hash)
			Expect(children).To(HaveLen(2))

			leaves, _ := driver.Leaves(ctx)
			Expect(leaves).To(HaveLen(2))
		})
	})
})
