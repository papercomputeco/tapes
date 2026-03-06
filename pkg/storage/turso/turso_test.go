package turso_test

import (
	"context"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/turso"
)

// tursoTestBucket creates a simple bucket for testing with the given text content
func tursoTestBucket(text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     "user",
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}

// skipWithoutTurso skips tests that require a Turso connection.
// Set TAPES_STORAGE_TURSO_DSN and TAPES_STORAGE_TURSO_AUTH_TOKEN to run.
func skipWithoutTurso() (string, string) {
	dsn := os.Getenv("TAPES_STORAGE_TURSO_DSN")
	token := os.Getenv("TAPES_STORAGE_TURSO_AUTH_TOKEN")
	if dsn == "" {
		Skip("TAPES_STORAGE_TURSO_DSN not set, skipping Turso integration tests")
	}
	return dsn, token
}

var _ = Describe("Driver", func() {
	var (
		driver *turso.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		dsn, token := skipWithoutTurso()
		ctx = context.Background()

		tmpDir := GinkgoT().TempDir()
		localPath := filepath.Join(tmpDir, "replica.db")

		var err error
		driver, err = turso.NewDriver(ctx, dsn,
			turso.WithAuthToken(token),
			turso.WithLocalPath(localPath),
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.Migrate(ctx)).To(Succeed())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	Describe("NewDriver", func() {
		It("creates a driver with a local replica path", func() {
			dsn, token := skipWithoutTurso()
			tmpDir := GinkgoT().TempDir()
			localPath := filepath.Join(tmpDir, "test-replica.db")

			d, err := turso.NewDriver(context.Background(), dsn,
				turso.WithAuthToken(token),
				turso.WithLocalPath(localPath),
			)
			Expect(err).NotTo(HaveOccurred())
			defer d.Close()

			Expect(d.Migrate(context.Background())).To(Succeed())
		})
	})

	Describe("Put and Get", func() {
		It("stores and retrieves a node", func() {
			node := merkle.NewNode(tursoTestBucket("test content"), nil)

			_, err := driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := driver.Get(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.Hash).To(Equal(node.Hash))
			Expect(retrieved.Bucket).To(Equal(node.Bucket))
			Expect(retrieved.ParentHash).To(BeNil())
		})

		It("stores and retrieves a node with parent", func() {
			parent := merkle.NewNode(tursoTestBucket("parent"), nil)
			child := merkle.NewNode(tursoTestBucket("child"), parent)

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
			node := merkle.NewNode(tursoTestBucket("test"), nil)

			isNew, err := driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())
			Expect(isNew).To(BeTrue())

			isNew, err = driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())
			Expect(isNew).To(BeFalse())

			nodes, _ := driver.List(ctx)
			Expect(nodes).To(HaveLen(1))
		})
	})

	Describe("Has", func() {
		It("returns true for existing node", func() {
			node := merkle.NewNode(tursoTestBucket("test"), nil)
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

	Describe("List", func() {
		It("returns all nodes", func() {
			node1 := merkle.NewNode(tursoTestBucket("node1"), nil)
			node2 := merkle.NewNode(tursoTestBucket("node2"), node1)
			node3 := merkle.NewNode(tursoTestBucket("node3"), node2)

			driver.Put(ctx, node1)
			driver.Put(ctx, node2)
			driver.Put(ctx, node3)

			nodes, err := driver.List(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodes).To(HaveLen(3))
		})
	})

	Describe("Roots", func() {
		It("returns all root nodes", func() {
			root1 := merkle.NewNode(tursoTestBucket("root1"), nil)
			root2 := merkle.NewNode(tursoTestBucket("root2"), nil)
			child := merkle.NewNode(tursoTestBucket("child"), root1)

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
			root := merkle.NewNode(tursoTestBucket("root"), nil)
			child := merkle.NewNode(tursoTestBucket("child"), root)
			leaf := merkle.NewNode(tursoTestBucket("leaf"), child)

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
			rootBucket := tursoTestBucket("root")
			childBucket := tursoTestBucket("child")
			grandchildBucket := tursoTestBucket("grandchild")

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

	Describe("Depth", func() {
		It("returns 0 for root node", func() {
			root := merkle.NewNode(tursoTestBucket("root"), nil)
			driver.Put(ctx, root)

			depth, err := driver.Depth(ctx, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(depth).To(Equal(0))
		})

		It("returns correct depth for nested nodes", func() {
			root := merkle.NewNode(tursoTestBucket("root"), nil)
			child := merkle.NewNode(tursoTestBucket("child"), root)
			grandchild := merkle.NewNode(tursoTestBucket("grandchild"), child)

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
})
