package sqlite_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/sqlite"
	"github.com/papercomputeco/tapes/pkg/storage/storagetest"
)

var _ = storagetest.RunListSessionsSpecs("sqlite", func() storage.Driver {
	ctx := context.Background()
	d, err := sqlite.NewDriver(ctx, ":memory:")
	Expect(err).NotTo(HaveOccurred())
	Expect(d.Migrate(ctx)).To(Succeed())
	return d
})

var _ = storagetest.RunAncestryChainBasicSpecs("sqlite", func() storage.Driver {
	ctx := context.Background()
	d, err := sqlite.NewDriver(ctx, ":memory:")
	Expect(err).NotTo(HaveOccurred())
	Expect(d.Migrate(ctx)).To(Succeed())
	return d
})

var _ = storagetest.RunAncestryChainsSpecs("sqlite", func() storage.Driver {
	ctx := context.Background()
	d, err := sqlite.NewDriver(ctx, ":memory:")
	Expect(err).NotTo(HaveOccurred())
	Expect(d.Migrate(ctx)).To(Succeed())
	return d
})

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

var _ = Describe("Driver", func() {
	var (
		driver *sqlite.Driver
		ctx    context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		driver, err = sqlite.NewDriver(ctx, ":memory:")
		Expect(err).NotTo(HaveOccurred())

		Expect(driver.Migrate(ctx)).To(Succeed())
	})

	AfterEach(func() {
		if driver != nil {
			driver.Close()
		}
	})

	Describe("NewDriver", func() {
		It("creates a driver with file database", func() {
			tmpDir := GinkgoT().TempDir()
			dbPath := filepath.Join(tmpDir, "test.db")

			s, err := sqlite.NewDriver(context.Background(), dbPath)
			Expect(err).NotTo(HaveOccurred())
			defer s.Close()

			Expect(s.Migrate(context.Background())).To(Succeed())

			// Verify file was created
			_, err = os.Stat(dbPath)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Put and Get", func() {
		It("stores and retrieves a node", func() {
			node := merkle.NewNode(sqliteTestBucket("test content"), nil)

			_, err := driver.Put(ctx, node)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := driver.Get(ctx, node.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.Hash).To(Equal(node.Hash))
			Expect(retrieved.Bucket).To(Equal(node.Bucket))
			Expect(retrieved.ParentHash).To(BeNil())
		})

		It("stores and retrieves a node with parent", func() {
			parent := merkle.NewNode(sqliteTestBucket("parent"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), parent)

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
			node := merkle.NewNode(sqliteTestBucket("test"), nil)

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
			node := merkle.NewNode(sqliteTestBucket("test"), nil)
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
			parent := merkle.NewNode(sqliteTestBucket("parent"), nil)
			child1 := merkle.NewNode(sqliteTestBucket("child1"), parent)
			child2 := merkle.NewNode(sqliteTestBucket("child2"), parent)

			driver.Put(ctx, parent)
			driver.Put(ctx, child1)
			driver.Put(ctx, child2)

			children, err := driver.GetByParent(ctx, &parent.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(children).To(HaveLen(2))
		})

		It("returns root nodes when parentHash is nil", func() {
			root1 := merkle.NewNode(sqliteTestBucket("root1"), nil)
			root2 := merkle.NewNode(sqliteTestBucket("root2"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), root1)

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
			node1 := merkle.NewNode(sqliteTestBucket("node1"), nil)
			node2 := merkle.NewNode(sqliteTestBucket("node2"), node1)
			node3 := merkle.NewNode(sqliteTestBucket("node3"), node2)

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
			root1 := merkle.NewNode(sqliteTestBucket("root1"), nil)
			root2 := merkle.NewNode(sqliteTestBucket("root2"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), root1)

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
			root := merkle.NewNode(sqliteTestBucket("root"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), root)
			leaf := merkle.NewNode(sqliteTestBucket("leaf"), child)

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
			rootBucket := sqliteTestBucket("root")
			childBucket := sqliteTestBucket("child")
			grandchildBucket := sqliteTestBucket("grandchild")

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
			// Linear chain: root -> child -> grandchild (leaf)
			rootBucket := sqliteTestBucket("root")
			childBucket := sqliteTestBucket("child")
			grandchildBucket := sqliteTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child := merkle.NewNode(childBucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child)

			driver.Put(ctx, root)
			driver.Put(ctx, child)
			driver.Put(ctx, grandchild)

			// Query branch from the leaf - should get all 3 nodes
			dag, err := merkle.LoadDag(ctx, driver, grandchild.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(3))

			// Verify structure using Get
			Expect(dag.Root.Bucket).To(Equal(rootBucket))
			Expect(dag.Get(child.Hash).Bucket).To(Equal(childBucket))
			Expect(dag.Get(grandchild.Hash).Bucket).To(Equal(grandchildBucket))
		})

		It("returns full branch for a root node with descendants", func() {
			// Linear chain: root -> child -> grandchild
			rootBucket := sqliteTestBucket("root")
			childBucket := sqliteTestBucket("child")
			grandchildBucket := sqliteTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child := merkle.NewNode(childBucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child)

			driver.Put(ctx, root)
			driver.Put(ctx, child)
			driver.Put(ctx, grandchild)

			// Query branch from the root - should get all 3 nodes
			dag, err := merkle.LoadDag(ctx, driver, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(3))

			// Verify structure using Get
			Expect(dag.Root.Bucket).To(Equal(rootBucket))
			Expect(dag.Get(child.Hash).Bucket).To(Equal(childBucket))
			Expect(dag.Get(grandchild.Hash).Bucket).To(Equal(grandchildBucket))
		})

		It("returns full branch for a middle node", func() {
			// Linear chain: root -> child -> grandchild
			rootBucket := sqliteTestBucket("root")
			childBucket := sqliteTestBucket("child")
			grandchildBucket := sqliteTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child := merkle.NewNode(childBucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child)

			driver.Put(ctx, root)
			driver.Put(ctx, child)
			driver.Put(ctx, grandchild)

			// Query branch from the middle node - should get all 3 nodes
			dag, err := merkle.LoadDag(ctx, driver, child.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(3))

			// Verify structure using Get
			Expect(dag.Root.Bucket).To(Equal(rootBucket))
			Expect(dag.Get(child.Hash).Bucket).To(Equal(childBucket))
			Expect(dag.Get(grandchild.Hash).Bucket).To(Equal(grandchildBucket))
		})

		It("returns all branches when there are multiple children", func() {
			// Tree structure:
			//       root
			//      /    \
			//   child1  child2
			//     |
			//  grandchild
			rootBucket := sqliteTestBucket("root")
			child1Bucket := sqliteTestBucket("child1")
			child2Bucket := sqliteTestBucket("child2")
			grandchildBucket := sqliteTestBucket("grandchild")

			root := merkle.NewNode(rootBucket, nil)
			child1 := merkle.NewNode(child1Bucket, root)
			child2 := merkle.NewNode(child2Bucket, root)
			grandchild := merkle.NewNode(grandchildBucket, child1)

			driver.Put(ctx, root)
			driver.Put(ctx, child1)
			driver.Put(ctx, child2)
			driver.Put(ctx, grandchild)

			// Query branch from root - should get all 4 nodes (both branches)
			dag, err := merkle.LoadDag(ctx, driver, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(4))

			// Root should be the DAG root
			Expect(dag.Root.Bucket).To(Equal(rootBucket))

			// All nodes should be present
			Expect(dag.Get(root.Hash)).NotTo(BeNil())
			Expect(dag.Get(child1.Hash)).NotTo(BeNil())
			Expect(dag.Get(child2.Hash)).NotTo(BeNil())
			Expect(dag.Get(grandchild.Hash)).NotTo(BeNil())
		})

		It("returns only ancestors and one branch when queried from a leaf", func() {
			// Tree structure:
			//       root
			//      /    \
			//   child1  child2
			rootBucket := sqliteTestBucket("root")
			child1Bucket := sqliteTestBucket("child1")
			child2Bucket := sqliteTestBucket("child2")

			root := merkle.NewNode(rootBucket, nil)
			child1 := merkle.NewNode(child1Bucket, root)
			child2 := merkle.NewNode(child2Bucket, root)

			driver.Put(ctx, root)
			driver.Put(ctx, child1)
			driver.Put(ctx, child2)

			// Query branch from child1 - should only get root + child1 (not child2)
			dag, err := merkle.LoadDag(ctx, driver, child1.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(2))

			// Verify structure using Get
			Expect(dag.Root.Bucket).To(Equal(rootBucket))
			Expect(dag.Get(child1.Hash).Bucket).To(Equal(child1Bucket))
			Expect(dag.Get(child2.Hash)).To(BeNil()) // child2 should not be in the DAG
		})
	})

	Describe("Depth", func() {
		It("returns 0 for root node", func() {
			root := merkle.NewNode(sqliteTestBucket("root"), nil)
			driver.Put(ctx, root)

			depth, err := driver.Depth(ctx, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(depth).To(Equal(0))
		})

		It("returns correct depth for nested nodes", func() {
			root := merkle.NewNode(sqliteTestBucket("root"), nil)
			child := merkle.NewNode(sqliteTestBucket("child"), root)
			grandchild := merkle.NewNode(sqliteTestBucket("grandchild"), child)

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
			// StopReason and Usage are now on Node, not Bucket
			node := merkle.NewNode(bucket, nil, merkle.NodeOptions{
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
			// Same content, same parent (nil) = same hash = stored once
			bucket := sqliteTestBucket("identical")
			node1 := merkle.NewNode(bucket, nil)
			node2 := merkle.NewNode(bucket, nil)

			Expect(node1.Hash).To(Equal(node2.Hash))

			driver.Put(ctx, node1)
			driver.Put(ctx, node2)

			nodes, _ := driver.List(ctx)
			Expect(nodes).To(HaveLen(1))
		})

		It("creates branches for different content with same parent", func() {
			parent := merkle.NewNode(sqliteTestBucket("parent"), nil)
			branch1 := merkle.NewNode(sqliteTestBucket("branch1"), parent)
			branch2 := merkle.NewNode(sqliteTestBucket("branch2"), parent)

			driver.Put(ctx, parent)
			driver.Put(ctx, branch1)
			driver.Put(ctx, branch2)

			// Parent should have 2 children (branches)
			children, _ := driver.GetByParent(ctx, &parent.Hash)
			Expect(children).To(HaveLen(2))

			// Both are leaves
			leaves, _ := driver.Leaves(ctx)
			Expect(leaves).To(HaveLen(2))
		})
	})
})

var _ = Describe("AncestryChain dangling parents [sqlite]", func() {
	// These specs exercise the same path as the shared dangling conformance
	// suite, but have to bypass referential integrity to set up the
	// scenario because the sqlite driver enables _foreign_keys=on. The
	// real dangling rows observed in production came from bulk imports
	// that wrote directly to the table with FKs disabled, so we mirror
	// that by injecting rows over a second FK-off sql.DB handle to the
	// same file.
	var (
		ctx     context.Context
		driver  *sqlite.Driver
		rawDB   *sql.DB
		dbPath  string
		tempDir string
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		tempDir, err = os.MkdirTemp("", "sqlite-dangling-*")
		Expect(err).NotTo(HaveOccurred())
		dbPath = filepath.Join(tempDir, "dangling.sqlite")

		driver, err = sqlite.NewDriver(ctx, dbPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(driver.Migrate(ctx)).To(Succeed())

		// Separate FK-disabled handle used only for the raw orphan insert.
		rawDB, err = sql.Open("sqlite3", dbPath+"?_foreign_keys=off")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if rawDB != nil {
			_ = rawDB.Close()
		}
		if driver != nil {
			_ = driver.Close()
		}
		_ = os.RemoveAll(tempDir)
	})

	insertOrphan := func(hash, parentHash string) {
		now := time.Now().UTC()
		_, err := rawDB.ExecContext(ctx,
			`INSERT INTO nodes (hash, parent_hash, bucket, type, role, created_at)
			 VALUES (?, ?, '{"type":"message","role":"user"}', 'message', 'user', ?)`,
			hash, parentHash, now)
		Expect(err).NotTo(HaveOccurred())
	}

	It("marks the chain incomplete when a parent_hash is missing", func() {
		phantom := "0000000000000000000000000000000000000000000000000000000000000000"
		orphanHash := "aaaa000000000000000000000000000000000000000000000000000000000000"
		childHash := "bbbb000000000000000000000000000000000000000000000000000000000000"

		insertOrphan(orphanHash, phantom)
		insertOrphan(childHash, orphanHash)

		chain, err := driver.AncestryChain(ctx, childHash)
		Expect(err).NotTo(HaveOccurred())
		Expect(chain.Nodes).To(HaveLen(2))
		Expect(chain.Incomplete).To(BeTrue())
		Expect(chain.MissingParent).To(Equal(phantom))
		Expect(chain.Complete()).To(BeFalse())
		// Node-first order: child first, orphan (last resolvable) at tail.
		Expect(chain.Nodes[0].Hash).To(Equal(childHash))
		Expect(chain.Nodes[1].Hash).To(Equal(orphanHash))
	})

	It("marks a single orphan node incomplete without returning an error", func() {
		phantom := "1111111111111111111111111111111111111111111111111111111111111111"
		orphanHash := "cccc000000000000000000000000000000000000000000000000000000000000"
		insertOrphan(orphanHash, phantom)

		chain, err := driver.AncestryChain(ctx, orphanHash)
		Expect(err).NotTo(HaveOccurred())
		Expect(chain.Nodes).To(HaveLen(1))
		Expect(chain.Incomplete).To(BeTrue())
		Expect(chain.MissingParent).To(Equal(phantom))
	})

	It("leaves Ancestry behavior compatible with callers that ignore the marker", func() {
		phantom := "2222222222222222222222222222222222222222222222222222222222222222"
		orphanHash := "dddd000000000000000000000000000000000000000000000000000000000000"
		insertOrphan(orphanHash, phantom)

		nodes, err := driver.Ancestry(ctx, orphanHash)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(1))
	})

	It("stops a two-node cycle in AncestryChain instead of looping", func() {
		hashA := "eeee000000000000000000000000000000000000000000000000000000000000"
		hashB := "ffff000000000000000000000000000000000000000000000000000000000000"
		// A → B → A. Neither can be inserted via Put (content addressing
		// forbids the pair, and the FK rejects it anyway), so we drop
		// the rows in directly over the FK-off handle.
		insertOrphan(hashA, hashB)
		insertOrphan(hashB, hashA)

		chain, err := driver.AncestryChain(ctx, hashA)
		Expect(err).NotTo(HaveOccurred())
		Expect(chain.Nodes).To(HaveLen(2))
		Expect(chain.Incomplete).To(BeTrue())
		Expect(chain.CycleDetected).To(BeTrue())
		Expect(chain.MissingParent).To(BeEmpty())
	})

	It("stops a cycle in the batched AncestryChains path", func() {
		hashA := "aaaacccc00000000000000000000000000000000000000000000000000000000"
		hashB := "bbbbcccc00000000000000000000000000000000000000000000000000000000"
		insertOrphan(hashA, hashB)
		insertOrphan(hashB, hashA)

		chains, err := driver.AncestryChains(ctx, []string{hashA})
		Expect(err).NotTo(HaveOccurred())
		cycleChain := chains[hashA]
		Expect(cycleChain).NotTo(BeNil())
		Expect(cycleChain.Incomplete).To(BeTrue())
		Expect(cycleChain.CycleDetected).To(BeTrue())
		Expect(cycleChain.Nodes).To(HaveLen(2))
	})
})
