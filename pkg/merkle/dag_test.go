package merkle_test

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// dagTestBucket creates a simple bucket for testing with the given role and text
func dagTestBucket(role, text string) merkle.Bucket {
	return merkle.Bucket{
		Type:     "message",
		Role:     role,
		Content:  []llm.ContentBlock{{Type: "text", Text: text}},
		Model:    "test-model",
		Provider: "test-provider",
	}
}

// buildTestDag is a helper that stores nodes with the in-memory driver and loads
// the DAG from the specified node hash. If loadFromHash is empty, it loads from
// the last node in the slice.
func buildTestDag(ctx context.Context, nodes []*merkle.Node, loadFromHash string) (*merkle.Dag, error) {
	driver := inmemory.NewDriver()
	for _, node := range nodes {
		if _, err := driver.Put(ctx, node); err != nil {
			return nil, err
		}
	}

	hash := loadFromHash
	if hash == "" && len(nodes) > 0 {
		hash = nodes[len(nodes)-1].Hash
	}

	return merkle.LoadDag(ctx, driver, hash)
}

var _ = Describe("Dag", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("NewDag", func() {
		It("creates an empty DAG", func() {
			dag := merkle.NewDag()
			Expect(dag).NotTo(BeNil())
			Expect(dag.Root).To(BeNil())
			Expect(dag.Size()).To(Equal(0))
		})
	})

	Describe("Get", func() {
		It("returns node by hash", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			found := dag.Get(root.Hash)
			Expect(found).NotTo(BeNil())
			Expect(found.Hash).To(Equal(root.Hash))
		})

		It("returns nil for unknown hash", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			found := dag.Get("unknown")
			Expect(found).To(BeNil())
		})
	})

	Describe("Size", func() {
		It("returns correct count for single node", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			Expect(dag.Size()).To(Equal(1))
		})

		It("returns correct count for chain", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "Hi"), root)

			dag, err := buildTestDag(ctx, []*merkle.Node{root, child}, "")
			Expect(err).NotTo(HaveOccurred())

			Expect(dag.Size()).To(Equal(2))
		})
	})

	Describe("Leaves", func() {
		It("returns empty for empty DAG", func() {
			dag := merkle.NewDag()
			Expect(dag.Leaves()).To(BeEmpty())
		})

		It("returns root if it's the only node", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			leaves := dag.Leaves()
			Expect(leaves).To(HaveLen(1))
			Expect(leaves[0].Hash).To(Equal(root.Hash))
		})

		It("returns leaf nodes in a chain", func() {
			root := merkle.NewNode(dagTestBucket("user", "1"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "2"), root)
			grandchild := merkle.NewNode(dagTestBucket("user", "3"), child)

			dag, err := buildTestDag(ctx, []*merkle.Node{root, child, grandchild}, "")
			Expect(err).NotTo(HaveOccurred())

			leaves := dag.Leaves()
			Expect(leaves).To(HaveLen(1))
			Expect(leaves[0].Hash).To(Equal(grandchild.Hash))
		})

		It("returns multiple leaves for branching DAG", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)
			branch1 := merkle.NewNode(dagTestBucket("assistant", "Hi 1"), root)
			branch2 := merkle.NewNode(dagTestBucket("assistant", "Hi 2"), root)

			// Load from root to get both branches
			dag, err := buildTestDag(ctx, []*merkle.Node{root, branch1, branch2}, root.Hash)
			Expect(err).NotTo(HaveOccurred())

			leaves := dag.Leaves()
			Expect(leaves).To(HaveLen(2))
		})
	})

	Describe("Walk", func() {
		It("visits all nodes depth-first", func() {
			root := merkle.NewNode(dagTestBucket("user", "1"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "2"), root)
			grandchild := merkle.NewNode(dagTestBucket("user", "3"), child)

			dag, err := buildTestDag(ctx, []*merkle.Node{root, child, grandchild}, "")
			Expect(err).NotTo(HaveOccurred())

			var visited []string
			err = dag.Walk(func(node *merkle.DagNode) (bool, error) {
				visited = append(visited, node.Bucket.ExtractText())
				return true, nil
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(visited).To(Equal([]string{"1", "2", "3"}))
		})

		It("stops when callback returns false", func() {
			root := merkle.NewNode(dagTestBucket("user", "1"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "2"), root)
			grandchild := merkle.NewNode(dagTestBucket("user", "3"), child)

			dag, err := buildTestDag(ctx, []*merkle.Node{root, child, grandchild}, "")
			Expect(err).NotTo(HaveOccurred())

			var visited []string
			err = dag.Walk(func(node *merkle.DagNode) (bool, error) {
				visited = append(visited, node.Bucket.ExtractText())

				// Stop after we hit "2"
				return node.Bucket.ExtractText() != "2", nil
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(visited).To(Equal([]string{"1", "2"}))
		})

		It("stops and propagates error from callback", func() {
			root := merkle.NewNode(dagTestBucket("user", "1"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "2"), root)
			grandchild := merkle.NewNode(dagTestBucket("user", "3"), child)

			dag, err := buildTestDag(ctx, []*merkle.Node{root, child, grandchild}, "")
			Expect(err).NotTo(HaveOccurred())

			testErr := errors.New("test error")
			var visited []string
			walkErr := dag.Walk(func(node *merkle.DagNode) (bool, error) {
				visited = append(visited, node.Bucket.ExtractText())
				if node.Bucket.ExtractText() == "2" {
					return false, testErr
				}
				return true, nil
			})
			Expect(walkErr).To(MatchError(testErr))

			// Should have stopped at node "2"
			Expect(visited).To(Equal([]string{"1", "2"}))
		})

		It("does nothing for empty DAG", func() {
			dag := merkle.NewDag()

			var visited []string
			err := dag.Walk(func(node *merkle.DagNode) (bool, error) {
				visited = append(visited, node.Bucket.ExtractText())
				return true, nil
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(visited).To(BeEmpty())
		})
	})

	Describe("Ancestors", func() {
		It("returns nil for unknown hash", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			Expect(dag.Ancestors("unknown")).To(BeNil())
		})

		It("returns just the node for root", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			ancestors := dag.Ancestors(root.Hash)
			Expect(ancestors).To(HaveLen(1))
			Expect(ancestors[0].Hash).To(Equal(root.Hash))
		})

		It("returns path from node to root", func() {
			root := merkle.NewNode(dagTestBucket("user", "1"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "2"), root)
			grandchild := merkle.NewNode(dagTestBucket("user", "3"), child)

			dag, err := buildTestDag(ctx, []*merkle.Node{root, child, grandchild}, "")
			Expect(err).NotTo(HaveOccurred())

			ancestors := dag.Ancestors(grandchild.Hash)
			Expect(ancestors).To(HaveLen(3))
			Expect(ancestors[0].Hash).To(Equal(grandchild.Hash))
			Expect(ancestors[1].Hash).To(Equal(child.Hash))
			Expect(ancestors[2].Hash).To(Equal(root.Hash))
		})
	})

	Describe("Descendants", func() {
		It("returns nil for unknown hash", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			Expect(dag.Descendants("unknown")).To(BeNil())
		})

		It("returns empty for leaf node", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			descendants := dag.Descendants(root.Hash)
			Expect(descendants).To(BeEmpty())
		})

		It("returns all descendants", func() {
			root := merkle.NewNode(dagTestBucket("user", "1"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "2"), root)
			grandchild := merkle.NewNode(dagTestBucket("user", "3"), child)

			dag, err := buildTestDag(ctx, []*merkle.Node{root, child, grandchild}, root.Hash)
			Expect(err).NotTo(HaveOccurred())

			descendants := dag.Descendants(root.Hash)
			Expect(descendants).To(HaveLen(2))

			hashes := make(map[string]bool)
			for _, d := range descendants {
				hashes[d.Hash] = true
			}
			Expect(hashes).To(HaveKey(child.Hash))
			Expect(hashes).To(HaveKey(grandchild.Hash))
		})
	})

	Describe("IsBranching", func() {
		It("returns false for unknown hash", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			Expect(dag.IsBranching("unknown")).To(BeFalse())
		})

		It("returns false for leaf", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)

			dag, err := buildTestDag(ctx, []*merkle.Node{root}, "")
			Expect(err).NotTo(HaveOccurred())

			Expect(dag.IsBranching(root.Hash)).To(BeFalse())
		})

		It("returns false for single child", func() {
			root := merkle.NewNode(dagTestBucket("user", "1"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "2"), root)

			dag, err := buildTestDag(ctx, []*merkle.Node{root, child}, "")
			Expect(err).NotTo(HaveOccurred())

			Expect(dag.IsBranching(root.Hash)).To(BeFalse())
		})

		It("returns true for multiple children", func() {
			// Build a tree: root -> branch1, root -> branch2
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)
			branch1 := merkle.NewNode(dagTestBucket("assistant", "Hi 1"), root)
			branch2 := merkle.NewNode(dagTestBucket("assistant", "Hi 2"), root)

			// Load from root to get both branches
			dag, err := buildTestDag(ctx, []*merkle.Node{root, branch1, branch2}, root.Hash)
			Expect(err).NotTo(HaveOccurred())

			Expect(dag.IsBranching(root.Hash)).To(BeTrue())
		})
	})

	Describe("BranchPoints", func() {
		It("returns empty for linear DAG", func() {
			root := merkle.NewNode(dagTestBucket("user", "1"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "2"), root)

			dag, err := buildTestDag(ctx, []*merkle.Node{root, child}, "")
			Expect(err).NotTo(HaveOccurred())

			Expect(dag.BranchPoints()).To(BeEmpty())
		})

		It("returns nodes with multiple children", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)
			branch1 := merkle.NewNode(dagTestBucket("assistant", "Hi 1"), root)
			branch2 := merkle.NewNode(dagTestBucket("assistant", "Hi 2"), root)

			// Load from root to get both branches
			dag, err := buildTestDag(ctx, []*merkle.Node{root, branch1, branch2}, root.Hash)
			Expect(err).NotTo(HaveOccurred())

			branchPoints := dag.BranchPoints()
			Expect(branchPoints).To(HaveLen(1))
			Expect(branchPoints[0].Hash).To(Equal(root.Hash))
		})
	})

	Describe("LoadDag", func() {
		var driver *inmemory.Driver

		BeforeEach(func() {
			driver = inmemory.NewDriver()
		})

		It("loads a single node", func() {
			root := merkle.NewNode(dagTestBucket("user", "Hello"), nil)
			_, err := driver.Put(ctx, root)
			Expect(err).NotTo(HaveOccurred())

			dag, err := merkle.LoadDag(ctx, driver, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(1))
			Expect(dag.Root.Hash).To(Equal(root.Hash))
		})

		It("loads a linear chain", func() {
			root := merkle.NewNode(dagTestBucket("user", "1"), nil)
			child := merkle.NewNode(dagTestBucket("assistant", "2"), root)
			grandchild := merkle.NewNode(dagTestBucket("user", "3"), child)

			_, err := driver.Put(ctx, root)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, child)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, grandchild)
			Expect(err).NotTo(HaveOccurred())

			// Load from the middle node
			dag, err := merkle.LoadDag(ctx, driver, child.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(3))
			Expect(dag.Root.Hash).To(Equal(root.Hash))

			// Should have all nodes in correct structure
			Expect(dag.Get(root.Hash).Children).To(HaveLen(1))
			Expect(dag.Get(child.Hash).Children).To(HaveLen(1))
			Expect(dag.Get(grandchild.Hash).Children).To(HaveLen(0))
		})

		It("loads a branching tree", func() {
			//       root
			//      /    \
			//   child1  child2
			//     |
			//  grandchild
			root := merkle.NewNode(dagTestBucket("user", "root"), nil)
			child1 := merkle.NewNode(dagTestBucket("assistant", "child1"), root)
			child2 := merkle.NewNode(dagTestBucket("assistant", "child2"), root)
			grandchild := merkle.NewNode(dagTestBucket("user", "grandchild"), child1)

			_, err := driver.Put(ctx, root)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, child1)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, child2)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, grandchild)
			Expect(err).NotTo(HaveOccurred())

			// Load from root - should get all 4 nodes
			dag, err := merkle.LoadDag(ctx, driver, root.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(4))
			Expect(dag.Root.Children).To(HaveLen(2))
		})

		It("loads only relevant branch when starting from leaf", func() {
			//       root
			//      /    \
			//   child1  child2
			root := merkle.NewNode(dagTestBucket("user", "root"), nil)
			child1 := merkle.NewNode(dagTestBucket("assistant", "child1"), root)
			child2 := merkle.NewNode(dagTestBucket("assistant", "child2"), root)

			_, err := driver.Put(ctx, root)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, child1)
			Expect(err).NotTo(HaveOccurred())
			_, err = driver.Put(ctx, child2)
			Expect(err).NotTo(HaveOccurred())

			// Load from child1 - should get root + child1, but NOT child2
			// (child2 is not an ancestor or descendant of child1)
			dag, err := merkle.LoadDag(ctx, driver, child1.Hash)
			Expect(err).NotTo(HaveOccurred())
			Expect(dag.Size()).To(Equal(2))
			Expect(dag.Get(child2.Hash)).To(BeNil())
		})

		It("returns error for non-existent hash", func() {
			_, err := merkle.LoadDag(ctx, driver, "nonexistent")
			Expect(err).To(HaveOccurred())
		})
	})
})
