// Package storagetest provides shared Ginkgo specs that any storage.Driver
// implementation can run against. It is intended for use only from _test.go
// files of driver packages.
package storagetest

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// DriverFactory builds a fresh, migrated driver. Called once per spec via
// BeforeEach so each spec gets an empty store.
type DriverFactory func() storage.Driver

// RunListSessionsSpecs registers a Describe block exercising ListSessions and
// CountSessions against the driver returned by makeDriver. Call from a _test.go
// file inside a `var _ = ...` initializer (or from inside an existing Describe).
//
// The label is included in the Describe text so failures from multiple drivers
// can be told apart in test output.
func RunListSessionsSpecs(label string, makeDriver DriverFactory) bool {
	return ginkgo.Describe("ListSessions/CountSessions ["+label+"]", func() {
		var (
			ctx    context.Context
			driver storage.Driver
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			driver = makeDriver()
		})

		ginkgo.AfterEach(func() {
			if driver != nil {
				_ = driver.Close()
			}
		})

		ginkgo.Describe("empty store", func() {
			ginkgo.It("returns an empty page with no cursor", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(page.Items).To(gomega.BeEmpty())
				gomega.Expect(page.NextCursor).To(gomega.BeEmpty())
			})

			ginkgo.It("counts zero", func() {
				stats, err := driver.CountSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(stats).To(gomega.Equal(storage.SessionStats{}))
			})
		})

		ginkgo.Describe("with a small set of sessions", func() {
			var (
				baseTime time.Time
				root     *merkle.Node
				leafA    *merkle.Node // newest
				leafB    *merkle.Node
				leafC    *merkle.Node // oldest
			)

			ginkgo.BeforeEach(func() {
				baseTime = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

				// root has children, so it is NOT a leaf.
				root = makeNode("root", nil, baseTime, "tapes", "claude", "claude-opus-4-6", "anthropic")
				putWith(ctx, driver, root)

				// Three distinct branches off of root → three leaves.
				leafC = makeNode("leaf-c", &root.Hash, baseTime.Add(1*time.Minute), "tapes", "claude", "claude-opus-4-6", "anthropic")
				leafB = makeNode("leaf-b", &root.Hash, baseTime.Add(2*time.Minute), "other", "opencode", "claude-sonnet-4-6", "anthropic")
				leafA = makeNode("leaf-a", &root.Hash, baseTime.Add(3*time.Minute), "tapes", "claude", "gpt-4o", "openai")

				putWith(ctx, driver, leafC)
				putWith(ctx, driver, leafB)
				putWith(ctx, driver, leafA)
			})

			ginkgo.It("returns leaves only, ordered newest first", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.Equal([]string{leafA.Hash, leafB.Hash, leafC.Hash}))
				gomega.Expect(page.NextCursor).To(gomega.BeEmpty())
			})

			ginkgo.It("excludes internal nodes (root has children)", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, n := range page.Items {
					gomega.Expect(n.Hash).NotTo(gomega.Equal(root.Hash))
				}
			})

			ginkgo.It("paginates with limit and cursor without dropping or duplicating items", func() {
				page1, err := driver.ListSessions(ctx, storage.ListOpts{Limit: 2})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page1.Items)).To(gomega.Equal([]string{leafA.Hash, leafB.Hash}))
				gomega.Expect(page1.NextCursor).NotTo(gomega.BeEmpty())

				page2, err := driver.ListSessions(ctx, storage.ListOpts{Limit: 2, Cursor: page1.NextCursor})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page2.Items)).To(gomega.Equal([]string{leafC.Hash}))
				gomega.Expect(page2.NextCursor).To(gomega.BeEmpty())
			})

			ginkgo.It("filters by project", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{Project: "tapes"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafA.Hash, leafC.Hash))
			})

			ginkgo.It("filters by agent", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{Agent: "opencode"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafB.Hash))
			})

			ginkgo.It("filters by model", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{Model: "gpt-4o"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafA.Hash))
			})

			ginkgo.It("filters by provider", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{Provider: "anthropic"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafB.Hash, leafC.Hash))
			})

			ginkgo.It("filters by since (inclusive)", func() {
				since := baseTime.Add(2 * time.Minute)
				page, err := driver.ListSessions(ctx, storage.ListOpts{Since: &since})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafA.Hash, leafB.Hash))
			})

			ginkgo.It("filters by until (exclusive)", func() {
				until := baseTime.Add(2 * time.Minute)
				page, err := driver.ListSessions(ctx, storage.ListOpts{Until: &until})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// leafC at base+1m, leafB at base+2m (excluded), leafA at base+3m (excluded)
				// root is at base+0m but is not a leaf.
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafC.Hash))
			})

			ginkgo.It("combines filters with AND semantics", func() {
				page, err := driver.ListSessions(ctx, storage.ListOpts{
					Project:  "tapes",
					Provider: "anthropic",
				})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(hashesOf(page.Items)).To(gomega.ConsistOf(leafC.Hash))
			})

			ginkgo.It("CountSessions reports correct totals", func() {
				stats, err := driver.CountSessions(ctx, storage.ListOpts{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(stats.SessionCount).To(gomega.Equal(3)) // 3 leaves
				gomega.Expect(stats.TurnCount).To(gomega.Equal(4))    // root + 3 leaves
				gomega.Expect(stats.RootCount).To(gomega.Equal(1))
			})

			ginkgo.It("CountSessions respects filters", func() {
				stats, err := driver.CountSessions(ctx, storage.ListOpts{Project: "tapes"})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				// Project filter applies per-node:
				// - 3 nodes have project=tapes (root, leafA, leafC)
				// - 2 of those are leaves (leafA, leafC)
				// - 1 of those is a root
				gomega.Expect(stats.SessionCount).To(gomega.Equal(2))
				gomega.Expect(stats.TurnCount).To(gomega.Equal(3))
				gomega.Expect(stats.RootCount).To(gomega.Equal(1))
			})

			ginkgo.It("rejects an invalid cursor", func() {
				_, err := driver.ListSessions(ctx, storage.ListOpts{Cursor: "not-a-real-cursor!!!"})
				gomega.Expect(err).To(gomega.HaveOccurred())
			})
		})
	})
}

// makeNode constructs a merkle.Node with deterministic content keyed off id.
// CreatedAt is set explicitly so tests are not dependent on wall-clock granularity.
func makeNode(id string, parentHash *string, createdAt time.Time, project, agent, model, provider string) *merkle.Node {
	bucket := merkle.Bucket{
		Type:      "message",
		Role:      "user",
		Content:   []llm.ContentBlock{{Type: "text", Text: id}},
		Model:     model,
		Provider:  provider,
		AgentName: agent,
	}
	var parent *merkle.Node
	if parentHash != nil {
		// Synthesise a stand-in parent so NewNode can hash with the right link.
		parent = &merkle.Node{Hash: *parentHash}
	}
	n := merkle.NewNode(bucket, parent, merkle.NodeMeta{Project: project})
	n.CreatedAt = createdAt
	return n
}

func putWith(ctx context.Context, d storage.Driver, n *merkle.Node) {
	_, err := d.Put(ctx, n)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func hashesOf(nodes []*merkle.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Hash
	}
	return out
}
