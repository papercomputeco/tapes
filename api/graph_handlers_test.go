package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/merkle"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var _ = Describe("GET /v1/sessions/:hash/graph", func() {
	var (
		server *Server
		inMem  storage.Driver
		ctx    context.Context

		root       *merkle.Node
		child1     *merkle.Node
		child2     *merkle.Node
		grandchild *merkle.Node
	)

	BeforeEach(func() {
		logger := tapeslogger.NewNoop()
		inMem = inmemory.NewDriver()
		ctx = context.Background()

		var err error
		server, err = NewServer(Config{ListenAddr: ":0"}, inMem, logger)
		Expect(err).NotTo(HaveOccurred())

		baseTime := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)
		root = merkle.NewNode(v1TestBucket("user", "root prompt", "claude-sonnet", "anthropic", "claude"), nil, merkle.NodeOptions{Project: "tapes"})
		root.CreatedAt = baseTime
		child1 = merkle.NewNode(v1TestBucket("assistant", "first branch", "claude-sonnet", "anthropic", "claude"), root, merkle.NodeOptions{Project: "tapes"})
		child1.CreatedAt = baseTime.Add(time.Minute)
		child2 = merkle.NewNode(v1TestBucket("assistant", "second branch", "gpt-4o", "openai", "codex"), root, merkle.NodeOptions{Project: "tapes"})
		child2.CreatedAt = baseTime.Add(2 * time.Minute)
		grandchild = merkle.NewNode(v1TestBucket("user", "follow-up", "claude-sonnet", "anthropic", "claude"), child1, merkle.NodeOptions{Project: "tapes"})
		grandchild.CreatedAt = baseTime.Add(3 * time.Minute)

		Expect(putNode(ctx, inMem, root)).To(Succeed())
		Expect(putNode(ctx, inMem, child1)).To(Succeed())
		Expect(putNode(ctx, inMem, child2)).To(Succeed())
		Expect(putNode(ctx, inMem, grandchild)).To(Succeed())
	})

	It("defaults to the root scope so branch siblings are visible", func() {
		body := decodeGraph(server, sessionGraphPath(grandchild.Hash))

		Expect(body.Hash).To(Equal(grandchild.Hash))
		Expect(body.RootHash).To(Equal(root.Hash))
		Expect(body.Scope).To(Equal("root"))
		Expect(graphNodeIDs(body.Nodes)).To(ConsistOf(root.Hash, child1.Hash, child2.Hash, grandchild.Hash))
		Expect(body.Links).To(ConsistOf(
			GraphLink{Source: root.Hash, Target: child1.Hash},
			GraphLink{Source: root.Hash, Target: child2.Hash},
			GraphLink{Source: child1.Hash, Target: grandchild.Hash},
		))
		Expect(body.BranchPoints).To(ConsistOf(root.Hash))
		Expect(body.Leaves).To(ConsistOf(child2.Hash, grandchild.Hash))

		rootNode := graphNodeByID(body.Nodes, root.Hash)
		Expect(rootNode.IsRoot).To(BeTrue())
		Expect(rootNode.IsBranchPoint).To(BeTrue())
		Expect(rootNode.ChildrenCount).To(Equal(2))
		Expect(rootNode.ParentID).To(BeNil())

		selected := graphNodeByID(body.Nodes, grandchild.Hash)
		Expect(selected.Selected).To(BeTrue())
		Expect(selected.Preview).To(Equal("follow-up"))
		Expect(selected.Depth).To(Equal(2))
	})

	It("can return just the selected branch", func() {
		body := decodeGraph(server, sessionGraphPath(child1.Hash)+"?scope=branch")

		Expect(body.Scope).To(Equal("branch"))
		Expect(graphNodeIDs(body.Nodes)).To(ConsistOf(root.Hash, child1.Hash, grandchild.Hash))
		Expect(body.Links).To(ConsistOf(
			GraphLink{Source: root.Hash, Target: child1.Hash},
			GraphLink{Source: child1.Hash, Target: grandchild.Hash},
		))
		Expect(body.BranchPoints).To(ContainElement(root.Hash))
		Expect(graphNodeByID(body.Nodes, child1.Hash).Selected).To(BeTrue())
	})

	It("can return ancestry only", func() {
		body := decodeGraph(server, sessionGraphPath(grandchild.Hash)+"?scope=ancestry")

		Expect(body.Scope).To(Equal("ancestry"))
		Expect(graphNodeIDs(body.Nodes)).To(Equal([]string{root.Hash, child1.Hash, grandchild.Hash}))
		Expect(body.Links).To(Equal([]GraphLink{
			{Source: root.Hash, Target: child1.Hash},
			{Source: child1.Hash, Target: grandchild.Hash},
		}))
	})

	It("marks responses truncated when max_nodes is reached", func() {
		body := decodeGraph(server, sessionGraphPath(grandchild.Hash)+"?max_nodes=2")

		Expect(body.Truncated).To(BeTrue())
		Expect(body.NodeLimit).To(Equal(2))
		Expect(body.Nodes).To(HaveLen(2))
		Expect(body.Leaves).NotTo(BeNil())
	})

	It("serializes empty graph arrays as JSON arrays", func() {
		isolated := merkle.NewNode(v1TestBucket("user", "solo", "m", "p", "claude"), nil, merkle.NodeOptions{Project: "tapes"})
		Expect(putNode(ctx, inMem, isolated)).To(Succeed())

		body := decodeGraph(server, sessionGraphPath(isolated.Hash))
		Expect(body.Leaves).To(Equal([]string{isolated.Hash}))
		Expect(body.BranchPoints).To(Equal([]string{}))
	})

	It("prioritizes the requested ancestry path before sibling branches when node-limited", func() {
		earlySibling := merkle.NewNode(v1TestBucket("assistant", "early sibling", "m", "p", "claude"), root, merkle.NodeOptions{Project: "tapes"})
		earlySibling.CreatedAt = root.CreatedAt.Add(-time.Minute)
		Expect(putNode(ctx, inMem, earlySibling)).To(Succeed())

		body := decodeGraph(server, sessionGraphPath(grandchild.Hash)+"?max_nodes=3")
		Expect(body.Truncated).To(BeTrue())
		Expect(graphNodeIDs(body.Nodes)).To(ConsistOf(root.Hash, child1.Hash, grandchild.Hash))
		Expect(graphNodeByID(body.Nodes, grandchild.Hash).Selected).To(BeTrue())
	})

	It("treats the top resolvable node as the visual root for incomplete ancestry", func() {
		phantomParent := "ffff000000000000000000000000000000000000000000000000000000000000"
		orphan := merkle.NewNode(v1TestBucket("user", "trimmed history", "m", "p", "claude"), &merkle.Node{Hash: phantomParent}, merkle.NodeOptions{Project: "tapes"})
		orphanChild := merkle.NewNode(v1TestBucket("assistant", "still visible", "m", "p", "claude"), orphan, merkle.NodeOptions{Project: "tapes"})
		Expect(putNode(ctx, inMem, orphan)).To(Succeed())
		Expect(putNode(ctx, inMem, orphanChild)).To(Succeed())

		body := decodeGraph(server, sessionGraphPath(orphanChild.Hash))
		Expect(body.Truncated).To(BeTrue())
		Expect(body.MissingParent).To(Equal(phantomParent))
		Expect(body.RootHash).To(Equal(orphan.Hash))
		Expect(graphNodeIDs(body.Nodes)).To(ConsistOf(orphan.Hash, orphanChild.Hash))

		visualRoot := graphNodeByID(body.Nodes, orphan.Hash)
		Expect(visualRoot.IsRoot).To(BeTrue())
		Expect(visualRoot.ParentID).To(BeNil())
		Expect(visualRoot.ParentHash).NotTo(BeNil())
		Expect(*visualRoot.ParentHash).To(Equal(phantomParent))
	})

	It("returns 404 for an unknown hash", func() {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, sessionGraphPath("does-not-exist"), nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusNotFound))
	})

	It("returns 400 for invalid query parameters", func() {
		for _, path := range []string{
			sessionGraphPath(grandchild.Hash) + "?scope=everything",
			sessionGraphPath(grandchild.Hash) + "?max_nodes=0",
			sessionGraphPath(grandchild.Hash) + "?max_nodes=5001",
		} {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusBadRequest))
		}
	})
})

var _ = Describe("minimal web UI", func() {
	var server *Server

	BeforeEach(func() {
		var err error
		server, err = NewServer(Config{ListenAddr: ":0", EnableWebUI: true}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
	})

	It("does not serve the UI unless explicitly enabled", func() {
		defaultServer, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := defaultServer.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusNotFound))
	})

	It("serves the D3 UI from / without a frontend build", func() {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("text/html"))

		raw, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).To(ContainSubstring("d3@7.9.0"))
		Expect(string(raw)).To(ContainSubstring("integrity=\"sha256-1AqCL/P1MtPZ9HQLjuNAO+QOIV1Q7AhiAl2fT14nSI=\""))
		Expect(string(raw)).To(ContainSubstring("/v1/sessions/"))
		Expect(string(raw)).To(ContainSubstring("/v1/sessions/summary"))
		Expect(string(raw)).NotTo(ContainSubstring("/v1/dags/"))
	})

	It("does not catch all unknown routes", func() {
		for _, path := range []string{"/graph", "/not-the-ui"} {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
			Expect(err).NotTo(HaveOccurred())
			resp, err := server.app.Test(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(fiber.StatusNotFound))
		}
	})
})

func sessionGraphPath(hash string) string {
	return "/v1/sessions/" + hash + "/graph"
}

func decodeGraph(server *Server, path string) GraphResponse {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	var body GraphResponse
	Expect(json.Unmarshal(raw, &body)).To(Succeed(), strings.TrimSpace(string(raw)))
	return body
}

func graphNodeIDs(nodes []GraphNode) []string {
	ids := make([]string, len(nodes))
	for i, node := range nodes {
		ids[i] = node.ID
	}
	return ids
}

func graphNodeByID(nodes []GraphNode, hash string) GraphNode {
	for _, node := range nodes {
		if node.ID == hash {
			return node
		}
	}
	Fail("node not found in graph response: " + hash)
	return GraphNode{}
}
