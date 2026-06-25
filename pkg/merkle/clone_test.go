package merkle_test

import (
	"strings"
	"unsafe"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/merkle"
)

func stringAliases(a, b string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return unsafe.StringData(a) == unsafe.StringData(b)
}

var _ = Describe("Node.CloneRetained", func() {
	// buf stands in for a raw request buffer; the node's content fields
	// are sub-slices of it, as zero-copy parsing produces.
	var buf string
	var node *merkle.Node
	var params *llm.RequestParams

	BeforeEach(func() {
		buf = "ROLEuser" + "TEXTwhat is the capital of France?" + "KINDmain" + "THREADagent-7"
		sub := func(marker, val string) string {
			i := strings.Index(buf, marker) + len(marker)
			return buf[i : i+len(val)]
		}
		bucket := merkle.Bucket{
			Type:    "message",
			Role:    sub("ROLE", "user"),
			Content: []llm.ContentBlock{{Type: "text", Text: sub("TEXT", "what is the capital of France?")}},
			Model:   "claude-opus-4-8",
		}
		node = merkle.NewNode(bucket, nil)
		node.Kind = sub("KIND", "main")
		node.ThreadID = sub("THREAD", "agent-7")
		sys := "SYS" + strings.Repeat("s", 64)
		params = &llm.RequestParams{System: sys[3:]}
		node.Request = params
	})

	It("keeps node.Hash unchanged — cloning is pure", func() {
		before := node.Hash
		node.CloneRetained(params.Clone())
		Expect(node.Hash).To(Equal(before))
		// A node built fresh from the cloned bucket hashes the same: the
		// content bytes are identical, only the backing array differs.
		Expect(merkle.NewNode(node.Bucket, nil).Hash).To(Equal(before))
	})

	It("breaks every alias to the raw buffer", func() {
		node.CloneRetained(params.Clone())
		Expect(stringAliases(node.Bucket.Role, buf)).To(BeFalse())
		Expect(stringAliases(node.Bucket.Content[0].Text, buf)).To(BeFalse())
		Expect(stringAliases(node.Kind, buf)).To(BeFalse())
		Expect(stringAliases(node.ThreadID, buf)).To(BeFalse())
	})

	It("preserves content and structure byte-for-byte", func() {
		node.CloneRetained(params.Clone())
		Expect(node.Bucket.Role).To(Equal("user"))
		Expect(node.Bucket.Content[0].Text).To(Equal("what is the capital of France?"))
		Expect(node.Kind).To(Equal("main"))
		Expect(node.ThreadID).To(Equal("agent-7"))
	})

	It("clones ParentHash into a fresh pointer", func() {
		child := merkle.NewNode(node.Bucket, node)
		Expect(child.ParentHash).NotTo(BeNil())
		orig := child.ParentHash
		child.CloneRetained(params.Clone())
		Expect(child.ParentHash).NotTo(BeIdenticalTo(orig))
		Expect(*child.ParentHash).To(Equal(node.Hash))
	})

	It("installs the provided request params", func() {
		cloned := params.Clone()
		node.CloneRetained(cloned)
		Expect(node.Request).To(BeIdenticalTo(cloned))
		Expect(node.Request.System).To(Equal(params.System))
		Expect(stringAliases(node.Request.System, params.System)).To(BeFalse())
	})
})
