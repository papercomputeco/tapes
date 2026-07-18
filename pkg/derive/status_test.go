package derive_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/sessions"
)

func assistantSpan(stopReason, text string) *derive.Span {
	return &derive.Span{StopReason: stopReason, Output: blocks(text)}
}

func gitToolSpan(command string) *derive.Span {
	return &derive.Span{
		Kind:   derive.SpanKindTool,
		Input:  []llm.ContentBlock{{Type: "tool_use", ToolName: "Bash", ToolInput: map[string]any{"command": command}}},
		Output: blocks("ok"),
	}
}

var _ = Describe("FoldSessionStatus", func() {
	It("is completed for an assistant terminal with a terminal stop_reason", func() {
		st := derive.FoldSessionStatus(nil, assistantSpan("end_turn", "done"))
		Expect(st.DerivedStatus).To(Equal(sessions.StatusCompleted))
		Expect(st.ToolResultCount).To(Equal(0))
		Expect(st.HasGitActivity).To(BeFalse())
	})

	It("is completed via git activity even when the terminal stop_reason is not terminal", func() {
		st := derive.FoldSessionStatus([]*derive.Span{gitToolSpan("git commit -m wip")}, assistantSpan("", "…"))
		Expect(st.HasGitActivity).To(BeTrue())
		Expect(st.DerivedStatus).To(Equal(sessions.StatusCompleted))
		Expect(st.ToolResultCount).To(Equal(1))
	})

	It("is failed when tool errors dominate the results", func() {
		errTool := &derive.Span{Kind: derive.SpanKindTool, Status: "error", Output: blocks("boom")}
		st := derive.FoldSessionStatus([]*derive.Span{errTool}, assistantSpan("tool_use", "k"))
		Expect(st.ToolResultCount).To(Equal(1))
		Expect(st.ToolErrorCount).To(Equal(1))
		Expect(st.DerivedStatus).To(Equal(sessions.StatusFailed))
	})

	It("counts tool results and errors across the session's tool spans", func() {
		ok1 := &derive.Span{Kind: derive.SpanKindTool, Output: blocks("a")}
		ok2 := &derive.Span{Kind: derive.SpanKindTool, Output: blocks("b")}
		err1 := &derive.Span{Kind: derive.SpanKindTool, Status: "error", Output: blocks("x")}
		st := derive.FoldSessionStatus([]*derive.Span{ok1, ok2, err1}, assistantSpan("end_turn", "done"))
		Expect(st.ToolResultCount).To(Equal(3))
		Expect(st.ToolErrorCount).To(Equal(1))
		// one error in three results is normal agentic behaviour, not failure.
		Expect(st.DerivedStatus).To(Equal(sessions.StatusCompleted))
	})

	It("is unknown when the session has no terminal response", func() {
		Expect(derive.FoldSessionStatus(nil, nil).DerivedStatus).To(Equal(sessions.StatusUnknown))
	})
})
