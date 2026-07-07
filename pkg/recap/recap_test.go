package recap_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/recap"
	"github.com/papercomputeco/tapes/pkg/skill"
)

// mockQuerier implements skill.Querier, mirroring pkg/skill's test double.
type mockQuerier struct {
	summaries map[string][]skill.TraceSummary
	traces    map[string]*skill.Trace
}

func (m *mockQuerier) TraceSummaries(_ context.Context, sessionID string) ([]skill.TraceSummary, error) {
	turns, ok := m.summaries[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %s not found", sessionID)
	}
	return turns, nil
}

func (m *mockQuerier) Trace(_ context.Context, traceID string) (*skill.Trace, error) {
	trace, ok := m.traces[traceID]
	if !ok {
		return nil, fmt.Errorf("trace %s not found", traceID)
	}
	return trace, nil
}

func mainSpan(seq int64, text string) skill.Span {
	return skill.Span{
		Kind:     "llm",
		CallKind: "main",
		Seq:      seq,
		Output:   []llm.ContentBlock{{Type: "text", Text: text}},
	}
}

func sessionQuerier() *mockQuerier {
	return &mockQuerier{
		summaries: map[string][]skill.TraceSummary{
			"sess-1": {
				{TraceID: "t1", UserPrompt: "Fix the flaky login test", StartedAt: time.Now()},
			},
		},
		traces: map[string]*skill.Trace{
			"t1": {TraceID: "t1", Spans: []skill.Span{
				mainSpan(1, "Found a race in the session fixture; pinning the clock."),
			}},
		},
	}
}

const validRecapJSON = `{
	"narrative": "The user set out to fix a flaky login test. The agent traced it to a race in the session fixture and pinned the clock.",
	"observation": "Flaky login tests in this repo tend to trace back to unpinned clocks in fixtures."
}`

var _ = Describe("Generate", func() {
	It("extracts narrative and observation from a session", func() {
		mockLLM := func(_ context.Context, _ string) (string, error) {
			return validRecapJSON, nil
		}
		r, err := recap.Generate(context.Background(), sessionQuerier(), mockLLM, "sess-1", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(r.Narrative).To(ContainSubstring("flaky login test"))
		Expect(r.Observation).To(ContainSubstring("unpinned clocks"))
	})

	It("feeds the transcript and past tense into the prompt when settled", func() {
		var captured string
		captureLLM := func(_ context.Context, prompt string) (string, error) {
			captured = prompt
			return validRecapJSON, nil
		}
		_, err := recap.Generate(context.Background(), sessionQuerier(), captureLLM, "sess-1", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(captured).To(ContainSubstring("Fix the flaky login test"))
		Expect(captured).To(ContainSubstring("session has ENDED"))
		Expect(captured).To(ContainSubstring("past tense"))
		// The narrative must center the author's intent, not agent activity.
		Expect(captured).To(ContainSubstring("what the AUTHOR is trying to accomplish"))
		Expect(captured).To(ContainSubstring("Do not narrate the agent's activity"))
	})

	It("asks for present tense when the session is live", func() {
		var captured string
		captureLLM := func(_ context.Context, prompt string) (string, error) {
			captured = prompt
			return validRecapJSON, nil
		}
		_, err := recap.Generate(context.Background(), sessionQuerier(), captureLLM, "sess-1", true)
		Expect(err).NotTo(HaveOccurred())
		Expect(captured).To(ContainSubstring("STILL RUNNING"))
		Expect(captured).To(ContainSubstring("present tense"))
	})

	It("tolerates an absent observation", func() {
		mockLLM := func(_ context.Context, _ string) (string, error) {
			return `{"narrative": "Did the thing.", "observation": ""}`, nil
		}
		r, err := recap.Generate(context.Background(), sessionQuerier(), mockLLM, "sess-1", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(r.Observation).To(BeEmpty())
	})

	It("retries on malformed JSON, appending the strictness nudge", func() {
		calls := 0
		flakyLLM := func(_ context.Context, prompt string) (string, error) {
			calls++
			if calls == 1 {
				Expect(prompt).NotTo(ContainSubstring("Return ONLY valid JSON, no markdown."))
				return "sorry, here's prose instead of JSON", nil
			}
			Expect(prompt).To(ContainSubstring("Return ONLY valid JSON, no markdown."))
			return validRecapJSON, nil
		}
		r, err := recap.Generate(context.Background(), sessionQuerier(), flakyLLM, "sess-1", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(calls).To(Equal(2))
		Expect(r.Narrative).NotTo(BeEmpty())
	})

	It("rejects a recap whose narrative is empty after all retries", func() {
		mockLLM := func(_ context.Context, _ string) (string, error) {
			return `{"narrative": "", "observation": "x"}`, nil
		}
		_, err := recap.Generate(context.Background(), sessionQuerier(), mockLLM, "sess-1", false)
		Expect(err).To(MatchError(ContainSubstring("narrative is empty")))
	})

	It("propagates transcript errors without calling the LLM", func() {
		called := false
		mockLLM := func(_ context.Context, _ string) (string, error) {
			called = true
			return validRecapJSON, nil
		}
		_, err := recap.Generate(context.Background(), sessionQuerier(), mockLLM, "sess-unknown", false)
		Expect(err).To(HaveOccurred())
		Expect(called).To(BeFalse())
	})

	It("strips markdown fences around the JSON", func() {
		mockLLM := func(_ context.Context, _ string) (string, error) {
			return "```json\n" + validRecapJSON + "\n```", nil
		}
		r, err := recap.Generate(context.Background(), sessionQuerier(), mockLLM, "sess-1", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(r.Narrative).To(ContainSubstring("flaky login test"))
	})

	It("elides the middle of an oversized transcript, keeping head and tail", func() {
		q := sessionQuerier()
		// One giant spine response pushes the transcript far past the cap.
		q.traces["t1"].Spans = []skill.Span{
			mainSpan(1, "GOAL-MARKER "+strings.Repeat("x", 60000)+" OUTCOME-MARKER"),
		}
		var captured string
		captureLLM := func(_ context.Context, prompt string) (string, error) {
			captured = prompt
			return validRecapJSON, nil
		}
		_, err := recap.Generate(context.Background(), q, captureLLM, "sess-1", false)
		Expect(err).NotTo(HaveOccurred())
		Expect(captured).To(ContainSubstring("GOAL-MARKER"))
		Expect(captured).To(ContainSubstring("OUTCOME-MARKER"))
		Expect(captured).To(ContainSubstring("[... transcript elided ...]"))
	})
})
