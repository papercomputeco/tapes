package deck

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage/ent"
)

var _ = Describe("Session labels", func() {
	It("builds labels from the most recent user prompts", func() {
		lineOne := "Investigate session titles"
		lineTwo := "Add label logic"
		lineThree := "Write label test"

		nodes := []*ent.Node{
			{
				ID:   "node-1",
				Role: "user",
				Content: []map[string]any{{
					"text": "checkout main and pull latest",
					"type": "text",
				}},
			},
			{ID: "node-2", Role: "assistant"},
			{
				ID:   "node-3",
				Role: "user",
				Content: []map[string]any{{
					"text": lineOne,
					"type": "text",
				}},
			},
			{
				ID:   "node-4",
				Role: "user",
				Content: []map[string]any{{
					"text": "Command: git checkout main && git pull",
					"type": "text",
				}},
			},
			{
				ID:   "node-5",
				Role: "user",
				Content: []map[string]any{{
					"text": lineTwo,
					"type": "text",
				}},
			},
			{ID: "node-6", Role: "assistant"},
			{
				ID:   "node-7",
				Role: "user",
				Content: []map[string]any{{
					"text": lineThree,
					"type": "text",
				}},
			},
			{ID: "node-8", Role: "assistant"},
		}

		expected := truncate(strings.Join([]string{lineOne, lineTwo, lineThree}, " / "), 36)
		label := buildLabel(nodes)

		Expect(label).To(Equal(expected))
		Expect(label).NotTo(ContainSubstring("checkout main"))
		Expect(label).NotTo(ContainSubstring("Command:"))
	})
})
