package derive

import (
	"strings"
	"unicode/utf8"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
)

var _ = Describe("joinTextBlocks truncation", func() {
	It("truncates on a rune boundary so previews stay valid UTF-8", func() {
		// 279 ASCII bytes followed by a 3-byte rune: a byte-indexed cut
		// at 280 would split the rune and Postgres would reject the
		// preview (SQLSTATE 22021).
		text := strings.Repeat("a", maxPreviewText-1) + "✓✓✓"
		blocks := []llm.ContentBlock{{Type: blockText, Text: text}}

		got := joinTextBlocks(blocks, false)
		Expect(len(got)).To(BeNumerically("<=", maxPreviewText))
		Expect(utf8.ValidString(got)).To(BeTrue())
		Expect(got).To(Equal(strings.Repeat("a", maxPreviewText-1)))
	})

	It("keeps short previews untouched", func() {
		blocks := []llm.ContentBlock{{Type: blockText, Text: "hello ✓"}}
		Expect(joinTextBlocks(blocks, false)).To(Equal("hello ✓"))
	})
})

var _ = Describe("Codex tool presentation", func() {
	It("labels one nested shell call and preserves its command", func() {
		name, input := displayTool("exec", map[string]any{
			"input": `const r = await tools.exec_command({cmd:"git status --short"}); text(r);`,
		})
		Expect(name).To(Equal("Bash"))
		Expect(input).To(HaveKeyWithValue("command", "git status --short"))
		Expect(input).NotTo(HaveKey("script"))
	})

	It("recognizes a skill load from its SKILL.md read", func() {
		name, input := displayTool("exec", map[string]any{
			"input": `const r = await tools.exec_command({"cmd":"sed -n '1,200p' /workspace/skills/exercise-codex/SKILL.md"}); text(r);`,
		})
		Expect(name).To(Equal("Skill"))
		Expect(input).To(HaveKeyWithValue("skill", "exercise-codex"))
	})

	It("summarizes real parallel calls without reading names from strings or comments", func() {
		name, input := displayTool("exec", map[string]any{
			"input": `const note = "tools.update_plan({})"; // tools.apply_patch("ignored")
const [a,b] = await Promise.all([tools.exec_command({cmd:"ls"}), tools.mcp__linear__list_issues({limit:3})]);`,
		})
		Expect(name).To(Equal("Parallel"))
		Expect(input).To(HaveKeyWithValue("description", "Bash + mcp__linear__list_issues"))
		Expect(input["calls"]).To(Equal([]any{
			map[string]any{"name": "Bash", "result_key": "a", "arguments": map[string]any{"command": "ls"}},
			map[string]any{"name": "mcp__linear__list_issues", "result_key": "b"},
		}))
	})

	It("finds tool calls in template expressions without scanning template text", func() {
		name, input := displayTool("exec", map[string]any{
			"input": "const label = `tools.update_plan({}): ${await tools.exec_command({cmd:\"ls\"})}`; text(label);",
		})
		Expect(name).To(Equal("Bash"))
		Expect(input).To(HaveKeyWithValue("command", "ls"))
	})

	It("only assigns Promise result keys to calls inside that Promise", func() {
		name, input := displayTool("exec", map[string]any{
			"input": `const before = await tools.update_plan({plan:[]});
const [files, issues] = await Promise.all([tools.exec_command({cmd:"ls"}), tools.mcp__linear__list_issues({limit:3})]);
text(JSON.stringify({before,files,issues}));`,
		})
		Expect(name).To(Equal("Parallel"))
		Expect(input["calls"]).To(Equal([]any{
			map[string]any{"name": "TaskPlan"},
			map[string]any{"name": "Bash", "result_key": "files", "arguments": map[string]any{"command": "ls"}},
			map[string]any{"name": "mcp__linear__list_issues", "result_key": "issues"},
		}))
	})

	It("extracts a returned Codex task identity", func() {
		Expect(codexAgentTaskName(`{"task_name":"/root/waddup_probe"}`)).To(Equal("/root/waddup_probe"))
		Expect(codexAgentTaskName("Script completed")).To(BeEmpty())
	})

	It("presents Codex collaboration calls like the existing Agent surface", func() {
		name, input := displayTool("spawn_agent", map[string]any{
			"task_name":  "nyt_headlines",
			"fork_turns": "all",
			"message":    "opaque encrypted prompt",
		})
		Expect(name).To(Equal("Agent"))
		Expect(input).To(HaveKeyWithValue("subagent_type", "Codex"))
		Expect(input).To(HaveKeyWithValue("description", "nyt_headlines"))
		Expect(input).NotTo(HaveKey("message"))
	})

	It("extracts Codex task-plan snapshots into structured presentation data", func() {
		name, input := displayTool("exec", map[string]any{
			"input": `const r = await tools.update_plan({plan:[{step:"Inspect traces",status:"in_progress"},{step:"Verify UI",status:"pending"}]}); text(r);`,
		})
		Expect(name).To(Equal("TaskPlan"))
		Expect(input["plan"]).To(Equal([]any{
			map[string]any{"step": "Inspect traces", "status": "in_progress"},
			map[string]any{"step": "Verify UI", "status": "pending"},
		}))
	})
})
