package skill_test

import (
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/skill"
)

var _ = Describe("Write", func() {
	It("writes a SKILL.md file to the correct path", func() {
		tmpDir := GinkgoT().TempDir()

		sk := &skill.Skill{
			Name:        "debug-react-hooks",
			Description: "Debug React hooks issues. Use when debugging useEffect loops.",
			Version:     "0.1.0",
			Tags:        []string{"react", "hooks", "debugging"},
			Type:        "workflow",
			Content:     "## Debug React Hooks\n\n1. Check dependency array\n2. Look for stale closures",
			Sessions:    []string{"sess-1", "sess-2"},
			CreatedAt:   time.Date(2026, 2, 17, 10, 0, 0, 0, time.UTC),
		}

		path, err := skill.Write(sk, tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(path).To(Equal(filepath.Join(tmpDir, "debug-react-hooks.md")))

		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())

		content := string(data)
		Expect(content).To(HavePrefix("---\n"))
		Expect(content).To(ContainSubstring("name: debug-react-hooks"))
		Expect(content).To(ContainSubstring("description: Debug React hooks issues"))
		Expect(content).To(ContainSubstring("version: 0.1.0"))
		Expect(content).To(ContainSubstring("tags: [react, hooks, debugging]"))
		Expect(content).To(ContainSubstring("type: workflow"))
		Expect(content).To(ContainSubstring("sessions: [sess-1, sess-2]"))
		Expect(content).To(ContainSubstring("## Debug React Hooks"))
		Expect(content).To(ContainSubstring("1. Check dependency array"))
	})
})

var _ = Describe("List", func() {
	It("lists skills from a directory", func() {
		tmpDir := GinkgoT().TempDir()

		// Write two skills
		sk1 := &skill.Skill{
			Name:        "skill-one",
			Description: "First skill",
			Version:     "0.1.0",
			Type:        "workflow",
			Content:     "## Steps\n\n1. Do thing one",
		}
		sk2 := &skill.Skill{
			Name:        "skill-two",
			Description: "Second skill",
			Version:     "0.2.0",
			Type:        "domain-knowledge",
			Content:     "## Knowledge\n\nSome domain knowledge",
		}

		_, err := skill.Write(sk1, tmpDir)
		Expect(err).NotTo(HaveOccurred())
		_, err = skill.Write(sk2, tmpDir)
		Expect(err).NotTo(HaveOccurred())

		skills, err := skill.List(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(skills).To(HaveLen(2))

		names := []string{skills[0].Name, skills[1].Name}
		Expect(names).To(ContainElements("skill-one", "skill-two"))
	})

	It("returns nil for non-existent directory", func() {
		skills, err := skill.List("/tmp/nonexistent-skill-dir-12345")
		Expect(err).NotTo(HaveOccurred())
		Expect(skills).To(BeNil())
	})
})

var _ = Describe("Sync", func() {
	It("copies a skill from source to target", func() {
		sourceDir := GinkgoT().TempDir()
		targetDir := GinkgoT().TempDir()

		sk := &skill.Skill{
			Name:        "test-skill",
			Description: "A test skill",
			Version:     "0.1.0",
			Type:        "workflow",
			Content:     "## Test\n\n1. Do the test",
		}

		_, err := skill.Write(sk, sourceDir)
		Expect(err).NotTo(HaveOccurred())

		path, err := skill.Sync("test-skill", sourceDir, targetDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(path).To(Equal(filepath.Join(targetDir, "test-skill.md")))

		// Verify the file was copied
		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(ContainSubstring("name: test-skill"))
		Expect(string(data)).To(ContainSubstring("## Test"))
	})

	It("returns error for non-existent skill", func() {
		sourceDir := GinkgoT().TempDir()
		targetDir := GinkgoT().TempDir()

		_, err := skill.Sync("nonexistent", sourceDir, targetDir)
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("ValidSkillType", func() {
	It("accepts valid skill types", func() {
		Expect(skill.ValidSkillType("workflow")).To(BeTrue())
		Expect(skill.ValidSkillType("domain-knowledge")).To(BeTrue())
		Expect(skill.ValidSkillType("prompt-template")).To(BeTrue())
	})

	It("rejects invalid skill types", func() {
		Expect(skill.ValidSkillType("invalid")).To(BeFalse())
		Expect(skill.ValidSkillType("")).To(BeFalse())
	})
})
