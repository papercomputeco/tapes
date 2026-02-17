package skill

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Write persists a Skill as a SKILL.md file at <dir>/<name>/SKILL.md.
func Write(sk *Skill, dir string) (string, error) {
	skillDir := filepath.Join(dir, sk.Name)
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		return "", fmt.Errorf("create skill directory: %w", err)
	}

	path := filepath.Join(skillDir, "SKILL.md")
	content := renderSkillMD(sk)

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return "", fmt.Errorf("write SKILL.md: %w", err)
	}

	return path, nil
}

// List scans a directory for skills and returns summaries.
func List(dir string) ([]*Skill, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read skills directory: %w", err)
	}

	var skills []*Skill
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		skillPath := filepath.Join(dir, entry.Name(), "SKILL.md")
		data, err := os.ReadFile(skillPath)
		if err != nil {
			continue // skip directories without SKILL.md
		}

		sk, err := parseSkillMD(string(data))
		if err != nil {
			continue
		}
		sk.Name = entry.Name()
		skills = append(skills, sk)
	}

	return skills, nil
}

// Sync copies a skill directory from source to target.
func Sync(name, sourceDir, targetDir string) (string, error) {
	srcPath := filepath.Join(sourceDir, name, "SKILL.md")
	data, err := os.ReadFile(srcPath)
	if err != nil {
		return "", fmt.Errorf("read source skill: %w", err)
	}

	dstDir := filepath.Join(targetDir, name)
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return "", fmt.Errorf("create target directory: %w", err)
	}

	dstPath := filepath.Join(dstDir, "SKILL.md")
	if err := os.WriteFile(dstPath, data, 0o600); err != nil {
		return "", fmt.Errorf("write target skill: %w", err)
	}

	return dstPath, nil
}

// SkillsDir returns the default skills directory (~/.tapes/skills).
func SkillsDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".tapes", "skills"), nil
}

// GlobalClaudeSkillsDir returns ~/.claude/skills/.
func GlobalClaudeSkillsDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".claude", "skills"), nil
}

// LocalClaudeSkillsDir returns .claude/skills/ relative to the current directory.
func LocalClaudeSkillsDir() string {
	return filepath.Join(".claude", "skills")
}

func renderSkillMD(sk *Skill) string {
	var b strings.Builder

	b.WriteString("---\n")
	fmt.Fprintf(&b, "name: %s\n", sk.Name)
	fmt.Fprintf(&b, "description: %s\n", sk.Description)
	fmt.Fprintf(&b, "version: %s\n", sk.Version)
	if len(sk.Tags) > 0 {
		fmt.Fprintf(&b, "tags: [%s]\n", strings.Join(sk.Tags, ", "))
	}
	if sk.Type != "" {
		fmt.Fprintf(&b, "type: %s\n", sk.Type)
	}
	if len(sk.Sessions) > 0 {
		fmt.Fprintf(&b, "sessions: [%s]\n", strings.Join(sk.Sessions, ", "))
	}
	if !sk.CreatedAt.IsZero() {
		fmt.Fprintf(&b, "created_at: %s\n", sk.CreatedAt.Format(time.RFC3339))
	}
	b.WriteString("---\n\n")
	b.WriteString(sk.Content)

	// Ensure trailing newline
	if !strings.HasSuffix(sk.Content, "\n") {
		b.WriteString("\n")
	}

	return b.String()
}

func parseSkillMD(content string) (*Skill, error) {
	// Split frontmatter from body
	if !strings.HasPrefix(content, "---\n") {
		return nil, errors.New("missing frontmatter delimiter")
	}

	rest := content[4:] // skip opening "---\n"
	before, after, ok := strings.Cut(rest, "\n---\n")
	if !ok {
		return nil, errors.New("missing closing frontmatter delimiter")
	}

	frontmatter := before
	body := strings.TrimSpace(after) // skip "\n---\n"

	sk := &Skill{
		Content: body,
		Version: "0.1.0",
	}

	for line := range strings.SplitSeq(frontmatter, "\n") {
		key, value, ok := strings.Cut(line, ": ")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		switch key {
		case "name":
			sk.Name = value
		case "description":
			sk.Description = value
		case "version":
			sk.Version = value
		case "type":
			sk.Type = value
		case "tags":
			sk.Tags = parseBracketList(value)
		case "sessions":
			sk.Sessions = parseBracketList(value)
		case "created_at":
			if t, err := time.Parse(time.RFC3339, value); err == nil {
				sk.CreatedAt = t
			}
		}
	}

	return sk, nil
}

func parseBracketList(s string) []string {
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
