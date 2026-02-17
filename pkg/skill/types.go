// Package skill provides LLM-powered extraction of reusable patterns from
// tapes sessions, outputting Claude Code SKILL.md files.
package skill

import (
	"slices"
	"time"
)

// Skill represents a Claude Code skill extracted from session data.
type Skill struct {
	Name        string    `json:"name"`        // kebab-case identifier
	Description string    `json:"description"` // trigger description for Claude
	Version     string    `json:"version"`     // semver, default "0.1.0"
	Tags        []string  `json:"tags"`        // e.g. ["debugging", "react"]
	Type        string    `json:"type"`        // "workflow", "domain-knowledge", "prompt-template"
	Content     string    `json:"content"`     // markdown body (instructions)
	Sessions    []string  `json:"sessions"`    // source session IDs
	CreatedAt   time.Time `json:"created_at"`
}

// SkillTypes enumerates valid skill type values.
var SkillTypes = []string{"workflow", "domain-knowledge", "prompt-template"}

// ValidSkillType returns true if the given type is a recognized skill type.
func ValidSkillType(t string) bool {
	return slices.Contains(SkillTypes, t)
}
