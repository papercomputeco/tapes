package storage

import "time"

// SkillRecord is the flat skills-table row surfaced by the /v1/skills API. It
// mirrors the console's SkillDraft shape so a generated skill round-trips
// through persistence without a separate projection. Fields absent in the DB
// (parent_slug NULL) are represented as empty strings so API callers never
// unwrap optional pgtype wrappers.
type SkillRecord struct {
	Slug                    string
	Name                    string
	Description             string
	Type                    string // "workflow" | "domain-knowledge" | "prompt-template"
	Version                 string // semver, e.g. "0.1.0"
	Visibility              string // "private" | "team"
	Tags                    []string
	Content                 string // markdown body
	IsAIGenerated           bool
	GeneratedFromSessionIDs []string
	ParentSlug              string // empty when not a fork
	CreatedAt               time.Time
	UpdatedAt               time.Time
}
