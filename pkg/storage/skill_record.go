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
	// AuthorSubject is the WorkOS user id (JWT sub) of the creator, stamped
	// from the gateway-trusted x-paper-auth-subject header the same way
	// sessions.auth_subject is captured. Empty when no header was present.
	AuthorSubject string
	// DownloadCount is a real usage signal — how many times the SKILL.md has
	// been downloaded.
	DownloadCount int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// SkillVersionRecord is one immutable published snapshot of a skill's content.
// The skill's working/current content lives on SkillRecord.Content; versions
// are history only.
type SkillVersionRecord struct {
	SkillSlug     string
	VersionNumber int
	Semver        string
	Changelog     string
	Content       string
	AuthorSubject string
	PublishedAt   time.Time
}
