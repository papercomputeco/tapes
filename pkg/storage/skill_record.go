package storage

import "time"

// SkillRecord is the flat skills-table row surfaced by the /v1/skills API. It
// mirrors the console's Skill shape so a generated skill round-trips through
// persistence without a separate projection. Fields absent in the DB (parent_id
// NULL) are represented as empty strings so API callers never unwrap optional
// pgtype wrappers.
type SkillRecord struct {
	// ID is the opaque, immutable identity (the route/URL key), mirroring
	// sessions. Slug is a cosmetic, non-unique display label and SKILL.md
	// filename derived from the name.
	ID                      string
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
	ParentID                string // empty when not a fork
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
	SkillID       string
	VersionNumber int
	Semver        string
	Changelog     string
	Content       string
	AuthorSubject string
	PublishedAt   time.Time
}

// SkillListOpts controls a single keyset page of skills (newest-edited first).
// A nil CursorTs means "first page". Query, Author and NotAuthor are all
// optional filters; an empty string disables each.
type SkillListOpts struct {
	Query     string     // name/description/tag search (empty = no filter)
	Author    string     // only skills authored by this subject ("mine")
	NotAuthor string     // exclude this subject ("team")
	CursorTs  *time.Time // keyset: updated_at of the last row on the prior page
	CursorID  string     // keyset tiebreak: id of that last row
	Limit     int        // page size; zero falls back to DefaultListLimit
}

// SkillCounts are the per-tab totals for a search: every matching skill, and
// how many the caller authored. "team" is derived as Total - Mine.
type SkillCounts struct {
	Total int64
	Mine  int64
}
