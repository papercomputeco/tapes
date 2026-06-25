package storage

import (
	"errors"
	"time"
)

// ErrSkillVersionConflict is returned by CreateSkillVersion when the
// (skill_id, version_number) pair already exists — i.e. a concurrent publish
// claimed the same number. Callers translate it into a retry rather than a 500.
var ErrSkillVersionConflict = errors.New("skill version number already exists")

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

// SkillListOpts controls a single keyset page of skills. Query, Author and
// NotAuthor are all optional filters; an empty string disables each.
type SkillListOpts struct {
	Query     string // name/description/tag search (empty = no filter)
	Author    string // only skills authored by this subject ("mine")
	NotAuthor string // exclude this subject ("team")
	// Sort selects the ordering and which cursor column applies:
	// "downloads" orders by download_count DESC (keyset on CursorDownloads);
	// anything else orders by updated_at DESC (keyset on CursorTs). Both
	// tiebreak on id DESC.
	Sort            string
	CursorTs        *time.Time // recent keyset: updated_at of the prior page's last row
	CursorDownloads *int64     // downloads keyset: download_count of that row
	CursorID        string     // keyset tiebreak: id of that last row
	Limit           int        // page size; zero falls back to DefaultListLimit
}

// SkillSortDownloads is the SkillListOpts.Sort value for most-downloaded order.
const SkillSortDownloads = "downloads"

// SkillCounts are the per-tab totals for a search: every matching skill, and
// how many the caller authored. "team" is derived as Total - Mine.
type SkillCounts struct {
	Total int64
	Mine  int64
}
