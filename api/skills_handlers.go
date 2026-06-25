package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/skill"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// skillStore is the capability interface the skills API needs from the
// storage layer. The Postgres driver implements it; the handlers return 501
// for drivers (e.g. the in-memory node store) that don't. Skills are keyed on
// an opaque id (the route key, mirroring sessions); slug is a cosmetic label.
type skillStore interface {
	UpsertSkill(ctx context.Context, orgID string, rec storage.SkillRecord) (*storage.SkillRecord, error)
	GetSkill(ctx context.Context, orgID, id string) (*storage.SkillRecord, error)
	ListSkills(ctx context.Context, orgID string, opts storage.SkillListOpts) ([]storage.SkillRecord, error)
	ListSkillsBySession(ctx context.Context, orgID, sessionID string) ([]storage.SkillRecord, error)
	CountSkills(ctx context.Context, orgID, query, author string) (storage.SkillCounts, error)
	NextSkillVersionNumber(ctx context.Context, orgID, skillID string) (int, error)
	CreateSkillVersion(ctx context.Context, orgID string, rec storage.SkillVersionRecord) (*storage.SkillVersionRecord, error)
	SetSkillVersion(ctx context.Context, orgID, skillID, semver string, updatedAt time.Time) error
	ListSkillVersions(ctx context.Context, orgID, skillID string) ([]storage.SkillVersionRecord, error)
	IncrementSkillDownloads(ctx context.Context, orgID, id string) error
	DeleteSkill(ctx context.Context, orgID, id string) (bool, error)
}

const (
	defaultSkillsLimit = 24
	maxSkillsLimit     = 100
)

// skillsCursor is the opaque keyset cursor for the skills list. It carries the
// last row's id plus both possible sort keys (updated_at and download_count);
// the active sort decides which one the next page filters on. Same base64(JSON)
// encoding sessions use. The console resets the cursor when the sort changes, so
// a cursor is only ever decoded under the sort that produced it.
type skillsCursor struct {
	UpdatedAt time.Time `json:"ts"`
	Downloads int64     `json:"dc"`
	ID        string    `json:"id"`
}

func encodeSkillsCursor(c skillsCursor) string {
	b, err := json.Marshal(c)
	if err != nil {
		panic(fmt.Sprintf("encoding skills cursor: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeSkillsCursor(token string) (skillsCursor, error) {
	if token == "" {
		return skillsCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return skillsCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	var c skillsCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return skillsCursor{}, fmt.Errorf("invalid cursor: %w", err)
	}
	if c.ID == "" {
		return skillsCursor{}, errors.New("invalid cursor: missing id")
	}
	return c, nil
}

// authSubjectHeader carries the WorkOS user id (JWT sub) — the same
// gateway-trusted header ingest reads onto sessions.auth_subject. We trust it
// the same way: the edge gateway stamps it from a validated JWT (and strips
// any client-sent value); in the local clearing the console sets it directly
// since it reaches the data-plane sidecar without the gateway in path.
const authSubjectHeader = "x-paper-auth-subject"

func authSubjectFromCtx(c *fiber.Ctx) string {
	return strings.TrimSpace(c.Get(authSubjectHeader))
}

// skillStoreOr501 type-asserts the driver to skillStore, writing a 501 and
// returning ok=false when the backend doesn't support skills.
func (s *Server) skillStoreOr501(c *fiber.Ctx) (skillStore, bool) {
	store, ok := s.driver.(skillStore)
	if !ok {
		_ = c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "skills not supported by this backend"})
		return nil, false
	}
	return store, true
}

// generateSkillRequest is the POST /v1/skills/generate body. It mirrors the
// console's GenerateSkillInput: the client nominates source sessions plus
// optional hints, and the server is authoritative on the skill body. Wire
// shape is camelCase to match the console's skills schemas (which predate and
// diverge from the snake_case convention the rest of tapes uses).
type generateSkillRequest struct {
	SessionIDs []string `json:"sessionIds"`
	Hint       *struct {
		Name        string   `json:"name"`
		Description string   `json:"description"`
		Type        string   `json:"type"`
		Tags        []string `json:"tags"`
	} `json:"hint"`
}

// skillResponse is the unified Skill shape the console expects (camelCase). id
// is the opaque identity / route key; slug is a cosmetic display label. content
// always lives on the skill row (versions are history only); parentId is null
// unless the skill is a duplicate/fork.
type skillResponse struct {
	ID                    string   `json:"id"`
	Slug                  string   `json:"slug"`
	ParentID              *string  `json:"parentId"`
	Name                  string   `json:"name"`
	Description           string   `json:"description"`
	Type                  string   `json:"type"`
	Version               string   `json:"version"`
	Visibility            string   `json:"visibility"`
	Tags                  []string `json:"tags"`
	Content               string   `json:"content"`
	IsAIGenerated         bool     `json:"isAiGenerated"`
	OriginatingSessionIDs []string `json:"originatingSessionIds"`
	AuthorID              string   `json:"authorId"`
	DownloadCount         int64    `json:"downloadCount"`
	CreatedAt             string   `json:"createdAt"`
	UpdatedAt             string   `json:"updatedAt"`
}

// skillVersionResponse is one immutable published snapshot.
type skillVersionResponse struct {
	ID            string `json:"id"`
	SkillID       string `json:"skillId"`
	VersionNumber int    `json:"versionNumber"`
	Semver        string `json:"semver"`
	PublishedAt   string `json:"publishedAt"`
	Changelog     string `json:"changelog"`
	Content       string `json:"content"`
	AuthorID      string `json:"authorId"`
}

// skillsListResponse is the paginated list envelope: one keyset page plus the
// opaque next_cursor (mirroring /v1/sessions) and the per-tab counts for the
// active search.
type skillsListResponse struct {
	Items      []skillResponse `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
	Counts     skillCountsResp `json:"counts"`
}

// skillCountsResp are the tab counts for the current search: all matching,
// authored by the caller (mine), and everyone else's (team = all - mine).
type skillCountsResp struct {
	All  int64 `json:"all"`
	Mine int64 `json:"mine"`
	Team int64 `json:"team"`
}

// handleGenerateSkill runs the pkg/skill LLM generator over the requested
// sessions, persists the result, and returns it as a SkillDraft.
//
// The generator reads session transcripts through an in-process, org-scoped
// querier (skillTraceQuerier) bound to the inbound org, so generation only
// ever sees sessions in the caller's tenant and needs no loopback HTTP hop.
func (s *Server) handleGenerateSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}

	var req generateSkillRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid request body"})
	}
	if len(req.SessionIDs) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "sessionIds is required and must be non-empty"})
	}

	skillType := "workflow"
	if req.Hint != nil && strings.TrimSpace(req.Hint.Type) != "" {
		skillType = strings.TrimSpace(req.Hint.Type)
	}
	if !skill.ValidSkillType(skillType) {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{
			Error: fmt.Sprintf("invalid type %q; valid types: %s", skillType, strings.Join(skill.SkillTypes, ", ")),
		})
	}

	// name may be empty — the generator then suggests a descriptive name from
	// the transcript rather than a generic skill-from-<id> placeholder.
	name := ""
	if req.Hint != nil {
		name = strings.TrimSpace(req.Hint.Name)
	}

	// Reuse the platform's shared search/embedding credential (threaded onto
	// Config) so skill extraction needs no separate provider key. Default to
	// openai — the search provider — when nothing is configured; NewLLMCaller
	// then falls back to env/credentials resolution for the key.
	llmCfg := skill.LLMCallerConfig{
		Provider: s.config.SkillLLMProvider,
		Model:    s.config.SkillLLMModel,
		APIKey:   s.config.SkillLLMAPIKey,
		BaseURL:  s.config.SkillLLMBaseURL,
	}
	if strings.TrimSpace(llmCfg.Provider) == "" {
		llmCfg.Provider = "openai"
	}
	llmCaller, err := skill.NewLLMCaller(llmCfg)
	if err != nil {
		// Skill generation borrows the tenant's search/embedding key; a
		// missing key means search is disabled for this tenant, not a
		// server fault. Surface that as an actionable 422 rather than a 500.
		if errors.Is(err, skill.ErrNoAPIKey) {
			return c.Status(fiber.StatusUnprocessableEntity).JSON(llm.ErrorResponse{
				Error: "skill generation requires the search/embedding feature to be enabled for this tenant",
			})
		}
		s.logger.Error("configure llm for skill generation", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "llm provider not configured"})
	}

	// Read transcripts through an in-process, org-scoped querier rather than
	// a loopback HTTP self-call: the self-call sent no X-Tapes-Org-Id, so in
	// staging/prod the trace reads hit the nil-org sentinel and generated an
	// empty skill for the real tenant.
	query, ok := s.skillTraceQuerier(orgIDFromCtx(c))
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "skills generation not supported by this backend"})
	}
	sk, err := skill.NewGenerator(query, llmCaller).Generate(c.Context(), req.SessionIDs, name, skillType, nil)
	if err != nil {
		// A source session the tenant can't see (wrong org or absent) is a 404,
		// not a server fault — the org-scoped querier refuses it before any LLM
		// call, so an unknown/cross-tenant session id never reads as a 500.
		if errors.Is(err, errSkillSessionNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "one or more source sessions were not found"})
		}
		s.logger.Error("generate skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: fmt.Sprintf("failed to generate skill: %v", err)})
	}

	now := time.Now().UTC()
	displayName := strings.TrimSpace(sk.Name)
	slug := slugifySkillName(displayName)
	if slug == "" {
		slug = fallbackSkillName(req.SessionIDs[0])
		displayName = slug
	}
	// Skills are keyed on an opaque id, so slug no longer has to be unique — two
	// generations whose names slugify the same coexist as distinct ids. Mint the
	// id here so the client can navigate to the new skill.
	rec := storage.SkillRecord{
		ID:                      uuid.NewString(),
		Slug:                    slug,
		Name:                    displayName,
		Description:             sk.Description,
		Type:                    sk.Type,
		Version:                 sk.Version,
		Visibility:              "private",
		Tags:                    sk.Tags,
		Content:                 sk.Content,
		IsAIGenerated:           true,
		GeneratedFromSessionIDs: sk.Sessions,
		AuthorSubject:           authSubjectFromCtx(c),
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	saved, err := store.UpsertSkill(c.Context(), orgIDFromCtx(c), rec)
	if err != nil {
		s.logger.Error("persist skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to persist skill"})
	}

	return c.Status(fiber.StatusCreated).JSON(skillFromRecord(*saved))
}

// handleGetSkill returns a persisted skill by its org-scoped id.
func (s *Server) handleGetSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}

	rec, err := store.GetSkill(c.Context(), orgIDFromCtx(c), c.Params("id"))
	if err != nil {
		s.logger.Error("get skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}
	return c.JSON(skillFromRecord(*rec))
}

// handleListSkills returns one keyset page of skills for the org (newest-edited
// first) plus the per-tab counts for the active search. Query params mirror
// /v1/sessions: limit, cursor (opaque), q (name/description/tag search), scope
// (all|mine|team). The cursor and counts make search/filter correct across the
// whole set rather than just the loaded page.
func (s *Server) handleListSkills(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	orgID := orgIDFromCtx(c)
	subject := authSubjectFromCtx(c)

	limit := defaultSkillsLimit
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := fmt.Sscanf(raw, "%d", &limit); err != nil || parsed != 1 || limit < 1 {
			limit = defaultSkillsLimit
		}
		if limit > maxSkillsLimit {
			limit = maxSkillsLimit
		}
	}

	query := strings.TrimSpace(c.Query("q"))
	opts := storage.SkillListOpts{Query: query, Limit: limit + 1} // +1 to detect has_more
	if c.Query("sort") == storage.SkillSortDownloads {
		opts.Sort = storage.SkillSortDownloads
	}
	switch c.Query("scope") {
	case "mine":
		opts.Author = subject
	case "team":
		opts.NotAuthor = subject
	}
	if raw := c.Query("cursor"); raw != "" {
		cur, err := decodeSkillsCursor(raw)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: err.Error()})
		}
		opts.CursorID = cur.ID
		if opts.Sort == storage.SkillSortDownloads {
			dc := cur.Downloads
			opts.CursorDownloads = &dc
		} else {
			ts := cur.UpdatedAt
			opts.CursorTs = &ts
		}
	}

	recs, err := store.ListSkills(c.Context(), orgID, opts)
	if err != nil {
		s.logger.Error("list skills", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list skills"})
	}

	var nextCursor string
	if len(recs) > limit {
		recs = recs[:limit]
		last := recs[len(recs)-1]
		nextCursor = encodeSkillsCursor(skillsCursor{
			UpdatedAt: last.UpdatedAt,
			Downloads: last.DownloadCount,
			ID:        last.ID,
		})
	}

	counts, err := store.CountSkills(c.Context(), orgID, query, subject)
	if err != nil {
		s.logger.Error("count skills", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list skills"})
	}

	items := make([]skillResponse, len(recs))
	for i, r := range recs {
		items[i] = skillFromRecord(r)
	}
	return c.JSON(skillsListResponse{
		Items:      items,
		NextCursor: nextCursor,
		Counts: skillCountsResp{
			All:  counts.Total,
			Mine: counts.Mine,
			Team: counts.Total - counts.Mine,
		},
	})
}

// handleListSessionSkills returns the skills generated from a given session
// (reverse lookup over provenance). Small result set, so it's unpaginated —
// the "Skills from this session" panel renders them directly.
func (s *Server) handleListSessionSkills(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	recs, err := store.ListSkillsBySession(c.Context(), orgIDFromCtx(c), c.Params("id"))
	if err != nil {
		s.logger.Error("list session skills", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list skills"})
	}
	items := make([]skillResponse, len(recs))
	for i, r := range recs {
		items[i] = skillFromRecord(r)
	}
	return c.JSON(fiber.Map{"items": items})
}

// updateSkillRequest is the PUT /v1/skills/:slug body — all fields optional;
// only present fields are applied onto the existing record.
type updateSkillRequest struct {
	Name        *string  `json:"name"`
	Description *string  `json:"description"`
	Type        *string  `json:"type"`
	Visibility  *string  `json:"visibility"`
	Tags        []string `json:"tags"`
	Content     *string  `json:"content"`
}

// handleUpdateSkill saves edits to a skill's working content/metadata. The
// upsert preserves created_at and author_subject (original creator stays
// authoritative).
func (s *Server) handleUpdateSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	orgID := orgIDFromCtx(c)
	existing, err := store.GetSkill(c.Context(), orgID, c.Params("id"))
	if err != nil {
		s.logger.Error("get skill for update", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if existing == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}

	var req updateSkillRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid request body"})
	}

	rec := *existing
	if req.Name != nil {
		rec.Name = *req.Name
		// slug is cosmetic now (the id is identity), so keep it in sync with the
		// name on rename — the SKILL.md filename then tracks the current name.
		if derived := slugifySkillName(rec.Name); derived != "" {
			rec.Slug = derived
		}
	}
	if req.Description != nil {
		rec.Description = *req.Description
	}
	if req.Type != nil {
		if !skill.ValidSkillType(*req.Type) {
			return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: fmt.Sprintf("invalid type %q", *req.Type)})
		}
		rec.Type = *req.Type
	}
	if req.Visibility != nil {
		rec.Visibility = *req.Visibility
	}
	if req.Tags != nil {
		rec.Tags = req.Tags
	}
	if req.Content != nil {
		rec.Content = *req.Content
	}
	rec.UpdatedAt = time.Now().UTC()

	saved, err := store.UpsertSkill(c.Context(), orgID, rec)
	if err != nil {
		s.logger.Error("update skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to save skill"})
	}
	return c.JSON(skillFromRecord(*saved))
}

// handleDeleteSkill removes a skill and its version history. Owner-gated: only
// the recorded author may delete (unattributed skills are deletable by anyone,
// matching the edit affordance).
func (s *Server) handleDeleteSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	orgID := orgIDFromCtx(c)
	existing, err := store.GetSkill(c.Context(), orgID, c.Params("id"))
	if err != nil {
		s.logger.Error("get skill for delete", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if existing == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}

	// Only the creator may delete. An empty author_subject means unattributed
	// (legacy/demo) — deletable by anyone, mirroring the edit gate.
	subject := authSubjectFromCtx(c)
	if existing.AuthorSubject != "" && subject != existing.AuthorSubject {
		return c.Status(fiber.StatusForbidden).JSON(llm.ErrorResponse{Error: "only the creator can delete this skill"})
	}

	if _, err := store.DeleteSkill(c.Context(), orgID, existing.ID); err != nil {
		s.logger.Error("delete skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to delete skill"})
	}
	return c.SendStatus(fiber.StatusNoContent)
}

// createSkillRequest is the POST /v1/skills body for an authored-from-scratch
// skill — only a name is required; the rest default to an empty private draft.
type createSkillRequest struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Type        string   `json:"type"`
	Tags        []string `json:"tags"`
	Content     string   `json:"content"`
}

// handleCreateSkill writes a new blank/authored skill (empty provenance),
// attributed to the caller. Generate is the AI path; this is the
// create-from-scratch path. The id is minted here; slug is a cosmetic label
// derived from the name (no longer unique).
func (s *Server) handleCreateSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}

	var req createSkillRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: "invalid request body"})
	}

	displayName := strings.TrimSpace(req.Name)
	if displayName == "" {
		displayName = "New skill"
	}
	skillType := strings.TrimSpace(req.Type)
	if skillType == "" {
		skillType = "workflow"
	}
	if !skill.ValidSkillType(skillType) {
		return c.Status(fiber.StatusBadRequest).JSON(llm.ErrorResponse{Error: fmt.Sprintf("invalid type %q", skillType)})
	}

	slug := slugifySkillName(displayName)
	if slug == "" {
		slug = "new-skill"
	}
	orgID := orgIDFromCtx(c)

	now := time.Now().UTC()
	rec := storage.SkillRecord{
		ID:                      uuid.NewString(),
		Slug:                    slug,
		Name:                    displayName,
		Description:             req.Description,
		Type:                    skillType,
		Version:                 "0.1.0",
		Visibility:              "private",
		Tags:                    req.Tags,
		Content:                 req.Content,
		IsAIGenerated:           false,
		GeneratedFromSessionIDs: nil,
		AuthorSubject:           authSubjectFromCtx(c),
		CreatedAt:               now,
		UpdatedAt:               now,
	}
	saved, err := store.UpsertSkill(c.Context(), orgID, rec)
	if err != nil {
		s.logger.Error("create skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to create skill"})
	}
	return c.Status(fiber.StatusCreated).JSON(skillFromRecord(*saved))
}

// publishSkillRequest is the POST /v1/skills/:slug/versions body.
type publishSkillRequest struct {
	Content   string `json:"content"`
	Changelog string `json:"changelog"`
}

// maxPublishAttempts bounds the retry loop that resolves a concurrent
// version-number collision when two publishes of the same skill race.
const maxPublishAttempts = 4

// handlePublishSkill snapshots the skill's content into an immutable version
// and bumps the skill's current semver (first publish 0.1.0, then patch).
func (s *Server) handlePublishSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	orgID := orgIDFromCtx(c)
	existing, err := store.GetSkill(c.Context(), orgID, c.Params("id"))
	if err != nil {
		s.logger.Error("get skill for publish", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if existing == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}
	skillID := existing.ID

	var req publishSkillRequest
	_ = c.BodyParser(&req)
	content := req.Content
	if strings.TrimSpace(content) == "" {
		content = existing.Content
	}

	now := time.Now().UTC()

	// Assigning the next version number (a MAX read) and inserting it are two
	// round-trips, so two concurrent publishes of the same skill can pick the
	// same number — the second insert then hits the (skill_id, version_number)
	// unique constraint. Retry on that conflict: the next MAX read sees the
	// committed competitor, so a bounded loop converges instead of 500-ing.
	var ver *storage.SkillVersionRecord
	var semver string
	for attempt := 0; attempt < maxPublishAttempts; attempt++ {
		n, err := store.NextSkillVersionNumber(c.Context(), orgID, skillID)
		if err != nil {
			s.logger.Error("next skill version", "error", err)
			return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to version skill"})
		}
		semver = fmt.Sprintf("0.1.%d", n-1) // n=1 -> 0.1.0, n=2 -> 0.1.1, …

		ver, err = store.CreateSkillVersion(c.Context(), orgID, storage.SkillVersionRecord{
			SkillID:       skillID,
			VersionNumber: n,
			Semver:        semver,
			Changelog:     req.Changelog,
			Content:       content,
			AuthorSubject: authSubjectFromCtx(c),
			PublishedAt:   now,
		})
		if err == nil {
			break
		}
		if errors.Is(err, storage.ErrSkillVersionConflict) && attempt < maxPublishAttempts-1 {
			continue // a concurrent publish took this number; recompute and retry
		}
		s.logger.Error("create skill version", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to publish skill"})
	}

	// Bump the skill's current semver; persist the published content as the new
	// head if it changed since the last save.
	if content != existing.Content {
		rec := *existing
		rec.Content = content
		rec.Version = semver
		rec.UpdatedAt = now
		if _, err := store.UpsertSkill(c.Context(), orgID, rec); err != nil {
			s.logger.Error("persist published content", "error", err)
		}
	} else if err := store.SetSkillVersion(c.Context(), orgID, skillID, semver, now); err != nil {
		s.logger.Error("bump skill version", "error", err)
	}

	return c.Status(fiber.StatusCreated).JSON(skillVersionFromRecord(*ver))
}

// handleListSkillVersions returns a skill's published version history.
func (s *Server) handleListSkillVersions(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	vers, err := store.ListSkillVersions(c.Context(), orgIDFromCtx(c), c.Params("id"))
	if err != nil {
		s.logger.Error("list skill versions", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to list versions"})
	}
	items := make([]skillVersionResponse, len(vers))
	for i, v := range vers {
		items[i] = skillVersionFromRecord(v)
	}
	return c.JSON(fiber.Map{"versions": items, "totalCount": len(items)})
}

// handleDuplicateSkill copies a skill under a fresh id, attributed to the
// duplicating user. Because slug is no longer an identity it can be shared with
// the parent freely — no "-copy" suffix is needed to stay distinct.
func (s *Server) handleDuplicateSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	orgID := orgIDFromCtx(c)
	existing, err := store.GetSkill(c.Context(), orgID, c.Params("id"))
	if err != nil {
		s.logger.Error("get skill for duplicate", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if existing == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}

	now := time.Now().UTC()
	rec := *existing
	rec.ID = uuid.NewString()
	rec.Name = existing.Name + " (copy)"
	rec.Slug = existing.Slug // slug is cosmetic; sharing the parent's reads fine
	rec.Visibility = "private"
	rec.Version = "0.1.0"
	rec.ParentID = existing.ID
	rec.AuthorSubject = authSubjectFromCtx(c)
	rec.CreatedAt = now
	rec.UpdatedAt = now

	saved, err := store.UpsertSkill(c.Context(), orgID, rec)
	if err != nil {
		s.logger.Error("duplicate skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to duplicate skill"})
	}
	return c.Status(fiber.StatusCreated).JSON(skillFromRecord(*saved))
}

// handleSkillMarkdown renders a drop-in SKILL.md (frontmatter + body) for the
// "Use this skill" download, via the same renderer the CLI uses.
func (s *Server) handleSkillMarkdown(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	rec, err := store.GetSkill(c.Context(), orgIDFromCtx(c), c.Params("id"))
	if err != nil {
		s.logger.Error("get skill for markdown", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}

	// Count the download as a real usage signal (best-effort — never fail the
	// download over a counter write).
	if err := store.IncrementSkillDownloads(c.Context(), orgIDFromCtx(c), rec.ID); err != nil {
		s.logger.Warn("increment skill downloads", "error", err)
	}

	// The SKILL.md frontmatter `name` must be the kebab slug (Claude Code
	// matches it to the skill's directory), not the human display name; the
	// display name carries no meaning in the on-disk file.
	sk := &skill.Skill{
		Name:        rec.Slug,
		Description: rec.Description,
		Version:     rec.Version,
		Tags:        rec.Tags,
		Type:        rec.Type,
		Content:     rec.Content,
		Sessions:    rec.GeneratedFromSessionIDs,
		CreatedAt:   rec.CreatedAt,
	}
	c.Set("Content-Type", "text/markdown; charset=utf-8")
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", rec.Slug+".md"))
	return c.SendString(skill.RenderSkillMD(sk))
}

// skillFromRecord maps the storage row to the camelCase Skill wire shape,
// normalizing nil slices to empty arrays and null parent_slug to a JSON null.
func skillFromRecord(rec storage.SkillRecord) skillResponse {
	tags := rec.Tags
	if tags == nil {
		tags = []string{}
	}
	sessions := rec.GeneratedFromSessionIDs
	if sessions == nil {
		sessions = []string{}
	}
	var parent *string
	if rec.ParentID != "" {
		p := rec.ParentID
		parent = &p
	}
	return skillResponse{
		ID:                    rec.ID,
		Slug:                  rec.Slug,
		ParentID:              parent,
		Name:                  rec.Name,
		Description:           rec.Description,
		Type:                  rec.Type,
		Version:               rec.Version,
		Visibility:            rec.Visibility,
		Tags:                  tags,
		Content:               rec.Content,
		IsAIGenerated:         rec.IsAIGenerated,
		OriginatingSessionIDs: sessions,
		AuthorID:              rec.AuthorSubject,
		DownloadCount:         rec.DownloadCount,
		CreatedAt:             rec.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:             rec.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func skillVersionFromRecord(rec storage.SkillVersionRecord) skillVersionResponse {
	return skillVersionResponse{
		ID:            fmt.Sprintf("%s-v%d", rec.SkillID, rec.VersionNumber),
		SkillID:       rec.SkillID,
		VersionNumber: rec.VersionNumber,
		Semver:        rec.Semver,
		PublishedAt:   rec.PublishedAt.UTC().Format(time.RFC3339),
		Changelog:     rec.Changelog,
		Content:       rec.Content,
		AuthorID:      rec.AuthorSubject,
	}
}

// fallbackSkillName derives a kebab-case name when the client supplies no
// hint name, e.g. "skill-from-1a2b3c4d".
func fallbackSkillName(sessionID string) string {
	short := strings.ToLower(sessionID)
	short = strings.ReplaceAll(short, "-", "")
	if len(short) > 8 {
		short = short[:8]
	}
	if short == "" {
		short = "session"
	}
	return "skill-from-" + short
}

// slugifySkillName lowercases and hyphenates an arbitrary name into the
// kebab-case slug the console uses as the URL segment.
func slugifySkillName(name string) string {
	var b strings.Builder
	prevHyphen := false
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			prevHyphen = false
		case b.Len() > 0 && !prevHyphen:
			b.WriteByte('-')
			prevHyphen = true
		}
	}
	return strings.Trim(b.String(), "-")
}
