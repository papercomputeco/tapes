package api

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/papercomputeco/tapes/pkg/llm"
	"github.com/papercomputeco/tapes/pkg/skill"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// skillStore is the capability interface the skills API needs from the
// storage layer. The Postgres driver implements it; the handlers return 501
// for drivers (e.g. the in-memory node store) that don't.
type skillStore interface {
	UpsertSkill(ctx context.Context, orgID string, rec storage.SkillRecord) (*storage.SkillRecord, error)
	GetSkill(ctx context.Context, orgID, slug string) (*storage.SkillRecord, error)
	ListSkills(ctx context.Context, orgID string, limit int) ([]storage.SkillRecord, error)
	NextSkillVersionNumber(ctx context.Context, orgID, slug string) (int, error)
	CreateSkillVersion(ctx context.Context, orgID string, rec storage.SkillVersionRecord) (*storage.SkillVersionRecord, error)
	SetSkillVersion(ctx context.Context, orgID, slug, semver string, updatedAt time.Time) error
	ListSkillVersions(ctx context.Context, orgID, slug string) ([]storage.SkillVersionRecord, error)
	IncrementSkillDownloads(ctx context.Context, orgID, slug string) error
	DeleteSkill(ctx context.Context, orgID, slug string) (bool, error)
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

// skillResponse is the unified Skill shape the console expects (camelCase).
// content always lives on the skill row (versions are history only);
// parentSlug is null unless the skill is a duplicate/fork.
type skillResponse struct {
	Slug                  string   `json:"slug"`
	ParentSlug            *string  `json:"parentSlug"`
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
	SkillSlug     string `json:"skillSlug"`
	VersionNumber int    `json:"versionNumber"`
	Semver        string `json:"semver"`
	PublishedAt   string `json:"publishedAt"`
	Changelog     string `json:"changelog"`
	Content       string `json:"content"`
	AuthorID      string `json:"authorId"`
}

// handleGenerateSkill runs the pkg/skill LLM generator over the requested
// sessions, persists the result, and returns it as a SkillDraft.
//
// The generator reads session transcripts through the trace API. We point it
// at this same server (a loopback self-call) rather than reimplementing the
// trace reads. NOTE: that self-call does not carry the X-Tapes-Org-Id header,
// so it reads under the nil-org bucket. This is correct for the current dev
// loop — the console's tapesFetch sends no org header either, so the inbound
// generate request and the internal trace reads share the nil-org tenant.
// Multi-tenant production needs org propagation (a direct driver-backed
// Querier); that is deliberately deferred for the walking skeleton.
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
		s.logger.Error("configure llm for skill generation", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "llm provider not configured"})
	}

	query := skill.NewAPIClient(s.selfAPITarget())
	sk, err := skill.NewGenerator(query, llmCaller).Generate(c.Context(), req.SessionIDs, name, skillType, nil)
	if err != nil {
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
	// Skills are keyed on (org_id, slug), so two generations whose names slugify
	// the same would collide — UpsertSkill would silently overwrite the earlier
	// skill. Mint a fresh org-unique slug instead so each generation lands on its
	// own row (and the client navigates to the new one).
	uniqueSlug, err := s.uniqueSkillSlug(c, store, orgIDFromCtx(c), slug)
	if err != nil {
		s.logger.Error("unique skill slug", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to persist skill"})
	}
	slug = uniqueSlug
	rec := storage.SkillRecord{
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

// handleGetSkill returns a persisted skill by its org-scoped slug.
func (s *Server) handleGetSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}

	rec, err := store.GetSkill(c.Context(), orgIDFromCtx(c), c.Params("slug"))
	if err != nil {
		s.logger.Error("get skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}
	return c.JSON(skillFromRecord(*rec))
}

// handleListSkills returns all skills for the org (newest-edited first). The
// console filters/sorts client-side, so a single capped page is returned.
func (s *Server) handleListSkills(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	recs, err := store.ListSkills(c.Context(), orgIDFromCtx(c), 0)
	if err != nil {
		s.logger.Error("list skills", "error", err)
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
	existing, err := store.GetSkill(c.Context(), orgID, c.Params("slug"))
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
	existing, err := store.GetSkill(c.Context(), orgID, c.Params("slug"))
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

	if _, err := store.DeleteSkill(c.Context(), orgID, existing.Slug); err != nil {
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
// create-from-scratch path. The slug is minted org-unique from the name.
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

	base := slugifySkillName(displayName)
	if base == "" {
		base = "new-skill"
	}
	orgID := orgIDFromCtx(c)
	slug, err := s.uniqueSkillSlug(c, store, orgID, base)
	if err != nil {
		s.logger.Error("unique skill slug", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to create skill"})
	}

	now := time.Now().UTC()
	rec := storage.SkillRecord{
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

// handlePublishSkill snapshots the skill's content into an immutable version
// and bumps the skill's current semver (first publish 0.1.0, then patch).
func (s *Server) handlePublishSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	orgID := orgIDFromCtx(c)
	slug := c.Params("slug")
	existing, err := store.GetSkill(c.Context(), orgID, slug)
	if err != nil {
		s.logger.Error("get skill for publish", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if existing == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}

	var req publishSkillRequest
	_ = c.BodyParser(&req)
	content := req.Content
	if strings.TrimSpace(content) == "" {
		content = existing.Content
	}

	n, err := store.NextSkillVersionNumber(c.Context(), orgID, slug)
	if err != nil {
		s.logger.Error("next skill version", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to version skill"})
	}
	semver := fmt.Sprintf("0.1.%d", n-1) // n=1 -> 0.1.0, n=2 -> 0.1.1, …
	now := time.Now().UTC()

	ver, err := store.CreateSkillVersion(c.Context(), orgID, storage.SkillVersionRecord{
		SkillSlug:     slug,
		VersionNumber: n,
		Semver:        semver,
		Changelog:     req.Changelog,
		Content:       content,
		AuthorSubject: authSubjectFromCtx(c),
		PublishedAt:   now,
	})
	if err != nil {
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
	} else if err := store.SetSkillVersion(c.Context(), orgID, slug, semver, now); err != nil {
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
	vers, err := store.ListSkillVersions(c.Context(), orgIDFromCtx(c), c.Params("slug"))
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

// handleDuplicateSkill copies a skill under a fresh slug, attributed to the
// duplicating user.
func (s *Server) handleDuplicateSkill(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	orgID := orgIDFromCtx(c)
	existing, err := store.GetSkill(c.Context(), orgID, c.Params("slug"))
	if err != nil {
		s.logger.Error("get skill for duplicate", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if existing == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}

	newSlug, err := s.uniqueDupSlug(c, store, orgID, existing.Slug)
	if err != nil {
		s.logger.Error("duplicate slug", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to duplicate skill"})
	}

	now := time.Now().UTC()
	rec := *existing
	rec.Slug = newSlug
	rec.Name = existing.Name + " (copy)"
	rec.Visibility = "private"
	rec.Version = "0.1.0"
	rec.ParentSlug = existing.Slug
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

// uniqueDupSlug finds an unused "<slug>-copy[-N]" slug for a duplicate.
func (s *Server) uniqueDupSlug(c *fiber.Ctx, store skillStore, orgID, slug string) (string, error) {
	return s.uniqueSkillSlug(c, store, orgID, slug+"-copy")
}

// uniqueSkillSlug returns base if it is free in the org, otherwise base-2,
// base-3, … until an unused org-scoped slug is found. Skills are keyed on
// (org_id, slug), so callers that UpsertSkill a new row must reserve a free
// slug first or risk overwriting an existing skill.
func (s *Server) uniqueSkillSlug(c *fiber.Ctx, store skillStore, orgID, base string) (string, error) {
	candidate := base
	for i := 2; i < 1000; i++ {
		existing, err := store.GetSkill(c.Context(), orgID, candidate)
		if err != nil {
			return "", err
		}
		if existing == nil {
			return candidate, nil
		}
		candidate = fmt.Sprintf("%s-%d", base, i)
	}
	return "", fmt.Errorf("could not find a free slug for %q", base)
}

// handleSkillMarkdown renders a drop-in SKILL.md (frontmatter + body) for the
// "Use this skill" download, via the same renderer the CLI uses.
func (s *Server) handleSkillMarkdown(c *fiber.Ctx) error {
	store, ok := s.skillStoreOr501(c)
	if !ok {
		return nil
	}
	rec, err := store.GetSkill(c.Context(), orgIDFromCtx(c), c.Params("slug"))
	if err != nil {
		s.logger.Error("get skill for markdown", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}

	// Count the download as a real usage signal (best-effort — never fail the
	// download over a counter write).
	if err := store.IncrementSkillDownloads(c.Context(), orgIDFromCtx(c), rec.Slug); err != nil {
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

// selfAPITarget builds the loopback base URL the generator's trace client
// dials. ListenAddr is typically ":8081"; prefix a loopback host so the
// address is dialable.
func (s *Server) selfAPITarget() string {
	addr := s.config.ListenAddr
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	return "http://" + addr
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
	if rec.ParentSlug != "" {
		p := rec.ParentSlug
		parent = &p
	}
	return skillResponse{
		Slug:                  rec.Slug,
		ParentSlug:            parent,
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
		ID:            fmt.Sprintf("%s-v%d", rec.SkillSlug, rec.VersionNumber),
		SkillSlug:     rec.SkillSlug,
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
