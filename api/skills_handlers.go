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

// skillDraftResponse is the SkillDraft shape the console expects from both
// generate and get. camelCase, parentSlug is null when absent.
type skillDraftResponse struct {
	Slug                    string   `json:"slug"`
	ParentSlug              *string  `json:"parentSlug"`
	Name                    string   `json:"name"`
	Description             string   `json:"description"`
	Type                    string   `json:"type"`
	Visibility              string   `json:"visibility"`
	Tags                    []string `json:"tags"`
	Content                 string   `json:"content"`
	IsAIGenerated           bool     `json:"isAiGenerated"`
	GeneratedFromSessionIDs []string `json:"generatedFromSessionIds"`
	CreatedAt               string   `json:"createdAt"`
	UpdatedAt               string   `json:"updatedAt"`
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
	store, ok := s.driver.(skillStore)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "skills not supported by this backend"})
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

	name := ""
	if req.Hint != nil {
		name = strings.TrimSpace(req.Hint.Name)
	}
	if name == "" {
		name = fallbackSkillName(req.SessionIDs[0])
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
	slug := slugifySkillName(sk.Name)
	if slug == "" {
		slug = fallbackSkillName(req.SessionIDs[0])
	}
	rec := storage.SkillRecord{
		Slug:                    slug,
		Name:                    sk.Name,
		Description:             sk.Description,
		Type:                    sk.Type,
		Version:                 sk.Version,
		Visibility:              "private",
		Tags:                    sk.Tags,
		Content:                 sk.Content,
		IsAIGenerated:           true,
		GeneratedFromSessionIDs: sk.Sessions,
		CreatedAt:               now,
		UpdatedAt:               now,
	}

	saved, err := store.UpsertSkill(c.Context(), orgIDFromCtx(c), rec)
	if err != nil {
		s.logger.Error("persist skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to persist skill"})
	}

	return c.Status(fiber.StatusCreated).JSON(skillDraftFromRecord(*saved))
}

// handleGetSkill returns a persisted skill by its org-scoped slug.
func (s *Server) handleGetSkill(c *fiber.Ctx) error {
	store, ok := s.driver.(skillStore)
	if !ok {
		return c.Status(fiber.StatusNotImplemented).JSON(llm.ErrorResponse{Error: "skills not supported by this backend"})
	}

	rec, err := store.GetSkill(c.Context(), orgIDFromCtx(c), c.Params("slug"))
	if err != nil {
		s.logger.Error("get skill", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(llm.ErrorResponse{Error: "failed to fetch skill"})
	}
	if rec == nil {
		return c.Status(fiber.StatusNotFound).JSON(llm.ErrorResponse{Error: "skill not found"})
	}
	return c.JSON(skillDraftFromRecord(*rec))
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

// skillDraftFromRecord maps the storage row to the camelCase SkillDraft wire
// shape, normalizing nil slices to empty arrays (the console schema requires
// arrays, not null) and null parent_slug to a JSON null.
func skillDraftFromRecord(rec storage.SkillRecord) skillDraftResponse {
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
	return skillDraftResponse{
		Slug:                    rec.Slug,
		ParentSlug:              parent,
		Name:                    rec.Name,
		Description:             rec.Description,
		Type:                    rec.Type,
		Visibility:              rec.Visibility,
		Tags:                    tags,
		Content:                 rec.Content,
		IsAIGenerated:           rec.IsAIGenerated,
		GeneratedFromSessionIDs: sessions,
		CreatedAt:               rec.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:               rec.UpdatedAt.UTC().Format(time.RFC3339),
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
