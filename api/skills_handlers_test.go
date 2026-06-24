package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// skillsStubDriver is an in-memory implementation of the unexported skillStore
// capability, recording the org it was scoped to so specs can assert org
// threading and exercise the read/write/version endpoints without Postgres.
type skillsStubDriver struct {
	storage.Driver

	skills    map[string]storage.SkillRecord
	versions  map[string][]storage.SkillVersionRecord
	downloads map[string]int
	lastOrg   string
}

func newSkillsStub() *skillsStubDriver {
	return &skillsStubDriver{
		skills:    map[string]storage.SkillRecord{},
		versions:  map[string][]storage.SkillVersionRecord{},
		downloads: map[string]int{},
	}
}

func (d *skillsStubDriver) IncrementSkillDownloads(_ context.Context, _, slug string) error {
	d.downloads[slug]++
	return nil
}

func (d *skillsStubDriver) UpsertSkill(_ context.Context, orgID string, rec storage.SkillRecord) (*storage.SkillRecord, error) {
	d.lastOrg = orgID
	d.skills[rec.Slug] = rec
	out := rec
	return &out, nil
}

func (d *skillsStubDriver) GetSkill(_ context.Context, orgID, slug string) (*storage.SkillRecord, error) {
	d.lastOrg = orgID
	if r, ok := d.skills[slug]; ok {
		out := r
		return &out, nil
	}
	return nil, nil
}

func (d *skillsStubDriver) DeleteSkill(_ context.Context, orgID, slug string) (bool, error) {
	d.lastOrg = orgID
	if _, ok := d.skills[slug]; !ok {
		return false, nil
	}
	delete(d.skills, slug)
	delete(d.versions, slug)
	return true, nil
}

func (d *skillsStubDriver) ListSkills(_ context.Context, orgID string, _ int) ([]storage.SkillRecord, error) {
	d.lastOrg = orgID
	out := make([]storage.SkillRecord, 0, len(d.skills))
	for _, r := range d.skills {
		out = append(out, r)
	}
	return out, nil
}

func (d *skillsStubDriver) NextSkillVersionNumber(_ context.Context, _, slug string) (int, error) {
	return len(d.versions[slug]) + 1, nil
}

func (d *skillsStubDriver) CreateSkillVersion(_ context.Context, _ string, rec storage.SkillVersionRecord) (*storage.SkillVersionRecord, error) {
	d.versions[rec.SkillSlug] = append(d.versions[rec.SkillSlug], rec)
	out := rec
	return &out, nil
}

func (d *skillsStubDriver) SetSkillVersion(_ context.Context, _, slug, semver string, _ time.Time) error {
	if r, ok := d.skills[slug]; ok {
		r.Version = semver
		d.skills[slug] = r
	}
	return nil
}

func (d *skillsStubDriver) ListSkillVersions(_ context.Context, _, slug string) ([]storage.SkillVersionRecord, error) {
	return d.versions[slug], nil
}

func seedStubSkill(d *skillsStubDriver, slug string) {
	d.skills[slug] = storage.SkillRecord{
		Slug:                    slug,
		Name:                    "Debug React Hooks",
		Description:             "desc",
		Type:                    "workflow",
		Version:                 "0.1.0",
		Visibility:              "private",
		Tags:                    []string{"react"},
		Content:                 "# body",
		IsAIGenerated:           true,
		GeneratedFromSessionIDs: []string{"sess-1"},
		AuthorSubject:           "user-seed",
	}
}

// doJSON issues a request (optionally with org + auth-subject headers) and
// returns the decoded body map (on 2xx) plus the status code.
func doJSON(server *Server, method, path, body, org, author string) (map[string]any, int) {
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewBufferString(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, path, rdr)
	Expect(err).NotTo(HaveOccurred())
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	if author != "" {
		req.Header.Set(authSubjectHeader, author)
	}
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	var out map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return out, resp.StatusCode
}

func newSkillsServer(stub *skillsStubDriver) *Server {
	server, err := NewServer(Config{ListenAddr: ":0"}, stub, tapeslogger.NewNoop())
	Expect(err).NotTo(HaveOccurred())
	return server
}

var _ = Describe("Skills handlers", func() {
	It("returns a unified camelCase skill scoped to the org", func() {
		stub := newSkillsStub()
		seedStubSkill(stub, "debug-react-hooks")
		server := newSkillsServer(stub)

		org := "11111111-1111-1111-1111-111111111111"
		body, status := doJSON(server, http.MethodGet, "/v1/skills/debug-react-hooks", "", org, "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(stub.lastOrg).To(Equal(org), "the read must be scoped to the requested tenant")
		Expect(body).To(HaveKeyWithValue("slug", "debug-react-hooks"))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", true))
		Expect(body).To(HaveKey("originatingSessionIds"))
		Expect(body).To(HaveKeyWithValue("authorId", "user-seed"))
		Expect(body).To(HaveKeyWithValue("version", "0.1.0"))
		Expect(body).To(HaveKeyWithValue("parentSlug", BeNil()))
	})

	It("returns 404 when the skill is absent", func() {
		server := newSkillsServer(newSkillsStub())
		_, status := doJSON(server, http.MethodGet, "/v1/skills/missing", "", "", "")
		Expect(status).To(Equal(fiber.StatusNotFound))
	})

	It("lists skills for the org", func() {
		stub := newSkillsStub()
		seedStubSkill(stub, "a-skill")
		server := newSkillsServer(stub)
		body, status := doJSON(server, http.MethodGet, "/v1/skills", "", "", "")
		Expect(status).To(Equal(fiber.StatusOK))
		items, _ := body["items"].([]any)
		Expect(items).To(HaveLen(1))
	})

	It("rejects generate with no sessionIds before touching storage", func() {
		server := newSkillsServer(newSkillsStub())
		body, status := doJSON(server, http.MethodPost, "/v1/skills/generate", `{"sessionIds":[]}`, "", "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(body["error"]).To(ContainSubstring("sessionIds"))
	})

	It("saves edits via PUT and persists content", func() {
		stub := newSkillsStub()
		seedStubSkill(stub, "s")
		server := newSkillsServer(stub)
		body, status := doJSON(server, http.MethodPut, "/v1/skills/s", `{"content":"# new body","name":"Renamed"}`, "", "")
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(body).To(HaveKeyWithValue("content", "# new body"))
		Expect(body).To(HaveKeyWithValue("name", "Renamed"))
		Expect(stub.skills["s"].Content).To(Equal("# new body"))
	})

	It("publishes an immutable version and stamps the author", func() {
		stub := newSkillsStub()
		seedStubSkill(stub, "s")
		server := newSkillsServer(stub)
		body, status := doJSON(server, http.MethodPost, "/v1/skills/s/versions", `{"changelog":"first"}`, "", "user-pub")
		Expect(status).To(Equal(fiber.StatusCreated))
		Expect(body).To(HaveKeyWithValue("semver", "0.1.0"))
		Expect(body).To(HaveKeyWithValue("authorId", "user-pub"))
		Expect(stub.versions["s"]).To(HaveLen(1))

		// second publish bumps the patch
		body, _ = doJSON(server, http.MethodPost, "/v1/skills/s/versions", `{"changelog":"second"}`, "", "")
		Expect(body).To(HaveKeyWithValue("semver", "0.1.1"))
	})

	It("duplicates a skill under a fresh slug attributed to the duplicator", func() {
		stub := newSkillsStub()
		seedStubSkill(stub, "s")
		server := newSkillsServer(stub)
		body, status := doJSON(server, http.MethodPost, "/v1/skills/s/duplicate", "", "", "user-dup")
		Expect(status).To(Equal(fiber.StatusCreated))
		Expect(body).To(HaveKeyWithValue("slug", "s-copy"))
		Expect(body).To(HaveKeyWithValue("authorId", "user-dup"))
		Expect(body).To(HaveKeyWithValue("parentSlug", "s"))
	})

	It("mints a distinct slug per duplicate instead of overwriting", func() {
		// Exercises the shared uniqueSkillSlug increment path the generate
		// handler also relies on: a second duplicate must not collide with the
		// first (which would silently overwrite it, since skills are keyed on
		// (org_id, slug)).
		stub := newSkillsStub()
		seedStubSkill(stub, "s")
		server := newSkillsServer(stub)

		first, status := doJSON(server, http.MethodPost, "/v1/skills/s/duplicate", "", "", "user-dup")
		Expect(status).To(Equal(fiber.StatusCreated))
		Expect(first).To(HaveKeyWithValue("slug", "s-copy"))

		second, status := doJSON(server, http.MethodPost, "/v1/skills/s/duplicate", "", "", "user-dup")
		Expect(status).To(Equal(fiber.StatusCreated))
		Expect(second).To(HaveKeyWithValue("slug", "s-copy-2"))
		Expect(stub.skills).To(HaveKey("s-copy"), "the first duplicate survives the second")
	})

	It("creates a blank skill from scratch attributed to the caller", func() {
		stub := newSkillsStub()
		server := newSkillsServer(stub)
		body, status := doJSON(server, http.MethodPost, "/v1/skills",
			`{"name":"My New Skill","description":"d"}`, "", "user-new")
		Expect(status).To(Equal(fiber.StatusCreated))
		Expect(body).To(HaveKeyWithValue("slug", "my-new-skill"))
		Expect(body).To(HaveKeyWithValue("authorId", "user-new"))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", false))
		Expect(body).To(HaveKeyWithValue("originatingSessionIds", BeEmpty()))
	})

	It("deletes a skill for its creator", func() {
		stub := newSkillsStub()
		seedStubSkill(stub, "s") // author_subject = user-seed
		server := newSkillsServer(stub)
		_, status := doJSON(server, http.MethodDelete, "/v1/skills/s", "", "", "user-seed")
		Expect(status).To(Equal(fiber.StatusNoContent))
		Expect(stub.skills).NotTo(HaveKey("s"))
	})

	It("forbids deleting a skill owned by another user", func() {
		stub := newSkillsStub()
		seedStubSkill(stub, "s") // author_subject = user-seed
		server := newSkillsServer(stub)
		_, status := doJSON(server, http.MethodDelete, "/v1/skills/s", "", "", "user-other")
		Expect(status).To(Equal(fiber.StatusForbidden))
		Expect(stub.skills).To(HaveKey("s"), "the skill survives a forbidden delete")
	})

	It("404s when deleting a missing skill", func() {
		stub := newSkillsStub()
		server := newSkillsServer(stub)
		_, status := doJSON(server, http.MethodDelete, "/v1/skills/missing", "", "", "user-x")
		Expect(status).To(Equal(fiber.StatusNotFound))
	})

	It("renders a drop-in SKILL.md", func() {
		stub := newSkillsStub()
		seedStubSkill(stub, "s")
		server := newSkillsServer(stub)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/skills/s/skill.md", nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(fiber.StatusOK))
		Expect(resp.Header.Get("Content-Type")).To(ContainSubstring("text/markdown"))
		Expect(resp.Header.Get("Content-Disposition")).To(ContainSubstring("s.md"))
		raw, _ := io.ReadAll(resp.Body)
		Expect(string(raw)).To(ContainSubstring("name: s"))
		Expect(string(raw)).To(ContainSubstring("# body"))
		Expect(stub.downloads["s"]).To(Equal(1), "download is counted as a usage signal")
	})

	It("returns 501 when the backend does not support skills", func() {
		server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		_, status := doJSON(server, http.MethodGet, "/v1/skills/anything", "", "", "")
		Expect(status).To(Equal(fiber.StatusNotImplemented))
	})
})
