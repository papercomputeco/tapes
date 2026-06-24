package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/gofiber/fiber/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// skillsStubDriver implements the unexported skillStore capability with canned
// responses, recording the org it was scoped to so specs can assert org
// threading and that validation short-circuits before any storage call.
type skillsStubDriver struct {
	storage.Driver

	getRecord   *storage.SkillRecord
	getErr      error
	lastGetOrg  string
	lastGetSlug string

	upsertCalls   int
	lastUpsertOrg string
}

func (d *skillsStubDriver) GetSkill(_ context.Context, orgID, slug string) (*storage.SkillRecord, error) {
	d.lastGetOrg = orgID
	d.lastGetSlug = slug
	return d.getRecord, d.getErr
}

func (d *skillsStubDriver) UpsertSkill(_ context.Context, orgID string, rec storage.SkillRecord) (*storage.SkillRecord, error) {
	d.upsertCalls++
	d.lastUpsertOrg = orgID
	return &rec, nil
}

func getSkill(server *Server, path, org string) (map[string]any, llm.ErrorResponse, int) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	Expect(err).NotTo(HaveOccurred())
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())

	var body map[string]any
	var errBody llm.ErrorResponse
	if resp.StatusCode == fiber.StatusOK {
		Expect(json.Unmarshal(raw, &body)).To(Succeed())
	} else {
		Expect(json.Unmarshal(raw, &errBody)).To(Succeed())
	}
	return body, errBody, resp.StatusCode
}

func postGenerate(server *Server, body, org string) (llm.ErrorResponse, int) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/skills/generate", bytes.NewBufferString(body))
	Expect(err).NotTo(HaveOccurred())
	req.Header.Set("Content-Type", "application/json")
	if org != "" {
		req.Header.Set(orgIDHeader, org)
	}
	resp, err := server.app.Test(req)
	Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	Expect(err).NotTo(HaveOccurred())
	var errBody llm.ErrorResponse
	_ = json.Unmarshal(raw, &errBody)
	return errBody, resp.StatusCode
}

var _ = Describe("Skills handlers", func() {
	It("returns a camelCase draft for a persisted skill, scoped to the org", func() {
		stub := &skillsStubDriver{getRecord: &storage.SkillRecord{
			Slug:                    "debug-react-hooks",
			Name:                    "Debug React Hooks",
			Description:             "desc",
			Type:                    "workflow",
			Version:                 "0.1.0",
			Visibility:              "private",
			Tags:                    []string{"react"},
			Content:                 "# body",
			IsAIGenerated:           true,
			GeneratedFromSessionIDs: []string{"sess-1"},
		}}
		server, err := NewServer(Config{ListenAddr: ":0"}, stub, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())

		org := "11111111-1111-1111-1111-111111111111"
		body, _, status := getSkill(server, "/v1/skills/debug-react-hooks", org)
		Expect(status).To(Equal(fiber.StatusOK))
		Expect(stub.lastGetOrg).To(Equal(org), "the read must be scoped to the requested tenant")
		Expect(stub.lastGetSlug).To(Equal("debug-react-hooks"))
		Expect(body).To(HaveKeyWithValue("slug", "debug-react-hooks"))
		Expect(body).To(HaveKeyWithValue("isAiGenerated", true))
		Expect(body).To(HaveKey("generatedFromSessionIds"))
		Expect(body).To(HaveKeyWithValue("parentSlug", BeNil()))
	})

	It("returns 404 when the skill is absent", func() {
		stub := &skillsStubDriver{getRecord: nil}
		server, err := NewServer(Config{ListenAddr: ":0"}, stub, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		_, errBody, status := getSkill(server, "/v1/skills/missing", "")
		Expect(status).To(Equal(fiber.StatusNotFound))
		Expect(errBody.Error).NotTo(BeEmpty())
	})

	It("rejects generate with no sessionIds before touching storage", func() {
		stub := &skillsStubDriver{}
		server, err := NewServer(Config{ListenAddr: ":0"}, stub, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		errBody, status := postGenerate(server, `{"sessionIds":[]}`, "")
		Expect(status).To(Equal(fiber.StatusBadRequest))
		Expect(errBody.Error).To(ContainSubstring("sessionIds"))
		Expect(stub.upsertCalls).To(Equal(0), "validation must short-circuit before persistence")
	})

	It("returns 501 when the backend does not support skills", func() {
		server, err := NewServer(Config{ListenAddr: ":0"}, inmemory.NewDriver(), tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		_, errBody, status := getSkill(server, "/v1/skills/anything", "")
		Expect(status).To(Equal(fiber.StatusNotImplemented))
		Expect(errBody.Error).To(ContainSubstring("not supported"))
	})
})
