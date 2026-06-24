package skill_test

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/skill"
)

var _ = Describe("NewLLMCaller", func() {
	// Skill generation borrows the search/embedding key, so "no key"
	// means search is disabled for the tenant — the handler maps the
	// sentinel to a 422, not a 500. Clear the env so resolution can't
	// pick up an ambient key on the test host.
	BeforeEach(func() {
		GinkgoT().Setenv("OPENAI_API_KEY", "")
		GinkgoT().Setenv("ANTHROPIC_API_KEY", "")
	})

	It("returns ErrNoAPIKey when no key resolves for a chat provider", func() {
		_, err := skill.NewLLMCaller(skill.LLMCallerConfig{Provider: "openai"})
		Expect(errors.Is(err, skill.ErrNoAPIKey)).To(BeTrue())
	})

	It("succeeds when an explicit key is supplied", func() {
		caller, err := skill.NewLLMCaller(skill.LLMCallerConfig{Provider: "openai", APIKey: "sk-test"})
		Expect(err).NotTo(HaveOccurred())
		Expect(caller).NotTo(BeNil())
	})

	It("does not require a key for ollama", func() {
		caller, err := skill.NewLLMCaller(skill.LLMCallerConfig{Provider: "ollama"})
		Expect(err).NotTo(HaveOccurred())
		Expect(caller).NotTo(BeNil())
	})
})
