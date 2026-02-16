package credentials_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/credentials"
)

var _ = Describe("ReadOpenCodeAuthFile", func() {
	It("returns nil when auth.json does not exist on a clean machine", func() {
		data, path := credentials.ReadOpenCodeAuthFile()
		if data != nil {
			// File exists on this machine — that's fine, just skip assertion.
			Expect(path).NotTo(BeEmpty())
			return
		}
		Expect(path).To(BeEmpty())
	})
})

var _ = Describe("PatchOpenCodeAuth", func() {
	It("removes specified provider OAuth entries", func() {
		original := []byte(`{
			"openai": {"type": "oauth", "access": "token123", "refresh": "rt_abc"},
			"anthropic": {"type": "oauth", "access": "sk-ant-token"}
		}`)

		updated, ok := credentials.PatchOpenCodeAuth(original, []string{"openai", "anthropic"})
		Expect(ok).To(BeTrue())

		var result map[string]json.RawMessage
		Expect(json.Unmarshal(updated, &result)).To(Succeed())
		Expect(result).NotTo(HaveKey("openai"))
		Expect(result).NotTo(HaveKey("anthropic"))
	})

	It("preserves unrelated entries", func() {
		original := []byte(`{
			"openai": {"type": "oauth", "access": "token123"},
			"custom-provider": {"type": "apikey", "key": "sk-custom"}
		}`)

		updated, ok := credentials.PatchOpenCodeAuth(original, []string{"openai"})
		Expect(ok).To(BeTrue())

		var result map[string]json.RawMessage
		Expect(json.Unmarshal(updated, &result)).To(Succeed())
		Expect(result).NotTo(HaveKey("openai"))
		Expect(result).To(HaveKey("custom-provider"))
	})

	It("handles empty provider list", func() {
		original := []byte(`{"openai": {"type": "oauth"}}`)

		updated, ok := credentials.PatchOpenCodeAuth(original, []string{})
		Expect(ok).To(BeTrue())

		var result map[string]json.RawMessage
		Expect(json.Unmarshal(updated, &result)).To(Succeed())
		Expect(result).To(HaveKey("openai"))
	})

	It("returns false for invalid JSON", func() {
		updated, ok := credentials.PatchOpenCodeAuth([]byte("not json"), []string{"openai"})
		Expect(ok).To(BeFalse())
		Expect(updated).To(BeNil())
	})

	It("returns false for nil input", func() {
		updated, ok := credentials.PatchOpenCodeAuth(nil, []string{"openai"})
		Expect(ok).To(BeFalse())
		Expect(updated).To(BeNil())
	})

	It("is safe when provider does not exist in auth", func() {
		original := []byte(`{"anthropic": {"type": "oauth"}}`)

		updated, ok := credentials.PatchOpenCodeAuth(original, []string{"openai"})
		Expect(ok).To(BeTrue())

		var result map[string]json.RawMessage
		Expect(json.Unmarshal(updated, &result)).To(Succeed())
		Expect(result).To(HaveKey("anthropic"))
	})
})
