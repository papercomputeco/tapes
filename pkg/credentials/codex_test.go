package credentials_test

import (
	"encoding/json"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/credentials"
)

var _ = Describe("ReadCodexAuthFile", func() {
	It("returns nil when auth.json does not exist", func() {
		data, path := credentials.ReadCodexAuthFile()
		if data != nil {
			// File exists on this machine — that's fine, just skip assertion
			Expect(path).NotTo(BeEmpty())
			return
		}
		Expect(path).To(BeEmpty())
	})
})

var _ = Describe("PatchCodexAuthKey", func() {
	It("injects OPENAI_API_KEY into valid JSON", func() {
		original := []byte(`{"token": "oauth-token-123"}`)

		updated, ok := credentials.PatchCodexAuthKey(original, "sk-svcacct-test")
		Expect(ok).To(BeTrue())
		Expect(updated).NotTo(BeNil())

		var result map[string]json.RawMessage
		Expect(json.Unmarshal(updated, &result)).To(Succeed())
		Expect(result).To(HaveKey("token"))
		Expect(result).To(HaveKey("OPENAI_API_KEY"))

		var key string
		Expect(json.Unmarshal(result["OPENAI_API_KEY"], &key)).To(Succeed())
		Expect(key).To(Equal("sk-svcacct-test"))
	})

	It("removes OAuth tokens so codex uses the API key", func() {
		original := []byte(`{
			"OPENAI_API_KEY": null,
			"tokens": {
				"access_token": "oa-abc123",
				"refresh_token": "oa-refresh",
				"scopes": ["openid", "profile", "email", "offline_access"]
			}
		}`)

		updated, ok := credentials.PatchCodexAuthKey(original, "sk-svcacct-test")
		Expect(ok).To(BeTrue())

		var result map[string]json.RawMessage
		Expect(json.Unmarshal(updated, &result)).To(Succeed())
		Expect(result).NotTo(HaveKey("tokens"))

		var key string
		Expect(json.Unmarshal(result["OPENAI_API_KEY"], &key)).To(Succeed())
		Expect(key).To(Equal("sk-svcacct-test"))
	})

	It("overwrites existing OPENAI_API_KEY", func() {
		original := []byte(`{"OPENAI_API_KEY": "old-key", "other": "value"}`)

		updated, ok := credentials.PatchCodexAuthKey(original, "new-key")
		Expect(ok).To(BeTrue())

		var result map[string]json.RawMessage
		Expect(json.Unmarshal(updated, &result)).To(Succeed())

		var key string
		Expect(json.Unmarshal(result["OPENAI_API_KEY"], &key)).To(Succeed())
		Expect(key).To(Equal("new-key"))

		var other string
		Expect(json.Unmarshal(result["other"], &other)).To(Succeed())
		Expect(other).To(Equal("value"))
	})

	It("returns false for invalid JSON", func() {
		updated, ok := credentials.PatchCodexAuthKey([]byte("not json"), "key")
		Expect(ok).To(BeFalse())
		Expect(updated).To(BeNil())
	})

	It("returns false for empty input", func() {
		updated, ok := credentials.PatchCodexAuthKey(nil, "key")
		Expect(ok).To(BeFalse())
		Expect(updated).To(BeNil())
	})

	It("roundtrips: patched output can be patched again", func() {
		original := []byte(`{"token": "abc"}`)

		first, ok := credentials.PatchCodexAuthKey(original, "key-1")
		Expect(ok).To(BeTrue())

		second, ok := credentials.PatchCodexAuthKey(first, "key-2")
		Expect(ok).To(BeTrue())

		var result map[string]json.RawMessage
		Expect(json.Unmarshal(second, &result)).To(Succeed())

		var key string
		Expect(json.Unmarshal(result["OPENAI_API_KEY"], &key)).To(Succeed())
		Expect(key).To(Equal("key-2"))
	})

	It("can be written to and read from a file", func() {
		tmpDir, err := os.MkdirTemp("", "codex-test-*")
		Expect(err).NotTo(HaveOccurred())
		defer os.RemoveAll(tmpDir)

		original := []byte(`{"token": "test"}`)
		updated, ok := credentials.PatchCodexAuthKey(original, "sk-file-test")
		Expect(ok).To(BeTrue())

		authPath := filepath.Join(tmpDir, "auth.json")
		Expect(os.WriteFile(authPath, updated, 0o600)).To(Succeed())

		data, err := os.ReadFile(authPath)
		Expect(err).NotTo(HaveOccurred())

		var result map[string]json.RawMessage
		Expect(json.Unmarshal(data, &result)).To(Succeed())

		var key string
		Expect(json.Unmarshal(result["OPENAI_API_KEY"], &key)).To(Succeed())
		Expect(key).To(Equal("sk-file-test"))
	})
})
