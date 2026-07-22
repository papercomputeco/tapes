package api

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/storage"
)

// resolveSessionDisplayTitle owns the one-place precedence every client
// renders (PCC-970): DisplayName -> DerivedTitle -> Preview -> Name -> a short
// harness id slice -> the session id. It must never return empty.
var _ = Describe("resolveSessionDisplayTitle", func() {
	base := storage.SessionRecord{
		ID:               "019f8be5-721b-72c5-a097-3bc50287f89f",
		HarnessSessionID: "clearing-example-abcdef123",
	}

	It("prefers the user's display_name over every derived/captured field", func() {
		s := base
		s.DisplayName = "My title"
		s.DerivedTitle = "derived"
		s.Preview = "hello"
		s.Name = "slug"
		Expect(resolveSessionDisplayTitle(s)).To(Equal("My title"))
	})

	It("falls back display_name -> derived_title -> preview -> name", func() {
		s := base
		s.DerivedTitle = "derived"
		s.Preview = "hello"
		s.Name = "slug"
		Expect(resolveSessionDisplayTitle(s)).To(Equal("derived"))
		s.DerivedTitle = ""
		Expect(resolveSessionDisplayTitle(s)).To(Equal("hello"))
		s.Preview = ""
		Expect(resolveSessionDisplayTitle(s)).To(Equal("slug"))
	})

	It("skips a JSON tool-result blob preview and uses the harness id slice", func() {
		s := base
		s.Preview = `{"channel_link":"https://example.com"}`
		Expect(resolveSessionDisplayTitle(s)).To(Equal("clearing-exa"))
	})

	It("falls back to the harness id slice when nothing human is set", func() {
		Expect(resolveSessionDisplayTitle(base)).To(Equal("clearing-exa"))
	})

	It("never returns empty: uses the session id when harness_session_id is empty (contract)", func() {
		s := storage.SessionRecord{ID: "019f8be5-721b-72c5-a097-3bc50287f89f"}
		Expect(resolveSessionDisplayTitle(s)).To(Equal("019f8be5-721b-72c5-a097-3bc50287f89f"))
	})
})
