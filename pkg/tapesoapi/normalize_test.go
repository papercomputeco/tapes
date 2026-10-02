package tapesoapi_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

var _ = Describe("NormalizePath", func() {
	DescribeTable("accepts well-formed paths",
		func(path string) {
			normalized, err := tapesoapi.NormalizePath(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(normalized).To(Equal(path))
		},
		Entry("root", "/"),
		Entry("plain", "/v1/sessions"),
		Entry("one parameter", "/v1/sessions/{id}"),
		Entry("two parameters", "/v1/sessions/{id}/traces/{traceId}"),
		Entry("dashed name", "/v1/sessions/{session-id}"),
		Entry("underscored name", "/v1/sessions/{session_id}"),
		Entry("adjacent parameters", "/v1/{a}{b}"),
	)

	DescribeTable("rejects malformed paths",
		func(path, reason string) {
			_, err := tapesoapi.NormalizePath(path)
			Expect(err).To(HaveOccurred(), "expected %q to be rejected: %s", path, reason)
		},
		Entry("empty", "", "no path at all"),
		Entry("relative", "v1/sessions", "must be absolute"),
		Entry("unnamed parameter", "/v1/sessions/{}", "no name between the braces"),
		Entry("unclosed brace", "/v1/sessions/{id", "one { against no }"),
		Entry("unopened brace", "/v1/sessions/id}", "no { against one }"),
		Entry("parameter carrying a slash", "/v1/{b/c}", "the grammar excludes / from a name"),
		Entry("one good and one slash-bearing parameter", "/v1/{a}{b/c}", "{b/c} is not a parameter"),
	)

	It("rejects a parameter carrying a slash rather than declaring none", func() {
		// The brace-balance check passes on this path, one { against one }, so
		// it used to be accepted. PathParams then reported no parameters for a
		// path that visibly has one, and the operation published with an
		// undeclared {b/c} segment.
		_, err := tapesoapi.NormalizePath("/v1/{b/c}")
		Expect(err).To(MatchError(ContainSubstring("malformed template parameter")))
		Expect(tapesoapi.PathParams("/v1/{b/c}")).To(BeEmpty())
	})

	It("still reports the parameter names of an accepted path", func() {
		Expect(tapesoapi.PathParams("/v1/sessions/{id}/traces/{traceId}")).
			To(Equal([]string{"id", "traceId"}))
	})

	It("trims a trailing slash without disturbing the parameters", func() {
		normalized, err := tapesoapi.NormalizePath("/v1/sessions/{id}/")
		Expect(err).NotTo(HaveOccurred())
		Expect(normalized).To(Equal("/v1/sessions/{id}"))
		Expect(tapesoapi.PathParams(normalized)).To(Equal([]string{"id"}))
	})
})
