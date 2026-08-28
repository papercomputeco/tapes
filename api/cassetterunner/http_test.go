package cassetterunner

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("RewriteProxyRequest", func() {
	// rewrite applies the shared proxy policy to a fresh request and returns
	// the outgoing URL, which is where the escaped form either survives the
	// hop or is destroyed by re-escaping.
	rewrite := func(path string) *url.URL {
		inbound, err := http.NewRequest(http.MethodGet, "http://core.example"+path, nil)
		Expect(err).NotTo(HaveOccurred())
		target, err := url.Parse("http://cassette.example:9000")
		Expect(err).NotTo(HaveOccurred())

		outbound := inbound.Clone(inbound.Context())
		RewriteProxyRequest(&httputil.ProxyRequest{In: inbound, Out: outbound}, target, path, "summary")

		return outbound.URL
	}

	It("passes a plain path through unchanged", func() {
		Expect(rewrite("/api/summary/reports/7").EscapedPath()).To(Equal("/api/summary/reports/7"))
	})

	It("keeps percent-encoded segments in their escaped form", func() {
		out := rewrite("/api/summary/things/My%20cool%20name")
		Expect(out.EscapedPath()).To(Equal("/api/summary/things/My%20cool%20name"),
			"re-escaping an already-escaped path double-encodes every %XX for the cassette")
		Expect(out.Path).To(Equal("/api/summary/things/My cool name"))
	})

	It("keeps multibyte segments in their escaped form", func() {
		out := rewrite("/api/summary/things/%F0%9F%8E%B8")
		Expect(out.EscapedPath()).To(Equal("/api/summary/things/%F0%9F%8E%B8"))
		Expect(out.Path).To(Equal("/api/summary/things/\U0001F3B8"))
	})

	It("keeps an escaped slash from becoming a separator", func() {
		out := rewrite("/api/summary/things/a%2Fb")
		Expect(out.EscapedPath()).To(Equal("/api/summary/things/a%2Fb"))
		Expect(out.Path).To(Equal("/api/summary/things/a/b"))
	})
})
