package update

import (
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func versionServer(body string, status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

var _ = Describe("CheckForUpdate", func() {
	Describe("shouldSkip", func() {
		It("skips empty version", func() {
			Expect(shouldSkip("")).To(BeTrue())
		})

		It("skips dev builds", func() {
			Expect(shouldSkip("dev")).To(BeTrue())
		})

		It("skips nightly builds", func() {
			Expect(shouldSkip("nightly")).To(BeTrue())
		})

		It("skips invalid semver", func() {
			Expect(shouldSkip("not-a-version")).To(BeTrue())
		})

		It("does not skip valid semver", func() {
			Expect(shouldSkip("v1.0.0")).To(BeFalse())
		})

		It("does not skip prerelease semver", func() {
			Expect(shouldSkip("v1.0.0-rc.1")).To(BeFalse())
		})
	})

	Describe("checkForUpdate", func() {
		It("returns remote version when remote is newer", func() {
			srv := versionServer("v2.0.0\n", http.StatusOK)
			defer srv.Close()

			result := checkForUpdate("v1.0.0", srv.URL)
			Expect(result).To(Equal("v2.0.0"))
		})

		It("returns empty when current equals remote", func() {
			srv := versionServer("v1.0.0\n", http.StatusOK)
			defer srv.Close()

			Expect(checkForUpdate("v1.0.0", srv.URL)).To(BeEmpty())
		})

		It("returns empty when current is newer than remote", func() {
			srv := versionServer("v1.0.0\n", http.StatusOK)
			defer srv.Close()

			Expect(checkForUpdate("v2.0.0", srv.URL)).To(BeEmpty())
		})

		It("returns empty for dev builds", func() {
			srv := versionServer("v2.0.0\n", http.StatusOK)
			defer srv.Close()

			Expect(checkForUpdate("dev", srv.URL)).To(BeEmpty())
		})

		It("returns empty when server returns non-200", func() {
			srv := versionServer("v2.0.0\n", http.StatusInternalServerError)
			defer srv.Close()

			Expect(checkForUpdate("v1.0.0", srv.URL)).To(BeEmpty())
		})

		It("returns empty when server is unreachable", func() {
			Expect(checkForUpdate("v1.0.0", "http://127.0.0.1:1")).To(BeEmpty())
		})

		It("returns empty when server returns invalid semver", func() {
			srv := versionServer("not-a-version\n", http.StatusOK)
			defer srv.Close()

			Expect(checkForUpdate("v1.0.0", srv.URL)).To(BeEmpty())
		})

		It("returns empty when server returns empty body", func() {
			srv := versionServer("", http.StatusOK)
			defer srv.Close()

			Expect(checkForUpdate("v1.0.0", srv.URL)).To(BeEmpty())
		})

		It("handles version with trailing whitespace", func() {
			srv := versionServer("  v2.0.0  \n", http.StatusOK)
			defer srv.Close()

			result := checkForUpdate("v1.0.0", srv.URL)
			Expect(result).To(Equal("v2.0.0"))
		})

		It("detects patch updates", func() {
			srv := versionServer("v1.0.1\n", http.StatusOK)
			defer srv.Close()

			result := checkForUpdate("v1.0.0", srv.URL)
			Expect(result).To(Equal("v1.0.1"))
		})
	})
})
