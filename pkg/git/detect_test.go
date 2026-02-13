package git_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/git"
)

var _ = Describe("RepoName", func() {
	It("returns the repository name when inside a git repo", func() {
		name := git.RepoName()
		Expect(name).ToNot(BeEmpty())
		// Running inside the tapes repo itself
		Expect(name).To(Equal("tapes"))
	})
})
