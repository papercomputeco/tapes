package git_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/git"
)

var _ = Describe("RepoName", func() {
	It("returns a non-empty name when inside a git repo", func() {
		name := git.RepoName()
		Expect(name).ToNot(BeEmpty())
	})
})
