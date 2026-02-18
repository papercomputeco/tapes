package git_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/git"
)

var _ = Describe("RepoName", func() {
	It("returns a non-empty name when inside a git repo", func() {
		name := git.RepoName(context.Background())
		Expect(name).ToNot(BeEmpty())
	})
})
