// The golden-file specs live beside the suite bootstrap rather than in
// fixtures/, which holds only data: a test package with no bootstrap of its own
// is a test that never runs, and the paths below were already written against
// this directory.
package v1alpha1_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
)

var _ = Describe("Manifest", func() {
	It("canonicalizes set-like arrays without mutating the manifest", func() {
		metadata, err := os.ReadFile("fixtures/summary.json")
		Expect(err).NotTo(HaveOccurred())

		first, err := v1alpha1.Parse(metadata)
		Expect(err).NotTo(HaveOccurred())
		second, err := v1alpha1.Parse(metadata)
		Expect(err).NotTo(HaveOccurred())
		second.Depends.Views[0], second.Depends.Views[1] = second.Depends.Views[1], second.Depends.Views[0]
		second.Config[0], second.Config[2] = second.Config[2], second.Config[0]
		originalFirstView := first.Depends.Views[0]

		firstJSON, err := first.MarshalCanonical()
		Expect(err).NotTo(HaveOccurred())
		golden, err := os.ReadFile("fixtures/summary.json")
		Expect(err).NotTo(HaveOccurred())
		Expect(firstJSON).To(Equal(golden))
		secondJSON, err := second.MarshalCanonical()
		Expect(err).NotTo(HaveOccurred())
		Expect(secondJSON).To(Equal(firstJSON))
		Expect(first.Depends.Views[0]).To(Equal(originalFirstView))

		firstDigest, err := first.Digest()
		Expect(err).NotTo(HaveOccurred())
		secondDigest, err := second.Digest()
		Expect(err).NotTo(HaveOccurred())
		Expect(secondDigest).To(Equal(firstDigest))
		Expect(string(firstDigest)).To(MatchRegexp(`^sha256:[0-9a-f]{64}$`))
	})
})
