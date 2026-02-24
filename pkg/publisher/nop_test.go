package publisher

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NopPublisher", func() {
	It("implements Publisher", func() {
		var p Publisher = NewNopPublisher()
		Expect(p).NotTo(BeNil())
	})

	It("returns nil from Publish", func() {
		p := NewNopPublisher()
		event, err := NewEvent("root-hash-123", buildNodeForEvent())
		Expect(err).NotTo(HaveOccurred())

		err = p.Publish(context.Background(), event)
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns nil from Close and is safe to call multiple times", func() {
		p := NewNopPublisher()
		Expect(p.Close()).To(Succeed())
		Expect(p.Close()).To(Succeed())
	})
})
