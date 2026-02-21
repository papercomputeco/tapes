package nop_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/eventstream"
	"github.com/papercomputeco/tapes/pkg/eventstream/nop"
)

var _ = Describe("Publisher", func() {
	It("creates a non-nil publisher", func() {
		p := nop.NewPublisher()
		Expect(p).NotTo(BeNil())
	})

	It("returns ErrNilTurnEvent for nil events", func() {
		p := nop.NewPublisher()
		err := p.PublishTurn(context.Background(), nil)
		Expect(err).To(MatchError(eventstream.ErrNilTurnEvent))
	})

	It("succeeds for non-nil events", func() {
		p := nop.NewPublisher()
		err := p.PublishTurn(context.Background(), &eventstream.TurnPersistedEvent{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("closes successfully", func() {
		p := nop.NewPublisher()
		Expect(p.Close()).To(Succeed())
	})
})
