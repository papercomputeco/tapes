package api

// The probe seam between the server and the runner: the server adapts its
// driver's optional probe capability into the plain-string function the
// runner takes, keeping the runner storage-free while the parse discipline —
// only validated identifiers reach the store — holds on this path exactly as
// it does on the request path.

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/api/cassetterunner"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

// probeCapableDriver wraps a real driver with a scriptable probe, standing in
// for the Postgres driver's capability.
type probeCapableDriver struct {
	storage.Driver

	err    error
	calls  int
	view   storage.PublishedViewName
	column storage.PublishedColumnName
}

func (d *probeCapableDriver) ProbePublishedView(
	_ context.Context,
	view storage.PublishedViewName,
	column storage.PublishedColumnName,
) error {
	d.calls++
	d.view = view
	d.column = column

	return d.err
}

var _ = Describe("publishedViewProbe", func() {
	It("hands the runner no probe when the driver has no probe capability", func() {
		Expect(publishedViewProbe(inmemory.NewDriver())).To(BeNil(),
			"nil is the runner's arm-everything signal — the pre-probe behavior for such backends")
	})

	It("parses the claim strings before they reach the store", func() {
		driver := &probeCapableDriver{Driver: inmemory.NewDriver()}
		probe := publishedViewProbe(driver)
		Expect(probe).NotTo(BeNil())

		Expect(probe(context.Background(), "testpub.attachments", "value")).To(Succeed())
		Expect(driver.calls).To(Equal(1))
		Expect(driver.view.String()).To(Equal("testpub.attachments"))
		Expect(driver.column.String()).To(Equal("value"))

		Expect(probe(context.Background(), "not-an-identifier", "value")).NotTo(Succeed())
		Expect(probe(context.Background(), "testpub.attachments", "Bad Column")).NotTo(Succeed())
		Expect(driver.calls).To(Equal(1),
			"a manifest string that fails the grammar must never reach the store")
	})

	It("returns the store's own verdict for a parsed probe", func() {
		driver := &probeCapableDriver{Driver: inmemory.NewDriver(), err: errors.New("permission denied")}
		probe := publishedViewProbe(driver)

		err := probe(context.Background(), "testpub.attachments", "value")
		Expect(err).To(MatchError(ContainSubstring("permission denied")))
	})
})

var _ = Describe("publishedViewProbe outcome classification", func() {
	It("marks a store-never-answered failure transient for the runner", func() {
		driver := &probeCapableDriver{
			Driver: inmemory.NewDriver(),
			err:    &storage.TransientProbeError{Err: errors.New("context deadline exceeded")},
		}
		probe := publishedViewProbe(driver)

		err := probe(context.Background(), "testpub.attachments", "value")
		var transient *cassetterunner.TransientProbeError
		Expect(errors.As(err, &transient)).To(BeTrue(),
			"no verdict from the store must reach the runner as no-verdict, never as a refusal")
		Expect(err.Error()).To(ContainSubstring("context deadline exceeded"))
	})

	It("passes a definitive store verdict through un-marked", func() {
		driver := &probeCapableDriver{Driver: inmemory.NewDriver(), err: errors.New("permission denied")}
		probe := publishedViewProbe(driver)

		err := probe(context.Background(), "testpub.attachments", "value")
		var transient *cassetterunner.TransientProbeError
		Expect(errors.As(err, &transient)).To(BeFalse(),
			"the store answered; the runner must treat the refusal as definitive")
	})

	It("treats a claim string that fails the grammar as definitive", func() {
		driver := &probeCapableDriver{Driver: inmemory.NewDriver()}
		probe := publishedViewProbe(driver)

		err := probe(context.Background(), "not-an-identifier", "value")
		var transient *cassetterunner.TransientProbeError
		Expect(err).To(HaveOccurred())
		Expect(errors.As(err, &transient)).To(BeFalse(),
			"a malformed declaration can never heal on retry; retention would hide it forever")
	})
})
