package worker

import (
	"context"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/derive"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// splitCaptureDriver adds the idle-split capability to captureDriver.
type splitCaptureDriver struct {
	*captureDriver

	mu      sync.Mutex
	assigns []storage.SyntheticSessionRequest
}

func (d *splitCaptureDriver) AssignSyntheticSession(_ context.Context, req storage.SyntheticSessionRequest) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.assigns = append(d.assigns, req)
	return req.Conversation + "-split", nil
}

var _ = Describe("Worker pool synthetic session idle split", func() {
	newPool := func(driver storage.Driver, gap time.Duration) *Pool {
		wp, err := NewPool(&Config{Driver: driver, Logger: tapeslogger.NewNoop(), SessionIdleGap: gap})
		Expect(err).NotTo(HaveOccurred())
		return wp
	}

	It("keys the raw turn and the sessions row by the same idle split", func() {
		driver := &splitCaptureDriver{captureDriver: newCaptureDriver()}
		wp := newPool(driver, 30*time.Minute)
		wp.Enqueue(sampleCaptureJob(&sessions.IngestEnvelope{}, rawBody))
		wp.Close()

		Expect(driver.assigns).To(HaveLen(1))
		Expect(driver.assigns[0].HarnessID).To(Equal(sessions.HarnessIDUnknown))
		want := driver.assigns[0].Conversation + "-split"

		raws := driver.RawTurns()
		Expect(raws).To(HaveLen(1))
		Expect(raws[0].HarnessSessionID).To(Equal(want))
		calls := driver.IngestCalls()
		Expect(calls).To(HaveLen(1))
		Expect(calls[0].SessionKey).To(Equal(want))
	})

	It("places the turn by its capture time, not the time the worker runs", func() {
		driver := &splitCaptureDriver{captureDriver: newCaptureDriver()}
		wp := newPool(driver, 30*time.Minute)
		capturedAt := time.Date(2026, 10, 13, 9, 0, 0, 123456000, time.UTC)
		job := sampleCaptureJob(&sessions.IngestEnvelope{}, rawBody)
		job.CapturedAt = capturedAt
		wp.Enqueue(job)
		wp.Close()

		Expect(driver.assigns).To(HaveLen(1))
		Expect(driver.assigns[0].CapturedAt).To(BeTemporally("==", capturedAt))
		raws := driver.RawTurns()
		Expect(raws).To(HaveLen(1))
		Expect(derive.CapturedAt(&raws[0])).To(BeTemporally("==", capturedAt),
			"the raw turn is dated by the instant the split compared")
	})

	It("never splits a harness-provided session id", func() {
		driver := &splitCaptureDriver{captureDriver: newCaptureDriver()}
		wp := newPool(driver, 30*time.Minute)
		wp.Enqueue(sampleCaptureJob(&sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: "harness-abc"}, rawBody))
		wp.Close()

		Expect(driver.assigns).To(BeEmpty())
		Expect(driver.RawTurns()[0].HarnessSessionID).To(Equal("harness-abc"))
	})

	It("keeps the bare conversation key when the split is disabled", func() {
		driver := &splitCaptureDriver{captureDriver: newCaptureDriver()}
		wp := newPool(driver, 0)
		wp.Enqueue(sampleCaptureJob(&sessions.IngestEnvelope{}, rawBody))
		wp.Close()

		Expect(driver.assigns).To(BeEmpty())
		Expect(driver.RawTurns()[0].HarnessSessionID).To(HaveLen(sessions.SyntheticHarnessSessionIDPrefixLen))
	})
})
