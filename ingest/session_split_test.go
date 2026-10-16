package ingest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/llm"
	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/sessions"
	"github.com/papercomputeco/tapes/pkg/storage"
)

// splitDriver adds the idle-split capability to captureDriver, answering
// every assignment with a fixed split of the requested conversation and
// recording what it was asked.
type splitDriver struct {
	*captureDriver

	mu      sync.Mutex
	assigns []storage.SyntheticSessionRequest
}

func (d *splitDriver) AssignSyntheticSession(_ context.Context, req storage.SyntheticSessionRequest) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.assigns = append(d.assigns, req)
	return req.Conversation + "-split", nil
}

func (d *splitDriver) Assigns() []storage.SyntheticSessionRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]storage.SyntheticSessionRequest(nil), d.assigns...)
}

func (d *splitDriver) IngestCalls() []storage.IngestTurnRequest {
	d.captureDriver.mu.Lock()
	defer d.captureDriver.mu.Unlock()
	return append([]storage.IngestTurnRequest(nil), d.ingestCalls...)
}

var _ = Describe("Ingest synthetic session idle split", func() {
	var (
		driver  *splitDriver
		baseURL string
	)

	BeforeEach(func() {
		driver = &splitDriver{captureDriver: newCaptureDriver()}
		s, err := ingest.New(ingest.Config{
			ListenAddr:     ":0",
			Project:        "test-project",
			SessionIdleGap: 30 * time.Minute,
		}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		go func() { _ = s.RunWithListener(ln) }()
		DeferCleanup(s.Close)
		baseURL = "http://" + ln.Addr().String()
	})

	post := func(requestID string, session *sessions.IngestEnvelope) {
		body, err := json.Marshal(ingest.TurnPayload{
			Provider:  "openai",
			AgentName: "bot",
			RawRequest: mustJSON(openaiRequest{
				Model:    "gpt-test",
				Messages: []openaiMessage{{Role: "system", Content: "you are a channel bot"}, {Role: "user", Content: "hello"}},
			}),
			Response: reducedResponse("gpt-test", "hi", &llm.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2}),
			Meta:     ingest.TurnMeta{RequestID: requestID, TsRequest: "2026-10-13T09:00:00Z"},
			Session:  session,
		})
		Expect(err).NotTo(HaveOccurred())
		resp, err := http.Post(baseURL+"/v1/ingest", "application/json", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusAccepted))
	}

	It("keys a turn without a harness session id by its idle split, raw turn and session alike", func() {
		post("req-bot-1", &sessions.IngestEnvelope{HarnessID: "hermes"})

		Eventually(driver.IngestCalls).WithTimeout(2 * time.Second).Should(HaveLen(1))
		Expect(driver.Assigns()).To(HaveLen(1))
		assign := driver.Assigns()[0]
		Expect(assign.HarnessID).To(Equal("hermes"))
		Expect(assign.Conversation).To(HaveLen(sessions.SyntheticHarnessSessionIDPrefixLen))
		Expect(assign.RequestID).To(Equal("req-bot-1"))
		Expect(assign.CapturedAt).To(BeTemporally("==", time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)),
			"the split compares capture time, not arrival time")
		Expect(assign.IdleGap).To(Equal(30 * time.Minute))

		want := assign.Conversation + "-split"
		Expect(driver.RawTurns()[0].HarnessSessionID).To(Equal(want))
		Expect(driver.IngestCalls()[0].SessionKey).To(Equal(want))
	})

	It("keys a turn that arrived with no session block like an envelope-less local capture", func() {
		post("req-bare-1", nil)

		Eventually(driver.IngestCalls).WithTimeout(2 * time.Second).Should(HaveLen(1))
		Expect(driver.Assigns()).To(HaveLen(1))
		assign := driver.Assigns()[0]
		Expect(assign.HarnessID).To(Equal(sessions.HarnessIDUnknown))
		Expect(assign.Conversation).To(HaveLen(sessions.SyntheticHarnessSessionIDPrefixLen))

		want := assign.Conversation + "-split"
		raw := driver.RawTurns()[0]
		Expect(raw.HarnessID).To(Equal(sessions.HarnessIDUnknown))
		Expect(raw.HarnessSessionID).To(Equal(want),
			"a keyed raw turn is marked for derive")
		Expect(driver.IngestCalls()[0].SessionKey).To(Equal(want),
			"the sessions row is created under the same key")
	})

	It("leaves a harness-provided session id alone", func() {
		post("req-claude-1", &sessions.IngestEnvelope{HarnessID: "claude", HarnessSessionID: "hs-provided"})

		Eventually(driver.IngestCalls).WithTimeout(2 * time.Second).Should(HaveLen(1))
		Expect(driver.Assigns()).To(BeEmpty())
		Expect(driver.RawTurns()[0].HarnessSessionID).To(Equal("hs-provided"))
		Expect(driver.IngestCalls()[0].SessionKey).To(BeEmpty())
	})
})
