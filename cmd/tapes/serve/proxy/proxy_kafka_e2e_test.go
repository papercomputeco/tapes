package proxycmder

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/papercomputeco/tapes/pkg/publisher"
)

var _ = Describe("serve proxy kafka e2e", func() {
	It("publishes one-turn conversation events to kafka", func() {
		brokersRaw := strings.TrimSpace(os.Getenv("TAPES_E2E_KAFKA_BROKERS"))
		topic := strings.TrimSpace(os.Getenv("TAPES_E2E_KAFKA_TOPIC"))
		proxyURL := strings.TrimSpace(os.Getenv("TAPES_E2E_PROXY_URL"))
		if brokersRaw == "" || topic == "" || proxyURL == "" {
			Skip("set TAPES_E2E_KAFKA_BROKERS, TAPES_E2E_KAFKA_TOPIC, and TAPES_E2E_PROXY_URL to run kafka e2e")
		}

		brokers := splitKafkaBrokers(brokersRaw)
		Expect(brokers).NotTo(BeEmpty())

		proxyBaseURL, err := url.Parse(proxyURL)
		Expect(err).NotTo(HaveOccurred())
		Expect(proxyBaseURL.Scheme).NotTo(BeEmpty())
		Expect(proxyBaseURL.Host).NotTo(BeEmpty())

		Eventually(func() error {
			conn, err := kafkago.Dial("tcp", brokers[0])
			if err != nil {
				return err
			}
			return conn.Close()
		}, 30*time.Second, 250*time.Millisecond).Should(Succeed())

		Eventually(func() error {
			conn, err := kafkago.DialLeader(context.Background(), "tcp", brokers[0], topic, 0)
			if err != nil {
				return err
			}
			return conn.Close()
		}, 30*time.Second, 250*time.Millisecond).Should(Succeed())

		reader := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: 0,
			MinBytes:  1,
			MaxBytes:  10e6,
			MaxWait:   500 * time.Millisecond,
		})
		DeferCleanup(func() { _ = reader.Close() })
		Expect(reader.SetOffset(kafkago.LastOffset)).To(Succeed())

		requestBody := []byte(`{"model":"llama3.2","stream":false,"messages":[{"role":"system","content":"You are helpful."},{"role":"user","content":"Say hello."}]}`)
		reqCtx, reqCancel := context.WithTimeout(context.Background(), 10*time.Second)
		DeferCleanup(reqCancel)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, proxyBaseURL.JoinPath("api/chat").String(), bytes.NewReader(requestBody))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = resp.Body.Close() })

		respPayload, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK), string(respPayload))

		events, keys, err := readKafkaEvents(reader, 3, 30*time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(events).To(HaveLen(3))

		rootHash := events[0].RootHash
		Expect(rootHash).NotTo(BeEmpty())

		for i, event := range events {
			Expect(event.Schema).To(Equal(publisher.SchemaNodeV1))
			Expect(event.RootHash).To(Equal(rootHash))
			Expect(event.Node.Hash).NotTo(BeEmpty())
			Expect(string(keys[i])).To(Equal(rootHash))
		}
	})
})

func readKafkaEvents(reader *kafkago.Reader, expectedCount int, timeout time.Duration) ([]publisher.Event, [][]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	events := make([]publisher.Event, 0, expectedCount)
	keys := make([][]byte, 0, expectedCount)

	for len(events) < expectedCount {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return nil, nil, err
		}

		var event publisher.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return nil, nil, err
		}

		events = append(events, event)
		keys = append(keys, append([]byte(nil), msg.Key...))
	}

	return events, keys, nil
}
