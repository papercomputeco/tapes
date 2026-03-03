package proxycmder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
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
		if brokersRaw == "" || topic == "" {
			Skip("set TAPES_E2E_KAFKA_BROKERS and TAPES_E2E_KAFKA_TOPIC to run kafka e2e")
		}

		brokers := splitKafkaBrokers(brokersRaw)
		Expect(brokers).NotTo(BeEmpty())

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

		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"model":"llama3.2","created_at":"2026-02-27T00:00:00Z","message":{"role":"assistant","content":"hello"},"done":true,"done_reason":"stop","prompt_eval_count":3,"eval_count":2}`))
		}))
		DeferCleanup(upstream.Close)

		repoRoot, err := findRepoRoot()
		Expect(err).NotTo(HaveOccurred())

		listenAddr, err := freeListenAddr(context.Background())
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)

		var stdout bytes.Buffer
		var stderr bytes.Buffer

		args := []string{
			"run", "./cli/tapes",
			"serve", "proxy",
			"--provider", "ollama",
			"--upstream", upstream.URL,
			"--listen", listenAddr,
			"--kafka-brokers", brokersRaw,
			"--kafka-topic", topic,
		}
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = repoRoot
		cmd.Env = os.Environ()
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		Expect(cmd.Start()).To(Succeed())

		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()

		DeferCleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				if cmd.Process != nil {
					_ = cmd.Process.Kill()
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
				}
			}
		})

		Eventually(func() error {
			conn, err := net.DialTimeout("tcp", listenAddr, 200*time.Millisecond)
			if err != nil {
				return err
			}
			return conn.Close()
		}, 45*time.Second, 200*time.Millisecond).Should(Succeed(), combinedProcessOutput(&stdout, &stderr))

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
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, "http://"+listenAddr+"/api/chat", bytes.NewReader(requestBody))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred(), combinedProcessOutput(&stdout, &stderr))
		DeferCleanup(func() { _ = resp.Body.Close() })

		respPayload, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK), "%s\n%s", combinedProcessOutput(&stdout, &stderr), string(respPayload))

		events, keys, err := readKafkaEvents(reader, 3, 30*time.Second)
		Expect(err).NotTo(HaveOccurred(), combinedProcessOutput(&stdout, &stderr))
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

func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	current := wd
	for {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current, nil
		}

		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("could not locate go.mod from %s", wd)
		}

		current = parent
	}
}

func freeListenAddr(ctx context.Context) (string, error) {
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer listener.Close()

	return listener.Addr().String(), nil
}

func combinedProcessOutput(stdout, stderr *bytes.Buffer) string {
	return fmt.Sprintf("proxy stdout:\n%s\nproxy stderr:\n%s", stdout.String(), stderr.String())
}
