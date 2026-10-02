package extproc

import (
	"bufio"
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
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

// Opt-in because this exercises the actual pinned Envoy image, not a fake
// ext_proc stream. Host networking requires Linux (or Docker Desktop's opt-in
// host networking). All ports are ephemeral and all resources are temporary.
var _ = Describe("Compose Envoy", Ordered, func() {
	var gateway string
	var upstreamRequests chan *http.Request
	var captures chan map[string]any
	var grpcServer *grpc.Server
	var releaseSSE chan struct{}

	BeforeAll(func() {
		if os.Getenv("TAPES_ENVOY_SMOKE") != "1" {
			Skip("run make smoke-envoy to exercise the pinned Envoy image")
		}
		upstreamRequests = make(chan *http.Request, 16)
		captures = make(chan map[string]any, 16)
		releaseSSE = make(chan struct{})
		ingestServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer GinkgoRecover()
			Expect(r.Method).To(Equal(http.MethodPost))
			Expect(r.URL.Path).To(Equal("/v1/ingest"))
			var capture map[string]any
			Expect(json.NewDecoder(r.Body).Decode(&capture)).To(Succeed())
			captures <- capture
			w.WriteHeader(http.StatusAccepted)
		}))
		DeferCleanup(ingestServer.Close)
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer GinkgoRecover()
			body, err := io.ReadAll(r.Body)
			Expect(err).NotTo(HaveOccurred())
			upstreamRequests <- r.Clone(r.Context())
			if bytes.Contains(body, []byte(`"stream":true`)) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hello\"},\"finish_reason\":null}]}\n\n")
				w.(http.Flusher).Flush()
				select {
				case <-releaseSSE:
				case <-r.Context().Done():
					return
				}
				_, _ = io.WriteString(w, "data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/v1/messages":
				_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
			case "/v1/responses":
				_, _ = io.WriteString(w, `{"id":"resp_1","object":"response","model":"gpt-4o","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hello","annotations":[]}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
			default:
				_, _ = io.WriteString(w, `{"id":"chatcmpl_1","object":"chat.completion","model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
			}
		}))
		DeferCleanup(upstream.Close)
		proc, err := NewProcessor(Config{IngestURL: ingestServer.URL, MaxInflight: 4, DispatchByteBudget: 8 << 20})
		Expect(err).NotTo(HaveOccurred())
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		grpcServer = grpc.NewServer()
		RegisterServer(grpcServer, proc)
		go func() { _ = grpcServer.Serve(listener) }()
		DeferCleanup(grpcServer.Stop)

		data, err := os.ReadFile("../compose/envoy/envoy.yaml")
		Expect(err).NotTo(HaveOccurred())
		productionConfig := bytes.Clone(data)
		var config map[string]any
		Expect(yaml.Unmarshal(data, &config)).To(Succeed())
		resources := config["static_resources"].(map[string]any)
		listeners := resources["listeners"].([]any)
		port := composeEnvoyFreePort()
		composeEnvoySocket(listeners[0])["port_value"] = port
		gateway = fmt.Sprintf("http://127.0.0.1:%d", port)
		composeEnvoySocket(config["admin"])["port_value"] = composeEnvoyFreePort()
		for _, item := range resources["clusters"].([]any) {
			cluster := item.(map[string]any)
			address := upstream.Listener.Addr().(*net.TCPAddr)
			if cluster["name"] == "tapes_extproc" {
				address = listener.Addr().(*net.TCPAddr)
			}
			// Only the test substitutes plaintext loopback upstreams. Production
			// TLS/SAN/SNI configuration is validated by make validate-envoy.
			delete(cluster, "transport_socket")
			endpoints := cluster["load_assignment"].(map[string]any)["endpoints"].([]any)
			lb := endpoints[0].(map[string]any)["lb_endpoints"].([]any)
			socket := composeEnvoySocket(lb[0].(map[string]any)["endpoint"])
			socket["address"] = "127.0.0.1"
			socket["port_value"] = address.Port
		}
		data, err = yaml.Marshal(config)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.MkdirAll("../build", 0o755)).To(Succeed())
		dir, err := os.MkdirTemp("../build", "tapes-envoy-smoke-")
		Expect(err).NotTo(HaveOccurred())
		dir, err = filepath.Abs(dir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = os.RemoveAll(dir) })
		Expect(os.Chmod(dir, 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "envoy.yaml"), data, 0o644)).To(Succeed())
		Expect(os.Chmod(filepath.Join(dir, "envoy.yaml"), 0o644)).To(Succeed())
		composeData, err := os.ReadFile("../docker-compose.yaml")
		Expect(err).NotTo(HaveOccurred())
		var compose struct {
			Services map[string]struct {
				Image string `yaml:"image"`
			} `yaml:"services"`
		}
		Expect(yaml.Unmarshal(composeData, &compose)).To(Succeed())
		// Bind an unchanged copy for validation: relabeling the real config
		// could interfere with an already-running Compose stack on SELinux.
		productionPath := filepath.Join(dir, "production.yaml")
		Expect(os.WriteFile(productionPath, productionConfig, 0o644)).To(Succeed())
		Expect(os.Chmod(productionPath, 0o644)).To(Succeed())
		validation := exec.Command("docker", "run", "--rm", "--user", "101:101",
			"-v", productionPath+":/etc/envoy/envoy.yaml:ro,Z", compose.Services["envoy"].Image,
			"--mode", "validate", "-c", "/etc/envoy/envoy.yaml")
		output, err := validation.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), string(output))
		name := fmt.Sprintf("tapes-envoy-smoke-%d", port)
		cmd := exec.Command("docker", "run", "--rm", "--user", "101:101", "--network", "host", "--name", name,
			"-v", dir+":/etc/envoy:ro,Z", compose.Services["envoy"].Image,
			"-c", "/etc/envoy/envoy.yaml", "--disable-hot-restart", "--concurrency", "1", "--log-level", "warn")
		cmd.Stdout, cmd.Stderr = GinkgoWriter, GinkgoWriter
		Expect(cmd.Start()).To(Succeed())
		exited := make(chan error, 1)
		go func() { exited <- cmd.Wait() }()
		DeferCleanup(func() {
			_ = exec.Command("docker", "stop", "-t", "1", name).Run()
		})
		Eventually(func() error {
			select {
			case err := <-exited:
				StopTrying(fmt.Sprintf("Envoy exited before readiness: %v", err)).Now()
			default:
			}
			client := &http.Client{Timeout: time.Second}
			resp, err := client.Get(gateway + "/not-a-route")
			if err == nil {
				_ = resp.Body.Close()
			}
			return err
		}, 90*time.Second, 200*time.Millisecond).Should(Succeed())
	})

	request := func(path, body string) *http.Response {
		GinkgoHelper()
		req, err := http.NewRequest(http.MethodPost, gateway+path, strings.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer mock-secret")
		req.Header.Set("X-Api-Key", "mock-key")
		req.Header.Set("Anthropic-Version", "2023-06-01")
		req.Header.Set("X-Tapes-Harness-Id", "smoke")
		req.Header.Set("X-Tapes-Harness-Session-Id", "envoy-smoke")
		req.Header.Set("X-Tapes-Future-Field", "private")
		req.Header.Set("X-Paper-Auth-Org-Id", "forged")
		req.Header.Set("X-Paper-Auth-Future", "forged")
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		return resp
	}
	assertUpstream := func(host string) {
		GinkgoHelper()
		var req *http.Request
		Eventually(upstreamRequests).Should(Receive(&req))
		Expect(req.Host).To(Equal(host))
		Expect(req.Header.Get("Authorization")).To(Equal("Bearer mock-secret"))
		Expect(req.Header.Get("X-Api-Key")).To(Equal("mock-key"))
		Expect(req.Header.Get("Anthropic-Version")).To(Equal("2023-06-01"))
		for key := range req.Header {
			Expect(strings.ToLower(key)).NotTo(HavePrefix("x-tapes-"))
			Expect(strings.ToLower(key)).NotTo(HavePrefix("x-paper-auth-"))
		}
	}
	assertCapture := func() {
		GinkgoHelper()
		var capture map[string]any
		Eventually(captures, 5*time.Second).Should(Receive(&capture))
		session := capture["session"].(map[string]any)
		Expect(session["harness_id"]).To(Equal("smoke"))
		Expect(session["harness_session_id"]).To(Equal("envoy-smoke"))
		Expect(session["org_id"]).To(BeElementOf(nil, ""))
	}

	It("routes and captures all three APIs while stripping private headers", func() {
		for _, path := range []string{"/v1/messages", "/v1/responses", "/v1/chat/completions"} {
			body := `{"model":"gpt-4o","max_tokens":8,"messages":[{"role":"user","content":"hello"}]}`
			if path == "/v1/responses" {
				body = `{"model":"gpt-4o","input":"hello"}`
			}
			resp := request(path, body)
			_, err := io.ReadAll(resp.Body)
			Expect(resp.Body.Close()).To(Succeed())
			Expect(err).NotTo(HaveOccurred())
			host := "api.openai.com"
			if path == "/v1/messages" {
				host = "api.anthropic.com"
			}
			assertUpstream(host)
			assertCapture()
		}
	})

	It("delivers SSE before upstream completion and captures the completed turn", func() {
		defer close(releaseSSE)
		resp := request("/v1/chat/completions", `{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hello"}]}`)
		defer resp.Body.Close()
		reader := bufio.NewReader(resp.Body)
		line, err := reader.ReadString('\n')
		Expect(err).NotTo(HaveOccurred())
		Expect(line).To(ContainSubstring("hello"))
		Expect(captures).NotTo(Receive())
		// Release the upstream only after observing its first event downstream.
		releaseSSE <- struct{}{}
		_, err = io.ReadAll(reader)
		Expect(err).NotTo(HaveOccurred())
		assertUpstream("api.openai.com")
		assertCapture()
	})

	It("fails open without leaking private headers when ext_proc is unavailable", func() {
		grpcServer.Stop()
		resp := request("/v1/chat/completions", `{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}]}`)
		_, err := io.ReadAll(resp.Body)
		Expect(resp.Body.Close()).To(Succeed())
		Expect(err).NotTo(HaveOccurred())
		assertUpstream("api.openai.com")
		Consistently(captures, 200*time.Millisecond).ShouldNot(Receive())
	})
})

func composeEnvoySocket(value any) map[string]any {
	return value.(map[string]any)["address"].(map[string]any)["socket_address"].(map[string]any)
}

func composeEnvoyFreePort() int {
	GinkgoHelper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	port := listener.Addr().(*net.TCPAddr).Port
	Expect(listener.Close()).To(Succeed())
	return port
}
