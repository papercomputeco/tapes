package startcmder

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/credentials"
	"github.com/papercomputeco/tapes/pkg/start"
)

var _ = Describe("runLogs", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-start-logs-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("returns an error when the daemon is not running", func() {
		cmder := &startCommander{configDir: tmpDir}
		err := cmder.runLogs(context.Background(), &bytes.Buffer{})
		Expect(err).To(MatchError("daemon is not running"))
	})

	It("streams new log entries for a healthy daemon", func() {
		cmder := &startCommander{configDir: tmpDir}
		manager, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/ping" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(server.Close)

		state := &start.State{
			DaemonPID: os.Getpid(),
			APIURL:    server.URL,
			LogPath:   manager.LogPath,
		}
		Expect(manager.SaveState(state)).To(Succeed())
		Expect(os.WriteFile(manager.LogPath, nil, 0o600)).To(Succeed())

		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)
		var out bytes.Buffer
		errChan := make(chan error, 1)
		go func() {
			errChan <- cmder.runLogs(ctx, &out)
		}()

		time.Sleep(50 * time.Millisecond)
		Expect(appendToFile(manager.LogPath, []byte("hello\n"))).To(Succeed())

		Eventually(out.String, 2*time.Second, 50*time.Millisecond).Should(ContainSubstring("hello"))
		cancel()
		Eventually(errChan, 2*time.Second, 50*time.Millisecond).Should(Receive(MatchError(context.Canceled)))
	})
})

var _ = Describe("followLog", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-follow-logs-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("tails new log content only", func() {
		logPath := filepath.Join(tmpDir, "start.log")
		Expect(os.WriteFile(logPath, []byte("old\n"), 0o600)).To(Succeed())

		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)
		var out bytes.Buffer
		errChan := make(chan error, 1)
		go func() {
			errChan <- followLog(ctx, logPath, &out)
		}()

		time.Sleep(50 * time.Millisecond)
		Expect(appendToFile(logPath, []byte("new\n"))).To(Succeed())

		Eventually(out.String, 2*time.Second, 50*time.Millisecond).Should(ContainSubstring("new"))
		Expect(out.String()).NotTo(ContainSubstring("old"))
		cancel()
		Eventually(errChan, 2*time.Second, 50*time.Millisecond).Should(Receive(MatchError(context.Canceled)))
	})
})

var _ = Describe("injectCredentials", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-inject-creds-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("injects stored credentials into env", func() {
		mgr, err := credentials.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.SetKey("openai", "sk-test-inject")).To(Succeed())

		cmder := &startCommander{configDir: tmpDir}
		env := cmder.injectCredentials([]string{"HOME=/tmp"})

		found := false
		for _, e := range env {
			if strings.HasPrefix(e, "OPENAI_API_KEY=") {
				Expect(e).To(Equal("OPENAI_API_KEY=sk-test-inject"))
				found = true
			}
		}
		Expect(found).To(BeTrue(), "OPENAI_API_KEY should be injected")
	})

	It("does not override existing env vars", func() {
		mgr, err := credentials.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.SetKey("openai", "sk-stored")).To(Succeed())

		cmder := &startCommander{configDir: tmpDir}
		env := cmder.injectCredentials([]string{"OPENAI_API_KEY=sk-existing"})

		count := 0
		for _, e := range env {
			if strings.HasPrefix(e, "OPENAI_API_KEY=") {
				Expect(e).To(Equal("OPENAI_API_KEY=sk-existing"))
				count++
			}
		}
		Expect(count).To(Equal(1), "existing env var should not be duplicated")
	})

	It("returns env unchanged when no credentials stored", func() {
		cmder := &startCommander{configDir: tmpDir}
		original := []string{"HOME=/tmp", "PATH=/usr/bin"}
		env := cmder.injectCredentials(original)
		Expect(env).To(Equal(original))
	})

	It("injects multiple providers", func() {
		mgr, err := credentials.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.SetKey("openai", "sk-openai-test")).To(Succeed())
		Expect(mgr.SetKey("anthropic", "sk-anthropic-test")).To(Succeed())

		cmder := &startCommander{configDir: tmpDir}
		env := cmder.injectCredentials([]string{})

		envMap := make(map[string]string)
		for _, e := range env {
			if k, v, ok := strings.Cut(e, "="); ok {
				envMap[k] = v
			}
		}
		Expect(envMap).To(HaveKeyWithValue("OPENAI_API_KEY", "sk-openai-test"))
		Expect(envMap).To(HaveKeyWithValue("ANTHROPIC_API_KEY", "sk-anthropic-test"))
	})
})

var _ = Describe("loadConfig project resolution", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-start-project-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("uses the --project flag when set", func() {
		cmder := &startCommander{configDir: tmpDir, project: "my-flag-project"}
		cfg, err := cmder.loadConfig()
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Project).To(Equal("my-flag-project"))
	})

	It("falls back to config file proxy.project", func() {
		configPath := filepath.Join(tmpDir, "config.toml")
		Expect(os.WriteFile(configPath, []byte("[proxy]\nproject = \"my-config-project\"\n"), 0o600)).To(Succeed())

		cmder := &startCommander{configDir: tmpDir}
		cfg, err := cmder.loadConfig()
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Project).To(Equal("my-config-project"))
	})

	It("prefers --project flag over config file", func() {
		configPath := filepath.Join(tmpDir, "config.toml")
		Expect(os.WriteFile(configPath, []byte("[proxy]\nproject = \"from-config\"\n"), 0o600)).To(Succeed())

		cmder := &startCommander{configDir: tmpDir, project: "from-flag"}
		cfg, err := cmder.loadConfig()
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Project).To(Equal("from-flag"))
	})

	It("returns empty project when no flag or config (git fallback is in runServices)", func() {
		cmder := &startCommander{configDir: tmpDir}
		cfg, err := cmder.loadConfig()
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.Project).To(BeEmpty())
	})
})

func appendToFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	return err
}
