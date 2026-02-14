package startcmder

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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

func appendToFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(data)
	return err
}
