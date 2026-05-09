package start_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/start"
)

var _ = Describe("WaitForDaemon", func() {
	var (
		tmpDir  string
		manager *start.Manager
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-wait-daemon-*")
		Expect(err).NotTo(HaveOccurred())
		manager, err = start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("returns state when daemon becomes healthy within timeout", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(server.Close)

		go func() {
			defer GinkgoRecover()
			time.Sleep(200 * time.Millisecond)
			lock, lockErr := manager.Lock()
			Expect(lockErr).NotTo(HaveOccurred())
			saveErr := manager.SaveState(&start.State{
				DaemonPID: os.Getpid(),
				APIURL:    server.URL,
				ProxyURL:  server.URL,
			})
			Expect(saveErr).NotTo(HaveOccurred())
			Expect(lock.Release()).To(Succeed())
		}()

		state, err := start.WaitForDaemon(context.Background(), manager, start.WaitOptions{Timeout: 5 * time.Second})
		Expect(err).NotTo(HaveOccurred())
		Expect(state).NotTo(BeNil())
		Expect(state.APIURL).To(Equal(server.URL))
	})

	It("times out when daemon never becomes healthy", func() {
		began := time.Now()
		_, err := start.WaitForDaemon(context.Background(), manager, start.WaitOptions{Timeout: time.Second})
		elapsed := time.Since(began)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("timed out"))
		Expect(err.Error()).To(ContainSubstring("tapes start --logs"))
		Expect(elapsed).To(BeNumerically("<", 3*time.Second))
	})

	It("returns error quickly when Done channel is closed", func() {
		done := make(chan struct{})
		close(done)

		began := time.Now()
		_, err := start.WaitForDaemon(context.Background(), manager, start.WaitOptions{Timeout: 10 * time.Second, Done: done})
		elapsed := time.Since(began)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("daemon process exited"))
		Expect(err.Error()).To(ContainSubstring("tapes start --logs"))
		Expect(elapsed).To(BeNumerically("<", 2*time.Second))
	})

	It("respects context cancellation", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := start.WaitForDaemon(ctx, manager, start.WaitOptions{Timeout: 10 * time.Second})
		Expect(err).To(MatchError(context.Canceled))
	})
})

var _ = Describe("StateHealthy", func() {
	It("returns false when state is nil", func() {
		Expect(start.StateHealthy(context.Background(), nil)).To(BeFalse())
	})

	It("returns false when DaemonPID is zero", func() {
		state := &start.State{APIURL: "http://localhost:1234"}
		Expect(start.StateHealthy(context.Background(), state)).To(BeFalse())
	})

	It("returns false when APIURL is empty", func() {
		state := &start.State{DaemonPID: os.Getpid()}
		Expect(start.StateHealthy(context.Background(), state)).To(BeFalse())
	})

	It("returns false when process is dead", func() {
		state := &start.State{DaemonPID: 999999999, APIURL: "http://localhost:1234"}
		Expect(start.StateHealthy(context.Background(), state)).To(BeFalse())
	})

	It("returns true when process alive and API reachable", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(server.Close)

		state := &start.State{DaemonPID: os.Getpid(), APIURL: server.URL}
		Expect(start.StateHealthy(context.Background(), state)).To(BeTrue())
	})

	It("returns false when process alive but API unreachable", func() {
		state := &start.State{DaemonPID: os.Getpid(), APIURL: "http://127.0.0.1:1"}
		Expect(start.StateHealthy(context.Background(), state)).To(BeFalse())
	})
})

var _ = Describe("LoadHealthyOrClear", func() {
	var (
		tmpDir  string
		manager *start.Manager
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-load-healthy-*")
		Expect(err).NotTo(HaveOccurred())
		manager, err = start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("returns nil and no error when no state exists", func() {
		state, err := start.LoadHealthyOrClear(context.Background(), manager, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(state).To(BeNil())
	})

	It("returns the state when daemon is alive and API reachable", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(server.Close)

		Expect(manager.SaveState(&start.State{
			DaemonPID: os.Getpid(),
			APIURL:    server.URL,
			ProxyURL:  server.URL,
		})).To(Succeed())

		state, err := start.LoadHealthyOrClear(context.Background(), manager, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(state).NotTo(BeNil())
		Expect(state.APIURL).To(Equal(server.URL))
	})

	It("clears stale state when the recorded daemon is dead", func() {
		Expect(manager.SaveState(&start.State{
			DaemonPID: 999999999,
			APIURL:    "http://127.0.0.1:1",
		})).To(Succeed())

		state, err := start.LoadHealthyOrClear(context.Background(), manager, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(state).To(BeNil())

		reloaded, err := manager.LoadState()
		Expect(err).NotTo(HaveOccurred())
		Expect(reloaded).To(BeNil())
	})

	It("returns DSNMismatchError when running daemon's DSN differs from wantDSN", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(server.Close)

		Expect(manager.SaveState(&start.State{
			DaemonPID:   os.Getpid(),
			APIURL:      server.URL,
			ProxyURL:    server.URL,
			PostgresDSN: "postgres://running/db_a",
		})).To(Succeed())

		state, err := start.LoadHealthyMatching(context.Background(), manager, "postgres://requested/db_b", nil)
		Expect(err).To(HaveOccurred())
		Expect(state).To(BeNil())

		var mismatch *start.DSNMismatchError
		Expect(errors.As(err, &mismatch)).To(BeTrue())
		Expect(mismatch.Running).To(Equal("postgres://running/db_a"))
		Expect(mismatch.Requested).To(Equal("postgres://requested/db_b"))
	})

	It("attaches when wantDSN matches the running DSN", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(server.Close)

		Expect(manager.SaveState(&start.State{
			DaemonPID:   os.Getpid(),
			APIURL:      server.URL,
			ProxyURL:    server.URL,
			PostgresDSN: "postgres://shared/db",
		})).To(Succeed())

		state, err := start.LoadHealthyMatching(context.Background(), manager, "postgres://shared/db", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(state).NotTo(BeNil())
	})

	It("attaches without checking when wantDSN is empty (caller has no preference)", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(server.Close)

		Expect(manager.SaveState(&start.State{
			DaemonPID:   os.Getpid(),
			APIURL:      server.URL,
			ProxyURL:    server.URL,
			PostgresDSN: "postgres://anything/db",
		})).To(Succeed())

		state, err := start.LoadHealthyMatching(context.Background(), manager, "", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(state).NotTo(BeNil())
	})

	It("attaches without checking when running daemon has no recorded DSN (legacy state)", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(server.Close)

		Expect(manager.SaveState(&start.State{
			DaemonPID: os.Getpid(),
			APIURL:    server.URL,
			ProxyURL:  server.URL,
		})).To(Succeed())

		state, err := start.LoadHealthyMatching(context.Background(), manager, "postgres://anything/db", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(state).NotTo(BeNil())
	})

	It("clears corrupt state and warns instead of returning an error", func() {
		Expect(os.WriteFile(tmpDir+"/start.json", []byte("{not json"), 0o600)).To(Succeed())

		warn := &bytes.Buffer{}
		state, err := start.LoadHealthyOrClear(context.Background(), manager, warn)
		Expect(err).NotTo(HaveOccurred())
		Expect(state).To(BeNil())
		Expect(warn.String()).To(ContainSubstring("clearing corrupted daemon state"))

		_, statErr := os.Stat(tmpDir + "/start.json")
		Expect(os.IsNotExist(statErr)).To(BeTrue())
	})
})
