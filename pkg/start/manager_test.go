package start_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/start"
)

var _ = Describe("Manager", func() {
	var tempDir string

	BeforeEach(func() {
		var err error
		tempDir, err = os.MkdirTemp("", "tapes-start-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if tempDir != "" {
			Expect(os.RemoveAll(tempDir)).To(Succeed())
		}
	})

	It("saves and loads state", func() {
		manager, err := start.NewManager(tempDir)
		Expect(err).NotTo(HaveOccurred())

		state := &start.State{
			DaemonPID:        123,
			ProxyURL:         "http://localhost:9000",
			APIURL:           "http://localhost:9001",
			ShutdownWhenIdle: true,
			Agents: []start.AgentSession{{
				Name: "claude",
				PID:  456,
			}},
		}

		Expect(manager.SaveState(state)).To(Succeed())
		loaded, err := manager.LoadState()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded).NotTo(BeNil())
		Expect(loaded.DaemonPID).To(Equal(123))
		Expect(loaded.ProxyURL).To(Equal("http://localhost:9000"))
		Expect(loaded.APIURL).To(Equal("http://localhost:9001"))
		Expect(loaded.ShutdownWhenIdle).To(BeTrue())
		Expect(loaded.Agents).To(HaveLen(1))
		Expect(loaded.Agents[0].Name).To(Equal("claude"))
		Expect(loaded.Agents[0].PID).To(Equal(456))
		Expect(loaded.LogPath).To(Equal(filepath.Join(tempDir, "start.log")))
	})

	It("clears state", func() {
		manager, err := start.NewManager(tempDir)
		Expect(err).NotTo(HaveOccurred())

		Expect(manager.SaveState(&start.State{DaemonPID: 1})).To(Succeed())
		Expect(manager.ClearState()).To(Succeed())

		loaded, err := manager.LoadState()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded).To(BeNil())
	})

	It("round-trips MenuPID through save and load", func() {
		manager, err := start.NewManager(tempDir)
		Expect(err).NotTo(HaveOccurred())

		state := &start.State{
			DaemonPID: 123,
			MenuPID:   789,
			ProxyURL:  "http://localhost:9000",
			APIURL:    "http://localhost:9001",
		}
		Expect(manager.SaveState(state)).To(Succeed())

		loaded, err := manager.LoadState()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.MenuPID).To(Equal(789))
	})

	It("omits MenuPID from JSON when zero", func() {
		manager, err := start.NewManager(tempDir)
		Expect(err).NotTo(HaveOccurred())

		state := &start.State{
			DaemonPID: 123,
			ProxyURL:  "http://localhost:9000",
			APIURL:    "http://localhost:9001",
		}
		Expect(manager.SaveState(state)).To(Succeed())

		data, err := os.ReadFile(manager.StatePath)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).NotTo(ContainSubstring("menu_pid"))
	})

	It("locks and releases", func() {
		manager, err := start.NewManager(tempDir)
		Expect(err).NotTo(HaveOccurred())

		lock, err := manager.Lock()
		Expect(err).NotTo(HaveOccurred())
		Expect(lock).NotTo(BeNil())
		Expect(lock.Release()).To(Succeed())
	})
})
