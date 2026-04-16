//go:build darwin

package menucmder

import (
	"bytes"
	"image/png"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/start"
)

var _ = Describe("NewMenuCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := NewMenuCmd()
		Expect(cmd.Use).To(Equal("menu"))
		Expect(cmd.Short).NotTo(BeEmpty())
	})
})

var _ = Describe("tapesLogoPNG", func() {
	It("decodes as a non-empty PNG", func() {
		Expect(tapesLogoPNG).NotTo(BeEmpty())

		img, err := png.Decode(bytes.NewReader(tapesLogoPNG))
		Expect(err).NotTo(HaveOccurred())
		Expect(img.Bounds().Dx()).To(BeNumerically(">", 0))
		Expect(img.Bounds().Dy()).To(BeNumerically(">", 0))
	})
})

var _ = Describe("readRunningAgents", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-menu-agents-*")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	It("returns 0 when no state file exists", func() {
		mgr, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(readRunningAgents(mgr)).To(Equal(0))
	})

	It("returns 0 when state has no agents", func() {
		mgr, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.SaveState(&start.State{
			DaemonPID: 123,
			ProxyURL:  "http://localhost:9000",
			APIURL:    "http://localhost:9001",
		})).To(Succeed())

		Expect(readRunningAgents(mgr)).To(Equal(0))
	})

	It("returns the correct agent count", func() {
		mgr, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(mgr.SaveState(&start.State{
			DaemonPID: 123,
			ProxyURL:  "http://localhost:9000",
			APIURL:    "http://localhost:9001",
			Agents: []start.AgentSession{
				{Name: "claude", PID: 100, StartedAt: time.Now()},
				{Name: "opencode", PID: 200, StartedAt: time.Now()},
			},
		})).To(Succeed())

		Expect(readRunningAgents(mgr)).To(Equal(2))
	})

	It("returns 0 when state file is corrupted", func() {
		mgr, err := start.NewManager(tmpDir)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(mgr.StatePath, []byte("not json"), 0o600)).To(Succeed())

		Expect(readRunningAgents(mgr)).To(Equal(0))
	})
})

var _ = Describe("onExit", func() {
	It("does not panic", func() {
		Expect(func() { onExit() }).NotTo(Panic())
	})
})
