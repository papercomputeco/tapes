package dotdir_test

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/dotdir"
)

var _ = Describe("dotdir.Manager checkout", func() {
	var tmpDir string
	var m *dotdir.Manager

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "dotdir-test-*")
		Expect(err).NotTo(HaveOccurred())
		m = dotdir.NewManager()
	})

	AfterEach(func() {
		os.RemoveAll(tmpDir)
	})

	Describe("LoadCheckout", func() {
		It("returns nil when no checkout file exists", func() {
			state, err := m.LoadCheckoutState(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).To(BeNil())
		})

		It("loads a valid checkout state", func() {
			// Write a checkout file manually
			data := `{"hash":"abc123","messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi there"}]}`
			err := os.WriteFile(filepath.Join(tmpDir, "checkout.json"), []byte(data), 0o644)
			Expect(err).NotTo(HaveOccurred())

			state, err := m.LoadCheckoutState(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(state).NotTo(BeNil())
			Expect(state.Hash).To(Equal("abc123"))
			Expect(state.Messages).To(HaveLen(2))
			Expect(state.Messages[0].Role).To(Equal("user"))
			Expect(state.Messages[0].Content).To(Equal("hello"))
			Expect(state.Messages[1].Role).To(Equal("assistant"))
			Expect(state.Messages[1].Content).To(Equal("hi there"))
		})

		It("returns error for invalid JSON", func() {
			err := os.WriteFile(filepath.Join(tmpDir, "checkout.json"), []byte("not json"), 0o644)
			Expect(err).NotTo(HaveOccurred())

			state, err := m.LoadCheckoutState(tmpDir)
			Expect(err).To(HaveOccurred())
			Expect(state).To(BeNil())
		})
	})

	Describe("SaveCheckout", func() {
		It("persists checkout state to disk", func() {
			state := &dotdir.CheckoutState{
				Hash: "def456",
				Messages: []dotdir.CheckoutMessage{
					{Role: "user", Content: "what is Go?"},
					{Role: "assistant", Content: "Go is a programming language."},
				},
			}

			err := m.SaveCheckout(state, tmpDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify the file exists
			_, err = os.Stat(filepath.Join(tmpDir, "checkout.json"))
			Expect(err).NotTo(HaveOccurred())

			// Load it back and verify
			loaded, err := m.LoadCheckoutState(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded.Hash).To(Equal("def456"))
			Expect(loaded.Messages).To(HaveLen(2))
		})

		It("returns error for nil state", func() {
			err := m.SaveCheckout(nil, tmpDir)
			Expect(err).To(HaveOccurred())
		})

		It("overwrites existing checkout state", func() {
			first := &dotdir.CheckoutState{
				Hash:     "first",
				Messages: []dotdir.CheckoutMessage{{Role: "user", Content: "first message"}},
			}
			second := &dotdir.CheckoutState{
				Hash:     "second",
				Messages: []dotdir.CheckoutMessage{{Role: "user", Content: "second message"}},
			}

			err := m.SaveCheckout(first, tmpDir)
			Expect(err).NotTo(HaveOccurred())

			err = m.SaveCheckout(second, tmpDir)
			Expect(err).NotTo(HaveOccurred())

			loaded, err := m.LoadCheckoutState(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded.Hash).To(Equal("second"))
		})
	})

	Describe("ClearCheckout", func() {
		It("removes the checkout file", func() {
			// First save a checkout
			state := &dotdir.CheckoutState{Hash: "to-clear", Messages: []dotdir.CheckoutMessage{}}
			err := m.SaveCheckout(state, tmpDir)
			Expect(err).NotTo(HaveOccurred())

			// Clear it
			err = m.ClearCheckout(tmpDir)
			Expect(err).NotTo(HaveOccurred())

			// Verify it's gone
			loaded, err := m.LoadCheckoutState(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded).To(BeNil())
		})

		It("succeeds when no checkout file exists", func() {
			err := m.ClearCheckout(tmpDir)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("round-trip", func() {
		It("saves and loads checkout state correctly", func() {
			state := &dotdir.CheckoutState{
				Hash: "abc123def456",
				Messages: []dotdir.CheckoutMessage{
					{Role: "system", Content: "You are a helpful assistant."},
					{Role: "user", Content: "Hello!"},
					{Role: "assistant", Content: "Hi! How can I help?"},
					{Role: "user", Content: "Tell me about Go."},
					{Role: "assistant", Content: "Go is a statically typed, compiled language."},
				},
			}

			err := m.SaveCheckout(state, tmpDir)
			Expect(err).NotTo(HaveOccurred())

			loaded, err := m.LoadCheckoutState(tmpDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded).To(Equal(state))
		})
	})
})
