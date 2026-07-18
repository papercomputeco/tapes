package statuscmder_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	statuscmder "github.com/papercomputeco/tapes/cmd/tapes/status"
)

var _ = Describe("NewStatusCmd", func() {
	It("creates a command with the correct use string", func() {
		cmd := statuscmder.NewStatusCmd()
		Expect(cmd.Use).To(Equal("status"))
	})

	It("accepts zero arguments", func() {
		cmd := statuscmder.NewStatusCmd()
		err := cmd.Args(cmd, []string{})
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects any arguments", func() {
		cmd := statuscmder.NewStatusCmd()
		err := cmd.Args(cmd, []string{"extra"})
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("Status command execution", func() {
	var (
		tmpDir  string
		origDir string
		out     bytes.Buffer
	)

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "tapes-status-test-*")
		Expect(err).NotTo(HaveOccurred())

		origDir, err = os.Getwd()
		Expect(err).NotTo(HaveOccurred())

		err = os.Chdir(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		out.Reset()
	})

	AfterEach(func() {
		err := os.Chdir(origDir)
		Expect(err).NotTo(HaveOccurred())
		os.RemoveAll(tmpDir)
	})

	It("reports the local .tapes dir and an unreachable API", func() {
		err := os.MkdirAll(filepath.Join(tmpDir, ".tapes"), 0o755)
		Expect(err).NotTo(HaveOccurred())

		cmd := statuscmder.NewStatusCmd()
		cmd.SetOut(&out)
		cmd.SetArgs([]string{"--api-target", "http://127.0.0.1:1"})
		err = cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		Expect(out.String()).To(ContainSubstring(".tapes"))
		Expect(out.String()).To(ContainSubstring("unreachable"))
	})

	It("reports capture stats when the API is reachable", func() {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			Expect(r.URL.Path).To(Equal("/v1/stats"))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"session_count":2,"turn_count":5,"total_cost":1.5}`))
		}))
		defer server.Close()

		cmd := statuscmder.NewStatusCmd()
		cmd.SetOut(&out)
		cmd.SetArgs([]string{"--api-target", server.URL})
		err := cmd.Execute()
		Expect(err).NotTo(HaveOccurred())

		Expect(out.String()).To(ContainSubstring("2 sessions"))
		Expect(out.String()).To(ContainSubstring("5 turns"))
	})
})
