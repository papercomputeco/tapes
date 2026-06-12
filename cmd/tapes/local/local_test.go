package localcmder

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("local", func() {
	var tmpDir string

	BeforeEach(func() {
		var err error
		tmpDir, err = os.MkdirTemp("", "local-test-*")
		Expect(err).NotTo(HaveOccurred())

		tmpDir, err = filepath.EvalSymlinks(tmpDir)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Describe("resolveLocalTapesDir", func() {
		It("uses the override config dir when provided", func() {
			overrideDir := filepath.Join(tmpDir, "override")

			result, err := resolveLocalTapesDir(overrideDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(overrideDir))
			Expect(result).To(BeADirectory())
		})

		It("falls back to ~/.tapes and creates it when no target exists", func() {
			homeDir := filepath.Join(tmpDir, "home")
			Expect(os.MkdirAll(homeDir, 0o755)).To(Succeed())

			origDir, err := os.Getwd()
			Expect(err).NotTo(HaveOccurred())
			Expect(os.Chdir(tmpDir)).To(Succeed())
			DeferCleanup(func() { Expect(os.Chdir(origDir)).To(Succeed()) })

			origHome := os.Getenv("HOME")
			Expect(os.Setenv("HOME", homeDir)).To(Succeed())
			DeferCleanup(func() { Expect(os.Setenv("HOME", origHome)).To(Succeed()) })

			result, err := resolveLocalTapesDir("")
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(filepath.Join(homeDir, ".tapes")))
			Expect(result).To(BeADirectory())
		})
	})

	Describe("ensureLocalPostgresDir", func() {
		It("creates a postgres subdirectory beneath the resolved tapes dir", func() {
			overrideDir := filepath.Join(tmpDir, ".tapes")

			result, err := ensureLocalPostgresDir(overrideDir)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(filepath.Join(overrideDir, postgresDirName)))
			Expect(result).To(BeADirectory())
		})
	})

	Describe("planOllama", func() {
		It("uses a native server that is already answering", func() {
			plan := planOllama(false, false, true, true)
			Expect(plan.useDocker).To(BeFalse())
			Expect(plan.needsStart).To(BeFalse())
		})

		It("asks the user to start a native install that is not serving", func() {
			plan := planOllama(false, false, false, true)
			Expect(plan.useDocker).To(BeFalse())
			Expect(plan.needsStart).To(BeTrue())
		})

		It("falls back to Docker when no native install exists", func() {
			plan := planOllama(false, false, false, false)
			Expect(plan.useDocker).To(BeTrue())
		})

		It("keeps an already-running container", func() {
			plan := planOllama(false, true, false, true)
			Expect(plan.useDocker).To(BeTrue())
		})

		It("honors --docker-ollama over a native install", func() {
			plan := planOllama(true, false, true, true)
			Expect(plan.useDocker).To(BeTrue())
		})
	})

	Describe("ollamaServing", func() {
		It("reports true for a server answering /api/version", func() {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/version" {
					w.WriteHeader(http.StatusOK)
					return
				}
				w.WriteHeader(http.StatusNotFound)
			}))
			DeferCleanup(srv.Close)

			Expect(ollamaServing(srv.URL)).To(BeTrue())
		})

		It("reports false when nothing is listening", func() {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			srv.Close()

			Expect(ollamaServing(srv.URL)).To(BeFalse())
		})
	})
})
