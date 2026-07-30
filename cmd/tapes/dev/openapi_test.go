package devcmder

// `tapes dev openapi` exists for exactly one reason — to add the per-field prose
// a deployed binary cannot read, because it lives in doc comments and a running
// server has no source tree. So the specs that matter are: the document is the
// same one the server publishes, and passing a docs root actually changes it.

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("dev openapi", func() {
	// run invokes the command as the CLI would and returns its stdout. The
	// command is rebuilt per call because its flags live on the commander.
	run := func(args ...string) []byte {
		var stdout, stderr bytes.Buffer
		cmd := newOpenAPICmd()
		cmd.SetOut(&stdout)
		cmd.SetErr(&stderr)
		cmd.SetArgs(args)

		Expect(cmd.Execute()).To(Succeed(), stderr.String())

		return stdout.Bytes()
	}

	// The suite runs in cmd/tapes/dev, so the module root is three levels up.
	// Naming it once here keeps the specs from each spelling out a path whose
	// depth is an artifact of where this file happens to live.
	moduleRoot := filepath.Join("..", "..", "..")

	It("compiles the read API contract by default", func() {
		var document map[string]any
		Expect(json.Unmarshal(run("--format", "json", "--docs-root", moduleRoot), &document)).To(Succeed())

		paths, ok := document["paths"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(paths).To(HaveKey("/v1/sessions"))
		Expect(paths).NotTo(HaveKey("/v1/ingest"),
			"the read API and the ingest write surface are separate documents")
	})

	It("compiles the ingest write surface on request", func() {
		var document map[string]any
		Expect(json.Unmarshal(run("ingest", "--format", "json", "--docs-root", moduleRoot), &document)).To(Succeed())

		paths, ok := document["paths"].(map[string]any)
		Expect(ok).To(BeTrue())
		Expect(paths).To(HaveKey("/v1/ingest"))
		Expect(paths).NotTo(HaveKey("/v1/sessions"),
			"an ingest deployment must not advertise a read surface it does not serve")
	})

	It("adds field prose that the served document cannot carry", func() {
		documented := run("--format", "json", "--docs-root", moduleRoot)
		bare := run("--format", "json", "--docs-root", "")

		// Asserted on a named field, not only on a count: a type-level
		// description survives into the served document too, so a count could
		// move for the wrong reason.
		Expect(fieldDescription(documented, "MCPRequest", "method")).NotTo(BeEmpty())
		Expect(fieldDescription(bare, "MCPRequest", "method")).To(BeEmpty())

		// And the count, because the claim is about the whole document: without a
		// source tree there is no field prose anywhere, which is exactly the gap
		// this command fills and the reason the served contract has it.
		Expect(fieldDescriptions(documented)).NotTo(BeEmpty())
		Expect(fieldDescriptions(bare)).To(BeEmpty())
	})

	It("writes to a file when asked, so a consumer can vendor the bytes", func() {
		out := filepath.Join(GinkgoT().TempDir(), "contract.yaml")
		Expect(run("--docs-root", moduleRoot, "--out", out)).To(BeEmpty(),
			"with --out the document goes to the file, not to stdout")

		written, err := os.ReadFile(out)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(written)).To(HavePrefix("components:"))
	})

	DescribeTable("rejects an input it cannot honour",
		func(args []string, because string) {
			cmd := newOpenAPICmd()
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			cmd.SetArgs(args)

			Expect(cmd.Execute()).To(MatchError(ContainSubstring(because)))
		},
		Entry("an unknown surface", []string{"proxy"}, `unknown surface "proxy"`),
		Entry("an unknown format", []string{"--format", "xml"}, `unknown format "xml"`),
		// Failing rather than silently returning the undocumented document:
		// someone who named a docs root asked for the prose, and its absence is
		// the one outcome they cannot see in the output.
		Entry("a docs root that is not a module", []string{"--docs-root", "/nonexistent"}, "read doc comments"),
	)

	It("is reachable as a subcommand of dev", func() {
		subcommands := NewDevCmd().Commands()
		names := make([]string, 0, len(subcommands))
		for _, sub := range subcommands {
			names = append(names, sub.Name())
		}
		Expect(names).To(ContainElement("openapi"))
	})
})

// describedSchemas is the shape both helpers below read: just deep enough to
// reach a property's description and no deeper.
type describedSchemas struct {
	Components struct {
		Schemas map[string]struct {
			Properties map[string]struct {
				Description string `json:"description"`
			} `json:"properties"`
		} `json:"schemas"`
	} `json:"components"`
}

func decodeSchemas(document []byte) describedSchemas {
	var decoded describedSchemas
	Expect(json.Unmarshal(document, &decoded)).To(Succeed())

	return decoded
}

// fieldDescription reads one property's description, returning "" when any step
// of the path is absent — which is the answer the specs are asking about.
func fieldDescription(document []byte, schema, field string) string {
	return decodeSchemas(document).Components.Schemas[schema].Properties[field].Description
}

// fieldDescriptions names every described property, as "Schema.field".
func fieldDescriptions(document []byte) []string {
	var out []string
	for name, schema := range decodeSchemas(document).Components.Schemas {
		for field, property := range schema.Properties {
			if property.Description != "" {
				out = append(out, name+"."+field)
			}
		}
	}

	return out
}
