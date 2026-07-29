package devcmder

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/ingest"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
	"github.com/papercomputeco/tapes/pkg/tapesoapi/gosource"
)

// This command exists for the one thing a running server cannot do.
//
// Both servers publish their contract at their own /openapi, compiled from the
// routes they registered, and that is the canonical copy — there is no checked-in
// file and nothing to regenerate. But a deployed binary has no source tree, so
// the document it serves carries route and operation prose (which lives in Go
// values) and not per-field prose (which lives in doc comments). This reads the
// comments out of a checkout and folds them in.
//
// So: `curl /openapi` for what the server serves; this for the fully-documented
// document, and for the vendoring case where a consumer wants bytes on disk
// without tapes keeping a copy it could let go stale.

const openAPILongDesc string = `Compile a tapes OpenAPI contract from the route registrations.

The running servers publish these themselves at their own /openapi, compiled
from the same registrations. This command exists to add what a deployed binary
cannot: per-field prose, read from the doc comments in a checkout.

  tapes dev openapi                          # read API, YAML, prose from .
  tapes dev openapi ingest                   # the ingest write surface
  tapes dev openapi --format json            # JSON instead of YAML
  tapes dev openapi --docs-root ''           # no source tree; shapes only
  tapes dev openapi --out api-contract.yaml  # for a consumer that vendors bytes

Nothing in this repository reads the output. A contract file is a copy of what
the server already states exactly, and a copy can be stale.`

// surfaces are the contracts tapes publishes, keyed by the name this command
// takes. They are separate documents because they are separate servers with
// different trust models — see the header of ingest/openapi.go.
var surfaces = map[string]func(context.Context, tapesoapi.TypeDocs) (*tapesoapi.CompiledDoc, error){
	"api":    api.CompileOpenAPI,
	"ingest": ingest.CompileOpenAPI,
}

type openAPICommander struct {
	docsRoot string
	format   string
	out      string
}

func newOpenAPICmd() *cobra.Command {
	cmder := &openAPICommander{}
	cmd := &cobra.Command{
		Use:       "openapi [api|ingest]",
		Short:     "Compile a published OpenAPI contract, with field prose from the source",
		Long:      openAPILongDesc,
		Args:      cobra.MaximumNArgs(1),
		ValidArgs: []string{"api", "ingest"},
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true

			return cmder.run(cmd, args)
		},
	}
	cmd.Flags().StringVar(&cmder.docsRoot, "docs-root", ".",
		"module root to read doc comments from; empty skips them")
	cmd.Flags().StringVar(&cmder.format, "format", "yaml", "output format (yaml|json)")
	cmd.Flags().StringVar(&cmder.out, "out", "", "write to a file instead of stdout")

	return cmd
}

func (c *openAPICommander) run(cmd *cobra.Command, args []string) error {
	surface := "api"
	if len(args) == 1 {
		surface = args[0]
	}
	compile, known := surfaces[surface]
	if !known {
		return fmt.Errorf("unknown surface %q; expected one of %s", surface, strings.Join(surfaceNames(), ", "))
	}
	// Both names are resolved before anything is read or compiled. A mistyped
	// flag should fail on the flag, not after a source-tree walk that reports
	// something else entirely as the problem.
	render, known := formats[c.format]
	if !known {
		return fmt.Errorf("unknown format %q; expected yaml or json", c.format)
	}

	var docs tapesoapi.TypeDocs
	if c.docsRoot != "" {
		// Failing rather than proceeding without prose: someone who passed a
		// docs root asked for the documented document, and silently handing
		// back the undocumented one is the outcome they cannot see.
		loaded, err := gosource.Load(c.docsRoot,
			gosource.SkipDirs("oapi-reference", "build", "migrations"))
		if err != nil {
			return fmt.Errorf("read doc comments from %s: %w", c.docsRoot, err)
		}
		docs = loaded
	}

	compiled, err := compile(cmd.Context(), docs)
	if err != nil {
		return fmt.Errorf("compile the %s contract: %w", surface, err)
	}
	for _, warning := range compiled.Warnings() {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s\n", warning)
	}

	rendered, err := render(compiled)
	if err != nil {
		return err
	}

	if c.out == "" {
		_, err = cmd.OutOrStdout().Write(rendered)

		return err
	}
	if err := os.WriteFile(c.out, rendered, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", c.out, err)
	}
	// The fingerprint goes out whole: it is here so two people can compare the
	// documents they compiled, and a truncation is a comparison that can agree
	// about bytes that differ.
	fmt.Fprintf(cmd.ErrOrStderr(), "wrote %s (openapi %s, %d paths, %s)\n",
		c.out, compiled.Version(), len(compiled.Paths()), compiled.Fingerprint())

	return nil
}

// formats maps a --format value to its renderer. One map rather than a list of
// legal values plus a switch, so validating the flag and rendering the document
// cannot come to disagree about which formats exist.
var formats = map[string]func(*tapesoapi.CompiledDoc) ([]byte, error){
	"yaml": func(compiled *tapesoapi.CompiledDoc) ([]byte, error) {
		rendered, err := compiled.YAML()
		if err != nil {
			return nil, fmt.Errorf("render yaml: %w", err)
		}

		return rendered, nil
	},
	// A trailing newline on the JSON: the document goes to a terminal as often
	// as to a file, and JSON() ends mid-line by design so a caller embedding it
	// does not have to strip one.
	//nolint:unparam // the signature is shared with the YAML renderer, which can fail
	"json": func(compiled *tapesoapi.CompiledDoc) ([]byte, error) {
		return append(compiled.JSON(), '\n'), nil
	},
}

func surfaceNames() []string {
	names := make([]string, 0, len(surfaces))
	for name := range surfaces {
		names = append(names, name)
	}
	// Sorted so the error message does not depend on map iteration order.
	sort.Strings(names)

	return names
}
