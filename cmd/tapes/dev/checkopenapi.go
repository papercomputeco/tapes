package devcmder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// tracesResponseSchema is the component the composite session-traces
// response is validated against. It is the 200 body of
// GET /v1/sessions/{id}/traces.
const tracesResponseSchema = "SessionTracesResponse"

const checkOpenAPILongDesc string = `Assert served wire conforms to the published OpenAPI schema.

Reads composite session-traces JSON (the GET /v1/sessions/{id}/traces
response, as written by ` + "`tapes dev trace-fixtures`" + ` or captured from a
live API) and validates each document against the ` + tracesResponseSchema + `
schema in the contract the API server compiles from its own routes — the same
contract paper vendors to generate its Rust client.

This closes the loop the projection model asks for — the published OpenAPI
contract must match what is served: check-invariants gates the structural
properties; check-openapi gates that the served field *types* match the
codegen-able spec, catching json.RawMessage-as-byte-array drift. It validates the types of the fields that are present; it
is a contract type check, not a structural completeness check (that is
check-invariants' job).

Runs over a fixture directory (every session-traces-<s>.json, skipping
the .slim previews) or an explicit file. Exits non-zero on any
non-conformance.

Example:
  tapes dev trace-fixtures --corpus "$c" --out /tmp/wire
  tapes dev check-openapi /tmp/wire`

type checkOpenAPICommander struct{}

func newCheckOpenAPICmd() *cobra.Command {
	cmder := &checkOpenAPICommander{}
	cmd := &cobra.Command{
		Use:   "check-openapi <wire-path>...",
		Short: "Validate trace wire against the published OpenAPI schema",
		Long:  checkOpenAPILongDesc,
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			return cmder.run(cmd, args)
		},
	}
	return cmd
}

func (c *checkOpenAPICommander) run(cmd *cobra.Command, paths []string) error {
	contract, err := publishedContract(cmd.Context())
	if err != nil {
		return err
	}

	files, err := collectComposites(paths)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no composite session-traces JSON found under %v", paths)
	}

	failed := 0
	for _, file := range files {
		violations, err := validateAgainstSchema(contract, file)
		if err != nil {
			cmd.Printf("✘ %s: %v\n", filepath.Base(file), err)
			failed++
			continue
		}
		if len(violations) > 0 {
			failed++
			cmd.Printf("✘ %s\n", filepath.Base(file))
			for _, v := range violations {
				cmd.Printf("    %s\n", v)
			}
			continue
		}
		cmd.Printf("✓ %s\n", filepath.Base(file))
	}

	cmd.Printf("checked %d composite file(s) against %s, %d failed\n", len(files), tracesResponseSchema, failed)
	if failed > 0 {
		return fmt.Errorf("%d file(s) did not conform to the OpenAPI schema", failed)
	}
	return nil
}

// publishedContract compiles the API server's contract from its own routes.
//
// Compiled rather than read from a file: the contract is what the running
// server describes, so checking wire against a checked-in copy would only prove
// the wire agrees with whatever that copy last said. Field prose is absent here
// — no docs root is passed — and prose is not what this command asserts.
func publishedContract(ctx context.Context) (*tapesoapi.CompiledDoc, error) {
	contract, err := api.CompileOpenAPI(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("compile the api contract: %w", err)
	}
	if _, ok := contract.ComponentSchema(tracesResponseSchema); !ok {
		return nil, fmt.Errorf("the api contract defines no %q schema", tracesResponseSchema)
	}
	return contract, nil
}

// validateAgainstSchema reads one composite file and validates it against
// the schema. Returns a human-readable violation list (empty when clean).
func validateAgainstSchema(contract *tapesoapi.CompiledDoc, path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return validateSchemaBytes(contract, raw)
}

// validateSchemaBytes decodes composite JSON and validates it against the
// schema (split out from the file reader so tests can exercise the
// contract check on in-memory fixtures without touching disk).
func validateSchemaBytes(contract *tapesoapi.CompiledDoc, raw []byte) ([]string, error) {
	// UseNumber keeps the digits the server sent: a large span ID that survives
	// the wire only to be rounded by float64 here would be reported as a
	// mismatch this command invented.
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var doc any
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}

	// A schema-validation failure is a reported non-conformance (a
	// violation string), not a program error — the caller distinguishes
	// "this file doesn't conform" from "the checker itself broke".
	verr := contract.ValidateInstance(tracesResponseSchema, doc)
	if verr == nil {
		return nil, nil
	}

	// One line per disagreement, so a file with four drifted fields reports
	// four places rather than one paragraph.
	var instanceErr *tapesoapi.InstanceError
	if errors.As(verr, &instanceErr) {
		violations := make([]string, 0, len(instanceErr.Violations))
		for _, violation := range instanceErr.Violations {
			violations = append(violations, violation.String())
		}
		return violations, nil
	}

	return []string{verr.Error()}, nil
}
