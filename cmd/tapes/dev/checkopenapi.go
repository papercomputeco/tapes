package devcmder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/spf13/cobra"

	"github.com/papercomputeco/tapes/api"
)

// tracesResponseSchema is the component the composite session-traces
// response is validated against. It is the 200 body of
// GET /v1/sessions/{id}/traces in api/openapi.yaml.
const tracesResponseSchema = "SessionTracesResponse"

const checkOpenAPILongDesc string = `Assert served wire conforms to the published OpenAPI schema.

Reads composite session-traces JSON (the GET /v1/sessions/{id}/traces
response, as written by ` + "`tapes dev trace-fixtures`" + ` or captured from a
live API) and validates each document against the ` + tracesResponseSchema + `
schema embedded in api/openapi.yaml — the same contract paper vendors to
generate its Rust client.

This closes the loop RFD 00007 Goal 2 asks for ("the published OpenAPI
contract matches what is served"): check-invariants gates the structural
properties; check-openapi gates that the served field *types* match the
codegen-able spec, catching the json.RawMessage-as-byte-array drift the
RFD calls out. It validates the types of the fields that are present; it
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
	schema, err := loadTracesResponseSchema()
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
		violations, err := validateAgainstSchema(schema, file)
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

// loadTracesResponseSchema loads the embedded OpenAPI contract and returns
// the resolved SessionTracesResponse schema, with internal $refs (SessionItem,
// TraceDetail, SpanItem, …) dereferenced so validation recurses into them.
func loadTracesResponseSchema() (*openapi3.Schema, error) {
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(api.OpenAPISpec())
	if err != nil {
		return nil, fmt.Errorf("load embedded openapi spec: %w", err)
	}
	ref, ok := doc.Components.Schemas[tracesResponseSchema]
	if !ok || ref.Value == nil {
		return nil, fmt.Errorf("openapi spec has no %q schema", tracesResponseSchema)
	}
	return ref.Value, nil
}

// validateAgainstSchema reads one composite file and validates it against
// the schema. Returns a human-readable violation list (empty when clean).
func validateAgainstSchema(schema *openapi3.Schema, path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return validateSchemaBytes(schema, raw)
}

// validateSchemaBytes decodes composite JSON and validates it against the
// schema (split out from the file reader so tests can exercise the
// contract check on in-memory fixtures without touching disk).
func validateSchemaBytes(schema *openapi3.Schema, raw []byte) ([]string, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	// A schema-validation failure is a reported non-conformance (a
	// violation string), not a program error — the caller distinguishes
	// "this file doesn't conform" from "the checker itself broke".
	if verr := schema.VisitJSON(doc); verr != nil {
		return []string{verr.Error()}, nil //nolint:nilerr // validation error is downgraded to a reported violation by design
	}
	return nil, nil
}
