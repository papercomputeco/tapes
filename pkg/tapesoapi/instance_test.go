package tapesoapi_test

// Instance validation is what `tapes dev check-openapi` runs captured wire
// through, so these specs are about the failure it exists to catch: a served
// field whose JSON type has drifted from the type the published contract
// promises, named precisely enough to find in a large payload.

import (
	"encoding/json"
	"errors"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

var _ = Describe("instance validation", func() {
	const schemas = `openapi: 3.0.3
info:
  title: Fixture
  version: 1.0.0
components:
  schemas:
    Widget:
      type: object
      required: [id, parts]
      additionalProperties: false
      properties:
        id:
          type: string
          minLength: 1
        count:
          type: integer
          minimum: 0
        ratio:
          type: number
        active:
          type: boolean
        kind:
          type: string
          enum: [round, square]
        note:
          type: string
          nullable: true
        parts:
          type: array
          items:
            $ref: '#/components/schemas/Part'
    Part:
      type: object
      required: [name]
      properties:
        name:
          type: string
        tags:
          type: array
          uniqueItems: true
          items:
            type: string
`

	var contract *tapesoapi.CompiledDoc

	BeforeEach(func() {
		parser := tapesoapi.NewParser(
			tapesoapi.WithInfo(tapesoapi.Info{Title: "Fixture", Version: "1.0.0"}))
		Expect(parser.AddDocument(ctx(), []byte(schemas), tapesoapi.WithoutInfo())).To(Succeed())

		var err error
		contract, err = parser.Compile(ctx(), tapesoapi.WithLint())
		Expect(err).NotTo(HaveOccurred())
	})

	// widget is a conforming value, which each spec below breaks in one place.
	widget := func() map[string]any {
		return map[string]any{
			"id":     "w-1",
			"count":  float64(3),
			"ratio":  1.5,
			"active": true,
			"kind":   "round",
			"note":   nil,
			"parts": []any{
				map[string]any{"name": "bolt", "tags": []any{"metal", "small"}},
			},
		}
	}

	// violations returns the findings as "pointer: message" lines.
	violations := func(value any) []string {
		GinkgoHelper()

		err := contract.ValidateInstance("Widget", value)
		if err == nil {
			return nil
		}

		var instance *tapesoapi.InstanceError
		Expect(errors.As(err, &instance)).To(BeTrue(), "expected an InstanceError, got %v", err)
		Expect(instance.Schema).To(Equal("Widget"))

		lines := make([]string, 0, len(instance.Violations))
		for _, violation := range instance.Violations {
			lines = append(lines, violation.String())
		}

		return lines
	}

	It("accepts a conforming value", func() {
		Expect(contract.ValidateInstance("Widget", widget())).To(Succeed())
	})

	It("exposes the component schemas it can check against", func() {
		Expect(contract.ComponentSchemas()).To(ConsistOf("Widget", "Part"))

		schema, ok := contract.ComponentSchema("Widget")
		Expect(ok).To(BeTrue())
		Expect(schema.Type).To(Equal(tapesoapi.TypeObject))

		// A copy, so a caller cannot reach into the compiled document.
		schema.Type = tapesoapi.TypeString
		again, _ := contract.ComponentSchema("Widget")
		Expect(again.Type).To(Equal(tapesoapi.TypeObject))
	})

	It("refuses a schema name the document does not define", func() {
		err := contract.ValidateInstance("Gadget", widget())
		Expect(err).To(MatchError(ContainSubstring(`no component schema "Gadget"`)))

		var instance *tapesoapi.InstanceError
		Expect(errors.As(err, &instance)).To(BeFalse(), "a missing schema is a caller error, not a violation")
	})

	Describe("type drift", func() {
		It("catches a string served as a number", func() {
			value := widget()
			value["id"] = float64(12)
			Expect(violations(value)).To(ConsistOf("/id: should be a string, got a number"))
		})

		It("catches an object served as an array", func() {
			// The json.RawMessage-as-byte-array drift this whole gate exists for.
			value := widget()
			value["parts"] = []any{[]any{1, 2, 3}}
			Expect(violations(value)).To(ConsistOf("/parts/0: should be an object, got an array"))
		})

		It("catches an array served as an object", func() {
			value := widget()
			value["parts"] = map[string]any{"not": "an array"}
			Expect(violations(value)).To(ConsistOf("/parts: should be an array, got an object"))
		})

		It("catches a non-integral value in an integer field", func() {
			value := widget()
			value["count"] = 3.5
			Expect(violations(value)).To(ConsistOf("/count: should be an integer, got 3.5"))
		})

		It("catches a null in a field that is not nullable", func() {
			value := widget()
			value["active"] = nil
			Expect(violations(value)).To(ConsistOf("/active: is null, but the schema requires boolean"))
		})

		It("allows a null where the schema says nullable", func() {
			value := widget()
			value["note"] = nil
			Expect(violations(value)).To(BeEmpty())
		})
	})

	Describe("named through the value it found", func() {
		It("points at the element inside a nested array", func() {
			value := widget()
			value["parts"] = []any{
				map[string]any{"name": "bolt"},
				map[string]any{"name": "nut", "tags": []any{"metal", float64(7)}},
			}
			Expect(violations(value)).To(ConsistOf("/parts/1/tags/1: should be a string, got a number"))
		})

		It("reports every disagreement rather than the first", func() {
			value := widget()
			value["id"] = float64(1)
			value["active"] = "yes"
			value["kind"] = "hexagonal"
			Expect(violations(value)).To(HaveLen(3))
		})
	})

	Describe("constraints", func() {
		It("catches a missing required property", func() {
			value := widget()
			delete(value, "parts")
			Expect(violations(value)).To(ConsistOf(`(root): is missing required property "parts"`))
		})

		It("catches a missing required property in a nested object", func() {
			value := widget()
			value["parts"] = []any{map[string]any{"tags": []any{"metal"}}}
			Expect(violations(value)).To(ConsistOf(`/parts/0: is missing required property "name"`))
		})

		It("catches a value outside its enum", func() {
			value := widget()
			value["kind"] = "hexagonal"
			Expect(violations(value)).To(ConsistOf(`/kind: is "hexagonal", which is not one of "round", "square"`))
		})

		It("catches a value below its minimum", func() {
			value := widget()
			value["count"] = float64(-1)
			Expect(violations(value)).To(ConsistOf("/count: is -1, below minimum 0"))
		})

		It("catches a string below its minLength", func() {
			value := widget()
			value["id"] = ""
			Expect(violations(value)).To(ConsistOf("/id: is 0 characters, below minLength 1"))
		})

		It("catches a repeated item where uniqueItems is set", func() {
			value := widget()
			value["parts"] = []any{
				map[string]any{"name": "bolt", "tags": []any{"metal", "metal"}},
			}
			Expect(violations(value)).To(ConsistOf("/parts/0/tags/1: repeats the value at index 0, but uniqueItems is set"))
		})

		It("catches a property the schema does not declare", func() {
			value := widget()
			value["surprise"] = true
			Expect(violations(value)).To(
				ConsistOf("/surprise: is not a declared property and additionalProperties is false"))
		})
	})

	It("keeps the digits a UseNumber decoder was given", func() {
		// A caller decodes with UseNumber precisely so a large identifier is not
		// rounded on the way through. Rejecting json.Number here would report a
		// mismatch this package invented.
		var value any
		decoder := json.NewDecoder(strings.NewReader(`{
		  "id": "w-1",
		  "count": 9007199254740993,
		  "parts": [{"name": "bolt"}]
		}`))
		decoder.UseNumber()
		Expect(decoder.Decode(&value)).To(Succeed())

		Expect(contract.ValidateInstance("Widget", value)).To(Succeed())
	})

	It("counts string length in characters rather than bytes", func() {
		// A multi-byte string counted in bytes would be rejected for being long
		// enough.
		value := widget()
		value["id"] = "é"
		Expect(violations(value)).To(BeEmpty())
	})
})
