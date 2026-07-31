package tapesoapi_test

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

type reflectScalars struct {
	Str       string          `json:"str"`
	Int       int             `json:"int"`
	Int64     int64           `json:"int64"`
	Unsigned  uint32          `json:"unsigned"`
	Float     float64         `json:"float"`
	Flag      bool            `json:"flag"`
	When      time.Time       `json:"when"`
	Raw       json.RawMessage `json:"raw"`
	Anything  any             `json:"anything"`
	Bytes     []byte          `json:"bytes"`
	Skipped   string          `json:"-"`
	unexposed string          //nolint:unused // the point of the field is that reflection skips it
}

type reflectNested struct {
	Rows  []reflectScalars          `json:"rows"`
	Index map[string]reflectScalars `json:"index"`
	One   *reflectScalars           `json:"one,omitempty"`
}

// reflectEmbedded promotes its embedded fields the way encoding/json does.
type reflectEmbedded struct {
	reflectBase
	Own string `json:"own"`
}

type reflectBase struct {
	Inherited string `json:"inherited"`
}

// reflectTagged states in tags what its Go types cannot.
type reflectTagged struct {
	Amount   int             `json:"amount" oas:"min=0,max=100,format=int32"`
	Slug     string          `json:"slug" oas:"minLength=1,maxLength=64,pattern=^[a-z-]+$"`
	Kind     string          `json:"kind" oas:"enum=alpha|beta|gamma"`
	Opaque   json.RawMessage `json:"opaque" oas:"type=object"`
	Blocks   json.RawMessage `json:"blocks" oas:"type=array:object"`
	Required string          `json:"required" oas:"required"`
	Nullable string          `json:"nullable" oas:"nullable"`
}

// reflectRecursive references itself, which a naive reflector recurses on
// forever.
type reflectRecursive struct {
	Name     string             `json:"name"`
	Children []reflectRecursive `json:"children"`
}

var _ = Describe("the schema reflector", func() {
	var reflector tapesoapi.Reflector

	BeforeEach(func() { reflector = tapesoapi.NewReflector() })

	// component resolves a reflected reference back to the schema it names,
	// since a named struct reflects to a $ref rather than to its body.
	component := func(schema *tapesoapi.Schema) *tapesoapi.Schema {
		GinkgoHelper()
		Expect(schema.Ref).NotTo(BeEmpty(), "expected a reference to a component")

		return reflector.Components()[schema.Ref[len("#/components/schemas/"):]]
	}

	reflect := func(value any) *tapesoapi.Schema {
		GinkgoHelper()
		schema, err := reflector.Reflect(value)
		Expect(err).NotTo(HaveOccurred())

		return schema
	}

	It("maps Go scalars onto JSON types and formats", func() {
		properties := component(reflect(reflectScalars{})).Properties

		Expect(properties["str"].Type).To(Equal(tapesoapi.TypeString))
		Expect(properties["int"].Type).To(Equal(tapesoapi.TypeInteger))
		Expect(properties["int64"].Format).To(Equal("int64"))
		Expect(properties["float"].Type).To(Equal(tapesoapi.TypeNumber))
		Expect(properties["flag"].Type).To(Equal(tapesoapi.TypeBoolean))

		// An unsigned integer cannot be negative, and saying so is free.
		Expect(properties["unsigned"].Minimum).To(HaveValue(BeEquivalentTo(0)))

		// time.Time serializes as a string. Reflecting into its unexported
		// fields would describe an object nobody ever sees on the wire.
		Expect(properties["when"].Type).To(Equal(tapesoapi.TypeString))
		Expect(properties["when"].Format).To(Equal("date-time"))

		// A []byte is base64 in JSON, not an array of integers.
		Expect(properties["bytes"].Type).To(Equal(tapesoapi.TypeString))
		Expect(properties["bytes"].Format).To(Equal("byte"))

		// Neither a RawMessage nor an interface carries a static shape, so the
		// honest schema is the one that constrains nothing.
		Expect(properties["raw"].Type).To(BeEmpty())
		Expect(properties["anything"].Type).To(BeEmpty())
	})

	It("honours the json tag", func() {
		properties := component(reflect(reflectScalars{})).Properties

		Expect(properties).NotTo(HaveKey("Skipped"), `json:"-" means not on the wire`)
		Expect(properties).NotTo(HaveKey("unexposed"), "unexported fields are not on the wire")
		Expect(properties).To(HaveKey("str"), "the json name wins over the Go name")
	})

	It("describes slices, maps, and pointers", func() {
		properties := component(reflect(reflectNested{})).Properties

		Expect(properties["rows"].Type).To(Equal(tapesoapi.TypeArray))
		Expect(properties["rows"].Items.Ref).To(HaveSuffix("reflectScalars"))

		// A Go map with string keys is a JSON object with a value schema.
		Expect(properties["index"].Type).To(Equal(tapesoapi.TypeObject))
		Expect(properties["index"].AdditionalProperties.Ref).To(HaveSuffix("reflectScalars"))

		// A pointer is the same type; optionality is expressed by leaving the
		// field out of `required`, not by changing what it holds.
		Expect(properties["one"].Ref).To(HaveSuffix("reflectScalars"))
	})

	It("promotes the fields of an embedded struct", func() {
		properties := component(reflect(reflectEmbedded{})).Properties

		// encoding/json flattens these onto the wire, so a schema that nested
		// them would describe an object that never appears.
		Expect(properties).To(HaveKey("inherited"))
		Expect(properties).To(HaveKey("own"))
		Expect(properties).NotTo(HaveKey("reflectBase"))
	})

	It("registers a type once however many times it is reflected", func() {
		first := reflect(reflectScalars{})
		second := reflect(reflectScalars{})

		Expect(second.Ref).To(Equal(first.Ref))
		Expect(reflector.Components()).To(HaveLen(1))
	})

	It("terminates on a self-referential type", func() {
		// The reference is returned before the body is derived, which is what
		// keeps this from recursing forever.
		schema := component(reflect(reflectRecursive{}))
		Expect(schema.Properties["children"].Items.Ref).To(HaveSuffix("reflectRecursive"))
	})

	Describe("the oas tag", func() {
		var properties map[string]*tapesoapi.Schema

		BeforeEach(func() { properties = component(reflect(reflectTagged{})).Properties })

		It("applies numeric and string constraints", func() {
			Expect(properties["amount"].Minimum).To(HaveValue(BeEquivalentTo(0)))
			Expect(properties["amount"].Maximum).To(HaveValue(BeEquivalentTo(100)))
			Expect(properties["slug"].MinLength).To(HaveValue(BeEquivalentTo(1)))
			Expect(properties["slug"].Pattern).To(Equal("^[a-z-]+$"))
		})

		It("applies an enum, pipe-separated because the tag is comma-separated", func() {
			Expect(properties["kind"].Enum).To(Equal([]any{"alpha", "beta", "gamma"}))
		})

		It("states a type the Go type cannot", func() {
			// A json.RawMessage always carrying an object is a fact about the
			// handler, not about the Go type, so the tag is where it belongs.
			Expect(properties["opaque"].Type).To(Equal(tapesoapi.TypeObject))
			Expect(properties["blocks"].Type).To(Equal(tapesoapi.TypeArray))
			Expect(properties["blocks"].Items.Type).To(Equal(tapesoapi.TypeObject))
		})

		It("marks a field required or nullable", func() {
			Expect(component(reflect(reflectTagged{})).Required).To(ContainElement("required"))
			Expect(properties["nullable"].Nullable).To(BeTrue())
		})
	})

	It("gives two same-named types distinct component names", func() {
		first := reflect(numericStatus())
		second := reflect(textualStatus())

		// Two different types cannot share one component: OpenAPI's component
		// space is flat and a generated client emits one type per key, so
		// collapsing them would publish a type neither package wrote.
		Expect(first.Ref).To(HaveSuffix("/Status"))
		Expect(second.Ref).To(HaveSuffix("/Status2"))
		Expect(reflector.Components()).To(HaveKeys("Status", "Status2"))

		// The rename is deterministic, not first-come: registration order is
		// what merge already makes stable.
		Expect(reflector.Components()["Status"].Properties).To(HaveKey("code"))
		Expect(reflector.Components()["Status2"].Properties).To(HaveKey("label"))
	})

	It("reuses one name for one type however it was reached", func() {
		Expect(reflect(numericStatus()).Ref).To(Equal(reflect(numericStatus()).Ref))
	})

	It("carries doc comments through when they are supplied", func() {
		reflector = tapesoapi.NewReflector(tapesoapi.WithDocs(stubDocs{
			typeDoc:  "reflectScalars is the fixture for scalar mapping.",
			fieldDoc: "Str is a string.",
		}))

		schema := component(reflect(reflectScalars{}))
		// Go's runtime carries no comments, so without this seam every
		// reflected schema would be structurally complete and undocumented.
		Expect(schema.Description).To(Equal("reflectScalars is the fixture for scalar mapping."))
		Expect(schema.Properties["str"].Description).To(Equal("Str is a string."))
	})
})

// numericStatus and textualStatus return two distinct types that share the bare
// name `Status`, which is the collision the reflector has to name its way out
// of. Function-local types are how two same-named types coexist in one file.
func numericStatus() any {
	type Status struct {
		Code int `json:"code"`
	}

	return Status{}
}

func textualStatus() any {
	type Status struct {
		Label string `json:"label"`
	}

	return Status{}
}

// stubDocs answers every lookup the same way, which is enough to prove the
// prose reaches the schema.
type stubDocs struct {
	typeDoc  string
	fieldDoc string
}

func (s stubDocs) TypeDoc(string, string) string          { return s.typeDoc }
func (s stubDocs) FieldDoc(string, string, string) string { return s.fieldDoc }
