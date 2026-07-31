package main

import (
	"context"
	"strings"

	oas "github.com/papercomputeco/tapes/pkg/tapesoapi"
)

// helloResponse is the body GET /hello returns.
//
// It exists as a type rather than as the inline map the handler builds so the
// schema can be reflected from it. That is the point of describing a cassette
// with tapesoapi: the contract is derived from the Go types on the wire, so a
// field renamed in the handler cannot keep its old name in the document.
type helloResponse struct {
	// Message is the greeting, followed by "world".
	Message string `json:"message"`

	// Greeting is the configured greeting on its own.
	Greeting string `json:"greeting"`

	// Cassette is the name this cassette is installed under.
	Cassette string `json:"cassette"`

	// Store names the backing store that answered. "memory" is not durable.
	Store string `json:"store" oas:"enum=memory|postgres"`

	// Rows is the current contents of the hello table.
	Rows []Row `json:"rows"`
}

// helloRequest is the optional body POST /hello accepts.
type helloRequest struct {
	// Hello is the first column, defaulting to "hello".
	Hello string `json:"hello" oas:"default=hello"`

	// World is the second column, defaulting to "world".
	World string `json:"world" oas:"default=world"`
}

// openAPIDocument renders this cassette's OpenAPI document.
//
// Every path is written under /api/<name>, which is what core's prefix
// admission requires: a fetched spec that declares an operation outside its own
// prefix is refused whole, because the operator approved the cassette before
// ever seeing its spec. Building the document from the runtime name rather than
// hardcoding "hello-world" means the same image installed under a second name
// publishes a correct spec for that name too.
//
// The manifest core admits the cassette on rides inside the document as a root
// extension, so there is one artifact to fetch and one thing to configure — and
// so a spec and the metadata describing it can never be fetched at two
// different versions.
func openAPIDocument(name string) []byte {
	prefix := "/api/" + name

	parser := oas.NewParser(oas.WithInfo(oas.Info{
		Title:       "Hello World Cassette",
		Description: "The smallest API that is still a tapes cassette.",
		Version:     "0.0.1",
	}))

	provenance := oas.Provenance{Kind: oas.KindManual, Name: "hello-world cassette"}

	// The manifest is contributed as a root extension on its own fragment.
	// Compile renders root extensions verbatim, so the shape core parses is
	// exactly the shape written here.
	_ = parser.AddFragment(oas.Fragment{
		Provenance: provenance,
		Extensions: map[string]any{"x-tapes-cassette": manifest(name)},
	})

	_ = parser.AddOperation("GET", prefix+"/hello",
		oas.NewOperation("getHello").
			Summary("Greet, and read back every stored row").
			Tag(name).
			JSONResponse(200, "The greeting and the contents of the hello table",
				parser.Schema(helloResponse{})).
			Build(),
		provenance)

	_ = parser.AddOperation("POST", prefix+"/hello",
		oas.NewOperation("createHello").
			Summary("Write one row to the hello table").
			Tag(name).
			OptionalJSONBody("The row to write", parser.Schema(helloRequest{})).
			JSONResponse(201, "The row that was written", parser.Schema(Row{})).
			Build(),
		provenance)

	// Compile is a pure function of what was added above, and every Add here is
	// a literal that cannot fail — so an error would mean this function is
	// wrong, not that the request is. Serving an empty body in that case would
	// hide it; core reports a cassette whose document does not parse, which is
	// the louder and more useful failure.
	compiled, err := parser.Compile(context.Background(), oas.WithTarget(oas.V30))
	if err != nil {
		return []byte(`{"error":"could not compile this cassette's OpenAPI document: ` +
			strings.ReplaceAll(err.Error(), `"`, `'`) + `"}`)
	}

	return compiled.JSON()
}

// manifest is the metadata core admits this cassette on.
func manifest(name string) map[string]any {
	return map[string]any{
		"kind": "cassette/v1alpha1",
		"cassette": map[string]any{
			"name":         name,
			"version":      "0.0.1",
			"display_name": "Hello World",
			"description":  "The smallest API that is still a tapes cassette.",
			"license":      "Apache-2.0",
			"homepage":     "https://github.com/papercomputeco/tapes",
			"image":        "tapes/hello-world-cassette:0.0.1",
			"port":         9999,
		},
		"depends": map[string]any{
			"core":  "v1",
			"views": []string{},
		},
		"api": map[string]any{
			"health":      "/ping",
			"openapi":     "/openapi",
			"prefix_path": "api",
		},
		"tables": []map[string]any{{"name": "hello"}},
		"config": []map[string]any{{
			"key":         "greeting",
			"type":        "string",
			"default":     "Hello",
			"description": "Greeting returned by the cassette.",
		}},
	}
}

// RoutePrefix is the prefix this cassette serves under, exported for tests.
func RoutePrefix(name string) string { return "/api/" + strings.TrimPrefix(name, "/") }
