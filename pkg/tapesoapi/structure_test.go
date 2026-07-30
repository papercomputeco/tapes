package tapesoapi_test

// Structural validation is what replaced the third-party validator that used to
// sit in front of the renderer, so these specs are the evidence for that trade:
// each one is a document the specification forbids, driven in through the public
// ingest path, failing the compile with a message that names where it came from.

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

var _ = Describe("structural validation", func() {
	// document wraps a body in the minimum valid preamble, so each spec below
	// shows only the part that is wrong.
	document := func(body string) []byte {
		return []byte("openapi: 3.0.3\ninfo:\n  title: Fixture\n  version: 1.0.0\n" + body)
	}

	// compile ingests a body and compiles it with lint switched off, so a
	// finding can only have come from structure.
	compile := func(body string) error {
		GinkgoHelper()

		parser := tapesoapi.NewParser(
			tapesoapi.WithInfo(tapesoapi.Info{Title: "Fixture", Version: "1.0.0"}))
		Expect(parser.AddDocument(ctx(), document(body), tapesoapi.WithoutInfo())).To(Succeed())
		_, err := parser.Compile(ctx(), tapesoapi.WithLint())

		return err
	}

	// rejects asserts the compile failed structurally and reported the given
	// text. The type assertion matters as much as the message: a caller that
	// wants to distinguish "invalid document" from "unresolved reference" does
	// it by type.
	rejects := func(body string, wants ...string) {
		GinkgoHelper()

		err := compile(body)
		Expect(err).To(HaveOccurred())

		var structural *tapesoapi.StructureError
		Expect(errors.As(err, &structural)).To(BeTrue(), "expected a StructureError, got %v", err)
		Expect(structural.Version).To(Equal(tapesoapi.V30))
		for _, want := range wants {
			Expect(err.Error()).To(ContainSubstring(want))
		}
	}

	const widget = `paths:
  /widgets/{id}:
    get:
      operationId: getWidget
      parameters:
        - name: id
          in: path
          required: true
          schema:
            type: string
      responses:
        "200":
          description: ok
`

	It("accepts a document that says nothing wrong", func() {
		Expect(compile(widget)).To(Succeed())
	})

	Describe("paths and parameters", func() {
		It("rejects a template parameter no operation describes", func() {
			rejects(`paths:
  /widgets/{id}:
    get:
      operationId: getWidget
      responses:
        "200":
          description: ok
`, "does not describe path parameter {id}")
		})

		It("rejects a described path parameter the template does not contain", func() {
			rejects(`paths:
  /widgets:
    get:
      operationId: listWidgets
      parameters:
        - name: id
          in: path
          required: true
          schema:
            type: string
      responses:
        "200":
          description: ok
`, `describes path parameter "id"`)
		})

		It("rejects an optional path parameter", func() {
			rejects(`paths:
  /widgets/{id}:
    get:
      operationId: getWidget
      parameters:
        - name: id
          in: path
          required: false
          schema:
            type: string
      responses:
        "200":
          description: ok
`, `path parameter "id" must be required`)
		})

		It("rejects the same parameter described twice", func() {
			rejects(`paths:
  /widgets:
    get:
      operationId: listWidgets
      parameters:
        - name: limit
          in: query
          schema:
            type: integer
        - name: limit
          in: query
          schema:
            type: string
      responses:
        "200":
          description: ok
`, `declares parameter "limit" in query more than once`)
		})

		It("rejects a parameter located somewhere that does not exist", func() {
			rejects(`paths:
  /widgets:
    post:
      operationId: createWidget
      parameters:
        - name: payload
          in: body
          schema:
            type: string
      responses:
        "200":
          description: ok
`, "which is not one of path, query, header, cookie")
		})

		It("describes a path parameter through a component reference", func() {
			// The common shape in a hand-written contract, and the one a
			// validator that does not follow references reports as missing.
			Expect(compile(`paths:
  /widgets/{id}:
    parameters:
      - $ref: '#/components/parameters/WidgetID'
    get:
      operationId: getWidget
      responses:
        "200":
          description: ok
components:
  parameters:
    WidgetID:
      name: id
      in: path
      required: true
      schema:
        type: string
`)).To(Succeed())
		})
	})

	Describe("content", func() {
		It("rejects a content key that is not a media type", func() {
			rejects(`paths:
  /widgets:
    post:
      operationId: createWidget
      requestBody:
        content:
          json:
            schema:
              type: object
      responses:
        "200":
          description: ok
`, `declares content type "json", which is not a media type`)
		})

		It("rejects a request body with no content", func() {
			rejects(`paths:
  /widgets:
    post:
      operationId: createWidget
      requestBody:
        description: nothing at all
      responses:
        "200":
          description: ok
`, "request body with no content")
		})
	})

	Describe("schemas", func() {
		It("rejects bounds no value can satisfy", func() {
			// Not a spec violation but a schema nothing validates against, and
			// nothing downstream will ever say so: the generated client compiles
			// and rejects every payload at runtime.
			rejects(`components:
  schemas:
    Widget:
      type: string
      minLength: 10
      maxLength: 2
`, "has minLength 10 above maxLength 2")
		})

		It("rejects a multipleOf of zero", func() {
			rejects(`components:
  schemas:
    Widget:
      type: number
      multipleOf: 0
`, "which must be greater than zero")
		})

		It("rejects a pattern that does not compile", func() {
			rejects(`components:
  schemas:
    Widget:
      type: string
      pattern: '[unterminated'
`, "which does not compile")
		})

		It("rejects a discriminator with nothing to select from", func() {
			rejects(`components:
  schemas:
    Widget:
      type: object
      discriminator:
        propertyName: kind
`, "no oneOf, anyOf, or allOf to select from")
		})

		It("reports a nested schema by the path that reaches it", func() {
			rejects(`components:
  schemas:
    Widget:
      type: object
      properties:
        parts:
          type: array
          items:
            type: string
            minLength: 5
            maxLength: 1
`, "schemas/Widget.properties.parts.items has minLength 5")
		})
	})

	Describe("security", func() {
		It("rejects a requirement naming a scheme nothing defines", func() {
			// The worst kind of silent failure: an undefined scheme disables auth
			// on the operation it was written to guard.
			rejects(`paths:
  /widgets:
    get:
      operationId: listWidgets
      security:
        - bearerAuth: []
      responses:
        "200":
          description: ok
`, `requires security scheme "bearerAuth"`)
		})

		It("rejects an apiKey scheme with no name or location", func() {
			rejects(`components:
  securitySchemes:
    apiKey:
      type: apiKey
`, `is type "apiKey" and requires name`, `is type "apiKey" and requires in`)
		})

		It("rejects an apiKey delivered somewhere it cannot be", func() {
			rejects(`components:
  securitySchemes:
    apiKey:
      type: apiKey
      name: X-Key
      in: body
`, `has in="body", which apiKey does not allow`)
		})
	})

	Describe("servers", func() {
		It("rejects a url template variable nothing declares", func() {
			rejects(`servers:
  - url: 'https://{region}.example.com'
`, "which no server variable declares")
		})

		It("rejects a server variable with no default", func() {
			rejects(`servers:
  - url: 'https://{region}.example.com'
    variables:
      region:
        enum: [us, eu]
`, `variable "region" has no default`)
		})
	})

	It("reports every violation in one error rather than the first", func() {
		err := compile(`paths:
  /widgets/{id}:
    get:
      operationId: getWidget
      responses:
        "200":
          description: ok
  /parts/{partId}:
    get:
      operationId: getPart
      responses:
        "200":
          description: ok
`)

		var structural *tapesoapi.StructureError
		Expect(errors.As(err, &structural)).To(BeTrue())
		Expect(structural.Violations).To(HaveLen(2))
	})

	It("skips structural validation when asked to serve a known-imperfect document", func() {
		// WithoutValidation exists so a bad upstream document can still be
		// served rather than taking the aggregate down with it.
		parser := tapesoapi.NewParser(
			tapesoapi.WithInfo(tapesoapi.Info{Title: "Fixture", Version: "1.0.0"}))
		Expect(parser.AddDocument(ctx(), document(`paths:
  /widgets/{id}:
    get:
      operationId: getWidget
      responses:
        "200":
          description: ok
`), tapesoapi.WithoutInfo())).To(Succeed())

		compiled, err := parser.Compile(ctx(), tapesoapi.WithoutValidation())
		Expect(err).NotTo(HaveOccurred())
		Expect(compiled.Paths()).To(ConsistOf("/widgets/{id}"))
	})

	It("reports structure ahead of lint", func() {
		// A lint finding about a document that is not yet valid is noise in
		// front of the reason it will not build, so the invalid document below
		// must fail structurally even though it also has no operationId.
		parser := tapesoapi.NewParser(
			tapesoapi.WithInfo(tapesoapi.Info{Title: "Fixture", Version: "1.0.0"}))
		Expect(parser.AddDocument(ctx(), document(`paths:
  /widgets/{id}:
    get:
      responses:
        "200":
          description: ok
`), tapesoapi.WithoutInfo())).To(Succeed())

		_, err := parser.Compile(ctx())
		var structural *tapesoapi.StructureError
		Expect(errors.As(err, &structural)).To(BeTrue(), "expected a StructureError, got %v", err)
	})
})
