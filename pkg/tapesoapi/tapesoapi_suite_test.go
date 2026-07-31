package tapesoapi_test

import (
	"context"
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/tapesoapi"
)

func TestTapesOAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "tapesoapi Suite")
}

// ctx is the context every spec compiles under. Compile performs no I/O, so a
// background context is the honest one.
func ctx() context.Context { return context.Background() }

// compileTree compiles a parser and decodes the result, which is how the specs
// assert on output without a typed model of every corner of OpenAPI.
func compileTree(parser *tapesoapi.Parser, options ...tapesoapi.CompileOption) map[string]any {
	GinkgoHelper()

	compiled, err := parser.Compile(ctx(), options...)
	Expect(err).NotTo(HaveOccurred())

	var tree map[string]any
	Expect(json.Unmarshal(compiled.JSON(), &tree)).To(Succeed())

	return tree
}

// at walks a decoded document by key, failing with the path it got lost on
// rather than a bare nil-map panic twelve frames deep.
func at(tree map[string]any, keys ...string) any {
	GinkgoHelper()

	var current any = tree
	for index, key := range keys {
		object, ok := current.(map[string]any)
		Expect(ok).To(BeTrue(), "expected an object at %v", keys[:index])
		current, ok = object[key]
		Expect(ok).To(BeTrue(), "no key %q at %v", key, keys[:index])
	}

	return current
}

// object is `at` for the common case of landing on an object.
func object(tree map[string]any, keys ...string) map[string]any {
	GinkgoHelper()

	out, ok := at(tree, keys...).(map[string]any)
	Expect(ok).To(BeTrue(), "expected an object at %v", keys)

	return out
}
