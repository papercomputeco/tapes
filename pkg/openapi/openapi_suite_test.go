package openapi_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOpenAPI(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OpenAPI Suite")
}
