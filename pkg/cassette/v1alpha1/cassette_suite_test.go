package v1alpha1_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/cassette/v1alpha1"
)

func TestCassette(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cassette V1Alpha1 Suite")

	// Panics if v1alpha1 does not implement the cassette manifest interface
	var _ cassette.Manifest = (*v1alpha1.Manifest)(nil)
}
