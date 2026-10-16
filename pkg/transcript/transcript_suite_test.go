package transcript_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTranscript(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Transcript Suite")
}
