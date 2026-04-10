package sessions_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSessions(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sessions Suite")
}
