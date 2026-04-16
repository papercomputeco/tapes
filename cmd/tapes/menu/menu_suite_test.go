//go:build darwin

package menucmder

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMenu(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Menu Command Suite")
}
