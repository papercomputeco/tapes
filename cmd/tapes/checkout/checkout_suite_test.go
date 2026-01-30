package checkoutcmder_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCheckout(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Checkout Command Suite")
}
