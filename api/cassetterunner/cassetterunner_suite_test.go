package cassetterunner_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

func TestCassetteRunner(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cassette Runner Suite")
}

// servedContracts is the contract set the core under test serves. Tests state
// it outright rather than importing api.DefaultContractVersions, because the
// runner's job is to admit against whatever set it is handed — coupling the
// tests to core's current answer would stop them from proving that.
func servedContracts() []cassette.ContractVersion {
	return []cassette.ContractVersion{"v1"}
}
