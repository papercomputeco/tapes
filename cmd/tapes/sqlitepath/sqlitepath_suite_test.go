package sqlitepath

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSQLitePath(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SQLite Path Suite")
}
