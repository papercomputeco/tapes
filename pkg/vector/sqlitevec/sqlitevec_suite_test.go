package sqlitevec_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSQLiteVec(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SQLiteVec Vector Suite")
}
