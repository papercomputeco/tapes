package skill_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSkill(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Skill Suite")
}
