package foocmder

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("foo command", func() {
	It("prints foo by default", func() {
		cmd := NewFooCmd()
		buf := &bytes.Buffer{}
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs([]string{})

		err := cmd.Execute()
		Expect(err).NotTo(HaveOccurred())
	})

	It("prints foobar when --bar flag is set", func() {
		cmd := NewFooCmd()
		buf := &bytes.Buffer{}
		cmd.SetOut(buf)
		cmd.SetErr(buf)
		cmd.SetArgs([]string{"--bar"})

		err := cmd.Execute()
		Expect(err).NotTo(HaveOccurred())
	})
})
