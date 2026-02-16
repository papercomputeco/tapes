package synccmder

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewSyncCmd", func() {
	It("creates a command with correct use name", func() {
		cmd := NewSyncCmd()
		Expect(cmd.Use).To(Equal("sync"))
	})

	It("is hidden", func() {
		cmd := NewSyncCmd()
		Expect(cmd.Hidden).To(BeTrue())
	})

	It("has the expected flags", func() {
		cmd := NewSyncCmd()

		sqliteFlag := cmd.Flags().Lookup("sqlite")
		Expect(sqliteFlag).NotTo(BeNil())
		Expect(sqliteFlag.Shorthand).To(Equal("s"))

		claudeDirFlag := cmd.Flags().Lookup("claude-dir")
		Expect(claudeDirFlag).NotTo(BeNil())

		dryRunFlag := cmd.Flags().Lookup("dry-run")
		Expect(dryRunFlag).NotTo(BeNil())

		verboseFlag := cmd.Flags().Lookup("verbose")
		Expect(verboseFlag).NotTo(BeNil())
		Expect(verboseFlag.Shorthand).To(Equal("v"))
	})

	It("accepts no positional arguments", func() {
		cmd := NewSyncCmd()
		err := cmd.Args(cmd, []string{"extra"})
		Expect(err).To(HaveOccurred())
	})
})
