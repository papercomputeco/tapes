package backfillcmder

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("NewBackfillCmd", func() {
	It("creates a command with correct use name", func() {
		cmd := NewBackfillCmd()
		Expect(cmd.Use).To(Equal("backfill"))
	})

	It("has the expected flags", func() {
		cmd := NewBackfillCmd()

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
		cmd := NewBackfillCmd()
		err := cmd.Args(cmd, []string{"extra"})
		Expect(err).To(HaveOccurred())
	})
})
