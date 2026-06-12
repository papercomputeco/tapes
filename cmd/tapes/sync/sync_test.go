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

	It("defaults auth-subject to the local username", func() {
		// Local backfills have no gateway-validated JWT to derive a
		// subject from; the closest honest identity is whoever is
		// running tapes. Explicit --auth-subject still overrides.
		cmd := NewSyncCmd()
		flag := cmd.Flags().Lookup("auth-subject")
		Expect(flag).NotTo(BeNil())
		Expect(flag.DefValue).To(Equal(localUserSubject()))
		Expect(localUserSubject()).NotTo(BeEmpty(),
			"test environments always have an OS user or $USER")
	})

	It("has the expected flags", func() {
		cmd := NewSyncCmd()

		postgresFlag := cmd.Flags().Lookup("postgres")
		Expect(postgresFlag).NotTo(BeNil())

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
