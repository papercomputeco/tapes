package ingest_test

// The body limit is a sum of named budgets, never a re-typed magic number, so
// it can never silently desync from the parts it is built from.

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/ingest"
)

var _ = Describe("Ingest body limits", func() {
	It("derives MaxIngestBodyBytes as the decoded-request term plus the base64 response budget plus the reserve", func() {
		// Arithmetic-from-parts: a full decoded request, raw_response
		// riding base64-encoded (4/3 growth), plus the 4 MiB reserve for
		// the reduced response, meta, and JSON scaffolding.
		Expect(ingest.MaxDecodedRequestBytes).To(Equal(32 << 20))
		Expect(ingest.MaxIngestBodyBytes).To(Equal(
			ingest.MaxDecodedRequestBytes + ingest.MaxRawResponseBytes*4/3 + 4<<20))

		// And the magnitude lands in the ~48 MiB band the raise targets.
		Expect(ingest.MaxIngestBodyBytes).To(BeNumerically(">", 45<<20))
		Expect(ingest.MaxIngestBodyBytes).To(BeNumerically("<=", 48<<20))
	})
})
