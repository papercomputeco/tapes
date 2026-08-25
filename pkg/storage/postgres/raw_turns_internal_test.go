package postgres

import (
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("transcriptTimeRange", func() {
	fallback := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)

	It("retains valid occurred_at_ms bounds when neighboring timestamp types are malformed", func() {
		records := json.RawMessage(`[
			{"timestamp":"2025-01-02T03:04:05Z"},
			{"timestamp":42,"payload":{"occurred_at_ms":1735787046000}},
			{"timestamp":[],"payload":{"occurred_at_ms":"not-a-number"}},
			{"timestamp":{},"payload":{"occurred_at_ms":1735787049000}}
		]`)

		earliest, latest := transcriptTimeRange(records, fallback)
		Expect(earliest).To(BeTemporally("==", time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)))
		Expect(latest).To(BeTemporally("==", time.Date(2025, 1, 2, 3, 4, 9, 0, time.UTC)))
	})

	It("retains valid timestamp bounds when neighboring occurred_at_ms types are malformed", func() {
		records := json.RawMessage(`[
			{"timestamp":"2025-01-02T03:04:05.125Z","payload":{"occurred_at_ms":[]}},
			{"timestamp":"invalid","payload":{"occurred_at_ms":{}}},
			{"timestamp":"2025-01-02T03:05:00Z","payload":{"occurred_at_ms":false}}
		]`)

		earliest, latest := transcriptTimeRange(records, fallback)
		Expect(earliest).To(BeTemporally("==", time.Date(2025, 1, 2, 3, 4, 5, 125000000, time.UTC)))
		Expect(latest).To(BeTemporally("==", time.Date(2025, 1, 2, 3, 5, 0, 0, time.UTC)))
	})

	It("falls back only when every timestamp candidate is invalid", func() {
		records := json.RawMessage(`[
			{"timestamp":17,"payload":{"occurred_at_ms":"bad"}},
			{"timestamp":"not-rfc3339","payload":{"occurred_at_ms":null}}
		]`)

		earliest, latest := transcriptTimeRange(records, fallback)
		Expect(earliest).To(Equal(fallback))
		Expect(latest).To(Equal(fallback))
	})
})
