package deck

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("HTTPQuery", func() {
	Describe("NewHTTPQuery", func() {
		It("assumes http when the api target omits a scheme", func() {
			q := NewHTTPQuery("localhost:8081", nil)
			Expect(q.apiTarget).To(Equal("http://localhost:8081"))
		})

		It("preserves explicit schemes", func() {
			q := NewHTTPQuery("https://example.com/api/", nil)
			Expect(q.apiTarget).To(Equal("https://example.com/api"))
		})
	})

	Describe("Overview", func() {
		It("fetches a single bounded summary page and pushes time filters down", func() {
			type capturedRequest struct {
				Limit string
				Since string
				Until string
			}

			var requests []capturedRequest
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/v1/sessions/summary"))
				qp := r.URL.Query()
				requests = append(requests, capturedRequest{
					Limit: qp.Get("limit"),
					Since: qp.Get("since"),
					Until: qp.Get("until"),
				})
				Expect(json.NewEncoder(w).Encode(httpSummaryResponse{})).To(Succeed())
			}))
			defer srv.Close()

			from := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
			until := time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)
			q := NewHTTPQuery(srv.URL, nil)

			overview, err := q.Overview(context.Background(), Filters{From: &from, To: &until})
			Expect(err).NotTo(HaveOccurred())
			Expect(overview).NotTo(BeNil())
			Expect(requests).To(HaveLen(1))
			Expect(requests[0].Limit).To(Equal("25"))
			Expect(requests[0].Since).To(Equal(from.UTC().Format(time.RFC3339)))
			Expect(requests[0].Until).To(Equal(until.UTC().Format(time.RFC3339)))
		})
	})
})
