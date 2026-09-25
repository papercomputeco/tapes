package api

// GET /v1/sessions/:id/raw_turns pages headers by raw turn id, over the
// same stub the size specs use: the stub pages the way the store does,
// so a walk over it is held to the unpaginated list.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	tapeslogger "github.com/papercomputeco/tapes/pkg/logger"
	"github.com/papercomputeco/tapes/pkg/storage"
	"github.com/papercomputeco/tapes/pkg/storage/inmemory"
)

var _ = Describe("raw turn headers pagination", func() {
	const sessionID = "7a1d2c3e-4b5f-4a6b-8c7d-9e0f1a2b3c4d"
	const otherSessionID = "0f9e8d7c-6b5a-4493-8271-605f4e3d2c1b"

	session := storage.SessionRecord{
		ID:               sessionID,
		HarnessID:        "claude-code",
		HarnessSessionID: "harness-session-1",
	}

	// headers builds n wire-log rows with ids 1..n. Ids are the store's
	// sequence, so they need not be dense in general; here they are, which
	// makes the walk's expectations readable.
	headers := func(n int) []storage.RawTurnHeader {
		out := make([]storage.RawTurnHeader, 0, n)
		for i := 1; i <= n; i++ {
			out = append(out, storage.RawTurnHeader{
				ID:         int64(i),
				Source:     storage.RawTurnSourceWire,
				RequestID:  "req-" + strconv.Itoa(i),
				ReceivedAt: time.Date(2026, 9, 24, 12, 0, i, 0, time.UTC),
			})
		}
		return out
	}

	newServer := func(rows []storage.RawTurnHeader) *Server {
		GinkgoHelper()
		driver := &rawTurnHeaderStub{Driver: inmemory.NewDriver(), session: session, headers: rows}
		server, err := NewServer(Config{ListenAddr: ":0"}, driver, tapeslogger.NewNoop())
		Expect(err).NotTo(HaveOccurred())
		return server
	}

	get := func(server *Server, query string) (int, []byte) {
		GinkgoHelper()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			"/v1/sessions/"+sessionID+"/raw_turns"+query, nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		return resp.StatusCode, body
	}

	getPage := func(server *Server, query string) RawTurnListResponse {
		GinkgoHelper()
		status, body := get(server, query)
		Expect(status).To(Equal(http.StatusOK), string(body))
		var page RawTurnListResponse
		Expect(json.Unmarshal(body, &page)).To(Succeed(), string(body))
		return page
	}

	// walk follows next_cursor from the first page to the last and returns
	// every page in order. query is the per-request query string without a
	// cursor; the walk appends its own.
	walk := func(server *Server, query string) []RawTurnListResponse {
		GinkgoHelper()
		var pages []RawTurnListResponse
		cursor := ""
		for {
			Expect(len(pages)).To(BeNumerically("<", 1000), "the cursor never ran out")
			q := query
			if cursor != "" {
				if q == "" {
					q = "?cursor=" + cursor
				} else {
					q += "&cursor=" + cursor
				}
			}
			page := getPage(server, q)
			pages = append(pages, page)
			if page.NextCursor == "" {
				return pages
			}
			cursor = page.NextCursor
		}
	}

	ids := func(items []RawTurnHeaderItem) []int64 {
		out := make([]int64, 0, len(items))
		for _, it := range items {
			out = append(out, it.ID)
		}
		return out
	}

	It("pages raw turn headers by id", func() {
		rows := headers(20)
		server := newServer(rows)

		// The reference is the listing served whole: one page large
		// enough to hold every row, which has no next page.
		whole := getPage(server, "?limit=1000")
		Expect(whole.Items).To(HaveLen(20))
		Expect(whole.NextCursor).To(BeEmpty())

		pages := walk(server, "?limit=7")
		Expect(pages).To(HaveLen(3), "20 rows at 7 per page")
		concat := make([]RawTurnHeaderItem, 0, len(rows))
		for i, page := range pages {
			if i < len(pages)-1 {
				Expect(page.Items).To(HaveLen(7))
				Expect(page.NextCursor).NotTo(BeEmpty())
			}
			concat = append(concat, page.Items...)
		}
		Expect(pages[2].Items).To(HaveLen(6))
		Expect(pages[2].NextCursor).To(BeEmpty(), "absence of the cursor, not page length, is the end")
		Expect(concat).To(Equal(whole.Items), "the walk equals the unpaginated list")

		// Each page resumes strictly after the last id the previous one
		// served: nothing repeats, nothing is skipped.
		Expect(ids(pages[1].Items)[0]).To(Equal(ids(pages[0].Items)[6] + 1))
		Expect(ids(pages[2].Items)[0]).To(Equal(ids(pages[1].Items)[6] + 1))

		// A cursor is bound to the boundary it names, so a walk that
		// begins mid-list serves only what follows.
		mid := encodeRawTurnsPageCursor(rawTurnsPageCursor{Session: sessionID, ID: 18})
		tail := getPage(server, "?cursor="+mid)
		Expect(ids(tail.Items)).To(Equal([]int64{19, 20}))
		Expect(tail.NextCursor).To(BeEmpty())
	})

	It("defaults to 200 raw turns", func() {
		server := newServer(headers(250))

		page := getPage(server, "")
		Expect(page.Items).To(HaveLen(200))
		Expect(page.NextCursor).NotTo(BeEmpty())

		rest := getPage(server, "?cursor="+page.NextCursor)
		Expect(rest.Items).To(HaveLen(50))
		Expect(rest.NextCursor).To(BeEmpty())
		Expect(rest.Items[0].ID).To(Equal(page.Items[199].ID + 1))

		// A session that fits exactly in the default page has no next
		// page: absence of the cursor, not page length, is the end.
		exact := getPage(newServer(headers(200)), "")
		Expect(exact.Items).To(HaveLen(200))
		Expect(exact.NextCursor).To(BeEmpty())
	})

	It("clamps raw-turn limit to 1000", func() {
		server := newServer(headers(1005))

		page := getPage(server, "?limit=5000")
		Expect(page.Items).To(HaveLen(1000))
		Expect(page.NextCursor).NotTo(BeEmpty())

		rest := getPage(server, "?limit=5000&cursor="+page.NextCursor)
		Expect(rest.Items).To(HaveLen(5))
		Expect(rest.NextCursor).To(BeEmpty())
	})

	It("rejects a limit that is not a positive integer", func() {
		server := newServer(headers(3))
		for _, raw := range []string{"0", "-1", "abc"} {
			status, body := get(server, "?limit="+raw)
			Expect(status).To(Equal(http.StatusBadRequest), raw)
			Expect(string(body)).To(ContainSubstring("limit must be a positive integer"))
		}
	})

	It("rejects a cursor for another session", func() {
		server := newServer(headers(3))

		foreign := encodeRawTurnsPageCursor(rawTurnsPageCursor{Session: otherSessionID, ID: 1})
		status, body := get(server, "?cursor="+foreign)
		Expect(status).To(Equal(http.StatusBadRequest))
		Expect(string(body)).To(ContainSubstring("cursor does not match session"))

		// A token that is not one of ours is a 400 too, not a first page.
		status, body = get(server, "?cursor=not-a-cursor")
		Expect(status).To(Equal(http.StatusBadRequest))
		Expect(string(body)).To(ContainSubstring("invalid cursor"))

		// The checks run before the session is looked up, so a bad cursor
		// on an unknown session is still a 400, matching the other pagers.
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			fmt.Sprintf("/v1/sessions/%s/raw_turns?cursor=%s", otherSessionID, foreign), nil)
		Expect(err).NotTo(HaveOccurred())
		resp, err := server.app.Test(req)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		Expect(resp.StatusCode).To(Equal(http.StatusNotFound),
			"a matching cursor on an unknown session falls through to the session lookup")
	})
})
