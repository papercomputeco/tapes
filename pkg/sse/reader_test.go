package sse

import (
	"bytes"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Reader", func() {
	var dst *bytes.Buffer

	BeforeEach(func() {
		dst = &bytes.Buffer{}
	})

	Describe("Next", func() {
		Context("with standard SSE events", func() {
			It("parses a single event", func() {
				src := strings.NewReader("data: hello world\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(Equal("hello world"))
				Expect(ev.Type).To(BeEmpty())
				Expect(ev.ID).To(BeEmpty())

				ev, err = r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev).To(BeNil())
			})

			It("parses multiple events", func() {
				src := strings.NewReader("data: first\n\ndata: second\n\n")
				r := NewTeeReader(src, dst)

				ev1, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev1.Data).To(Equal("first"))

				ev2, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev2.Data).To(Equal("second"))

				ev3, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev3).To(BeNil())
			})

			It("parses event type", func() {
				src := strings.NewReader("event: content_block_delta\ndata: {\"type\":\"delta\"}\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Type).To(Equal("content_block_delta"))
				Expect(ev.Data).To(Equal("{\"type\":\"delta\"}"))
			})

			It("parses event ID", func() {
				src := strings.NewReader("id: 42\ndata: hello\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.ID).To(Equal("42"))
				Expect(ev.Data).To(Equal("hello"))
			})

			It("joins multiple data lines with newline", func() {
				src := strings.NewReader("data: line one\ndata: line two\ndata: line three\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(Equal("line one\nline two\nline three"))
			})
		})

		Context("with OpenAI-style SSE", func() {
			It("parses OpenAI streaming chunks", func() {
				input := "data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n" +
					"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n" +
					"data: [DONE]\n\n"
				src := strings.NewReader(input)
				r := NewTeeReader(src, dst)

				ev1, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev1.Data).To(Equal("{\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}"))

				ev2, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev2.Data).To(Equal("{\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\" world\"}}]}"))

				ev3, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev3.Data).To(Equal("[DONE]"))

				ev4, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev4).To(BeNil())
			})
		})

		Context("with Anthropic-style SSE", func() {
			It("parses Anthropic streaming events with event types", func() {
				input := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\"}}\n\n" +
					"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"Hello\"}}\n\n" +
					"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
				src := strings.NewReader(input)
				r := NewTeeReader(src, dst)

				ev1, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev1.Type).To(Equal("message_start"))
				Expect(ev1.Data).To(ContainSubstring("message_start"))

				ev2, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev2.Type).To(Equal("content_block_delta"))
				Expect(ev2.Data).To(ContainSubstring("Hello"))

				ev3, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev3.Type).To(Equal("message_stop"))

				ev4, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev4).To(BeNil())
			})
		})

		Context("with SSE comments", func() {
			It("ignores comment lines in parsed events", func() {
				src := strings.NewReader(": this is a comment\ndata: hello\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(Equal("hello"))
			})

			It("forwards comment lines to dst", func() {
				src := strings.NewReader(": keep-alive\ndata: hello\n\n")
				r := NewTeeReader(src, dst)

				_, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(dst.String()).To(ContainSubstring(": keep-alive\n"))
			})
		})

		Context("with data field variations", func() {
			It("handles data field with no space after colon", func() {
				src := strings.NewReader("data:no-space\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(Equal("no-space"))
			})

			It("handles empty data field", func() {
				src := strings.NewReader("data:\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(BeEmpty())
			})

			It("handles data field with only a space (empty value per spec)", func() {
				src := strings.NewReader("data: \n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(BeEmpty())
			})
		})

		Context("verbatim byte forwarding", func() {
			It("forwards all bytes including \\n\\n delimiters to dst", func() {
				input := "data: first\n\ndata: second\n\n"
				src := strings.NewReader(input)
				r := NewTeeReader(src, dst)

				_, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				_, err = r.Next()
				Expect(err).NotTo(HaveOccurred())

				Expect(dst.String()).To(Equal(input))
			})

			It("preserves exact OpenAI SSE framing in dst", func() {
				input := "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\ndata: [DONE]\n\n"
				src := strings.NewReader(input)
				r := NewTeeReader(src, dst)

				_, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				_, err = r.Next()
				Expect(err).NotTo(HaveOccurred())

				Expect(dst.String()).To(Equal(input))
			})

			It("preserves exact Anthropic SSE framing in dst", func() {
				input := "event: content_block_delta\ndata: {\"delta\":{\"text\":\"Hi\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
				src := strings.NewReader(input)
				r := NewTeeReader(src, dst)

				_, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				_, err = r.Next()
				Expect(err).NotTo(HaveOccurred())

				Expect(dst.String()).To(Equal(input))
			})

			It("preserves comment lines in dst output", func() {
				input := ": comment\ndata: hello\n\n"
				src := strings.NewReader(input)
				r := NewTeeReader(src, dst)

				_, err := r.Next()
				Expect(err).NotTo(HaveOccurred())

				Expect(dst.String()).To(Equal(input))
			})
		})

		Context("edge cases", func() {
			It("returns nil on empty input", func() {
				src := strings.NewReader("")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev).To(BeNil())
			})

			It("returns nil on input with only blank lines", func() {
				src := strings.NewReader("\n\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev).To(BeNil())
			})

			It("yields event when stream ends without trailing blank line", func() {
				src := strings.NewReader("data: unterminated")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(Equal("unterminated"))

				ev, err = r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev).To(BeNil())
			})

			It("skips leading blank lines before first event", func() {
				src := strings.NewReader("\n\ndata: hello\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(Equal("hello"))
			})

			It("ignores unknown fields", func() {
				src := strings.NewReader("retry: 3000\nfoo: bar\ndata: hello\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(Equal("hello"))
			})

			It("handles field with no colon", func() {
				// Per spec: if a line has no colon, the entire line is the field name
				// with an empty value. Unknown fields are ignored.
				src := strings.NewReader("data\n\n")
				r := NewTeeReader(src, dst)

				ev, err := r.Next()
				Expect(err).NotTo(HaveOccurred())
				Expect(ev.Data).To(BeEmpty())
			})
		})
	})
})
