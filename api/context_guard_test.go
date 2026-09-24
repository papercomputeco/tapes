package api

// Guard for the request-context contract of this package. Under Fiber v3,
// c.RequestCtx() is the fasthttp context: its Done() fires only on server
// shutdown and it carries no deadline, so a client hang-up or gateway timeout
// cancels nothing scheduled on it. c.Context() is the user context seeded by
// requestIDMiddleware, which is where cancellation and deadlines live. Every
// storage, driver, and cassette-spec call must therefore take c.Context().
//
// This spec scans the package source so a new handler cannot quietly reach
// for the fasthttp context again.

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// requestCtxAllowlist names the files that may call c.RequestCtx(), each with
// the reason the use is about the raw fasthttp request rather than storage.
var requestCtxAllowlist = map[string]string{
	"request_id_middleware.go": "seeds the user context from the fasthttp context; the one place the derivation must start",
	"cassettes.go":             "raw connection access: response body stream, connection-close, and client-gone watch on the net.Conn",
	"cassette_stream.go":       "builds the upstream proxy request from the wire method, URI, host, remote address, and TLS state",
}

var _ = Describe("request context plumbing", func() {
	It("passes the fasthttp context only where the raw request is needed", func() {
		sources, err := filepath.Glob("*.go")
		Expect(err).NotTo(HaveOccurred())
		Expect(sources).NotTo(BeEmpty(), "spec must run from the api package directory")

		var offenders []string
		for _, path := range sources {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			if _, allowed := requestCtxAllowlist[filepath.Base(path)]; allowed {
				continue
			}
			offenders = append(offenders, requestCtxUses(path)...)
		}

		Expect(offenders).To(BeEmpty(),
			"c.RequestCtx() is the fasthttp context and is never cancelled by the client; "+
				"storage calls must take c.Context(). Offending sites:\n  "+
				strings.Join(offenders, "\n  "))
	})
})

// requestCtxUses returns every file:line in path whose code (not a comment)
// calls RequestCtx().
func requestCtxUses(path string) []string {
	file, err := os.Open(path)
	Expect(err).NotTo(HaveOccurred())
	defer file.Close()

	var uses []string
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(text, "//") {
			continue
		}
		if strings.Contains(text, ".RequestCtx()") {
			uses = append(uses, fmt.Sprintf("%s:%d: %s", path, line, text))
		}
	}
	Expect(scanner.Err()).NotTo(HaveOccurred())

	return uses
}
