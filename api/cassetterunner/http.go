package cassetterunner

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

// NewHTTPClient returns the shared cassette client policy. Call sites own their
// operation-specific timeout through request contexts.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Transport: http.DefaultTransport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// RewriteProxyRequest applies the target and forwarding policy shared by the
// REST proxy and MCP tool calls.
func RewriteProxyRequest(request *httputil.ProxyRequest, target *url.URL, path string, name cassette.Name) {
	request.Out.URL.Scheme = target.Scheme
	request.Out.URL.Host = target.Host
	request.Out.URL.Path = path
	request.Out.URL.RawPath = ""
	request.Out.Host = target.Host
	request.Out.Header.Del("Forwarded")
	request.Out.Header.Del("X-Forwarded-For")
	request.Out.Header.Del("X-Forwarded-Host")
	request.Out.Header.Del("X-Forwarded-Proto")
	request.SetXForwarded()
	request.Out.Header.Set("X-Tapes-Cassette", string(name))
}

// CopyRequestHeaders copies caller identity and tracing headers to a fabricated
// upstream request while dropping transport and representation controls that
// do not apply to its new JSON body.
func CopyRequestHeaders(target, source http.Header) {
	for key, values := range source {
		if excludedRequestHeader(key) {
			continue
		}
		for _, value := range values {
			target.Add(key, value)
		}
	}
	for _, value := range source.Values("Connection") {
		for name := range strings.SplitSeq(value, ",") {
			target.Del(strings.TrimSpace(name))
		}
	}
}

func excludedRequestHeader(name string) bool {
	switch http.CanonicalHeaderKey(name) {
	case "Accept-Encoding", "Connection", "Content-Encoding", "Content-Length", "Content-Md5", "Digest", "Expect", "Forwarded", "If-Match", "If-Modified-Since", "If-None-Match", "If-Range", "If-Unmodified-Since", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Range", "Te", "Trailer", "Transfer-Encoding", "Upgrade", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto":
		return true
	default:
		return false
	}
}
