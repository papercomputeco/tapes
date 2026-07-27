package cassetterunner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/papercomputeco/tapes/pkg/cassette"
	"github.com/papercomputeco/tapes/pkg/openapi"
)

// maxSpecBytes caps a fetched OpenAPI document. Specs are tens of kilobytes;
// the limit is here so an unreachable-but-answering endpoint cannot stream
// core out of memory.
const maxSpecBytes = 8 << 20

// fetched is one conditional GET of a cassette's OpenAPI document.
type fetched struct {
	// document is the parsed body, nil when the server answered 304.
	document *openapi.Document

	// etag is the validator to send on the next fetch, empty when the server
	// published none.
	etag string

	// notModified reports that the cached copy is still current.
	notModified bool
}

// fetch retrieves and parses an OpenAPI document, revalidating with etag when
// one is held.
//
// Both refresh paths go through here. They differ in what they do with the
// result — one admits a cassette it has never seen, the other refreshes one an
// operator registered — but the transport rules are identical, and keeping one
// implementation is what stops them from drifting into two sets of size
// limits, status handling, and error text.
func (runner *Runner) fetch(ctx context.Context, source, etag string) (fetched, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
	if err != nil {
		return fetched{}, err
	}
	request.Header.Set("Accept", "application/json")
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}

	response, err := runner.client.Do(request)
	if err != nil {
		return fetched{}, fmt.Errorf("fetching %s: %w", safeSource(source), err)
	}
	defer response.Body.Close()

	validator := response.Header.Get("ETag")
	if response.StatusCode == http.StatusNotModified {
		return fetched{etag: etag, notModified: true}, nil
	}
	if response.StatusCode != http.StatusOK {
		return fetched{}, fmt.Errorf("%s returned %s", safeSource(source), response.Status)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxSpecBytes+1))
	if err != nil {
		return fetched{}, fmt.Errorf("reading %s: %w", safeSource(source), err)
	}
	if len(body) > maxSpecBytes {
		return fetched{}, fmt.Errorf("%s exceeds %d bytes", safeSource(source), maxSpecBytes)
	}

	document, err := openapi.Parse(body)
	if err != nil {
		return fetched{}, err
	}

	return fetched{document: document, etag: validator}, nil
}

// publication is a document rewritten onto core's public surface, in the two
// shapes the cache holds.
type publication struct {
	document []byte
	parsed   *openapi.Document
	digest   cassette.Digest
}

// republish moves every path a cassette declares from its own listener onto
// core's public surface.
//
// The digest covers the republished bytes rather than the fetched ones,
// because those are what a client is handed and caches against: moving the
// public surface has to move the ETag.
func republish(document *openapi.Document, instance *Instance) (*publication, error) {
	rewritten, err := document.RewritePrefix(instance.LocalPrefix(), instance.Prefix())
	if err != nil {
		return nil, fmt.Errorf("cassette %q: %w; the whole document is refused", instance.Name, err)
	}
	encoded, err := rewritten.Marshal()
	if err != nil {
		return nil, fmt.Errorf("cassette %q: %w", instance.Name, err)
	}
	sum := sha256.Sum256(encoded)

	return &publication{
		document: encoded,
		parsed:   rewritten,
		digest:   cassette.Digest("sha256:" + hex.EncodeToString(sum[:])),
	}, nil
}

// sourceOrigin validates a configured source URL and returns the origin to
// proxy cassette traffic to.
//
// The rules are strict because this string becomes a proxy target. Userinfo is
// refused rather than stripped so a credential can never reach a rejection
// message, and a fragment is refused because it would mean the configured
// value and the fetched value disagree.
func sourceOrigin(source string) (string, error) {
	parsed, err := url.Parse(source)
	if err != nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" ||
		parsed.Fragment != "" ||
		parsed.User != nil {
		return "", fmt.Errorf(
			"cassette source %q must be a full http(s) URL without userinfo or a fragment", safeSource(source))
	}

	return parsed.Scheme + "://" + parsed.Host, nil
}

// safeSource renders a source URL with any credential removed, for the error
// messages and rejections core publishes over HTTP.
func safeSource(source string) string {
	parsed, err := url.Parse(source)
	if err != nil {
		return "<invalid URL>"
	}
	if parsed.User == nil {
		return source
	}
	parsed.User = url.User("redacted")

	return parsed.String()
}
