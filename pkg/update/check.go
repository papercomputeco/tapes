// Package update provides a best-effort mechanism for checking whether a newer
// version of tapes is available. The check is intentionally soft: network
// errors, timeouts, and malformed responses are silently ignored so that
// normal CLI operation is never disrupted.
package update

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/mod/semver"
)

const (
	// versionURL is the public URL serving the latest released semver tag.
	versionURL = "https://download.tapes.dev/latest/version"

	// httpTimeout caps how long we wait for the version check response.
	httpTimeout = 2 * time.Second

	// maxBodyBytes prevents reading an unexpectedly large response.
	maxBodyBytes = 128
)

// CheckForUpdate compares currentVersion against the latest published version
// at download.tapes.dev/latest/version. Returns the remote version string when
// an update is available, or "" if current, unreachable, or a non-release
// build. This function never returns an error.
func CheckForUpdate(currentVersion string) string {
	return checkForUpdate(currentVersion, versionURL)
}

// checkForUpdate is the internal implementation, accepting a configurable URL
// so tests can point at httptest servers.
func checkForUpdate(currentVersion, url string) string {
	if shouldSkip(currentVersion) {
		return ""
	}

	remoteVersion, ok := fetchRemoteVersion(url)
	if !ok {
		return ""
	}

	if semver.Compare(currentVersion, remoteVersion) < 0 {
		return remoteVersion
	}

	return ""
}

// shouldSkip returns true for non-release builds that should never trigger
// an update prompt (dev builds, nightlies, empty strings, or anything that
// isn't a valid semver tag).
func shouldSkip(v string) bool {
	switch v {
	case "", "dev", "nightly":
		return true
	}

	return !semver.IsValid(v)
}

// fetchRemoteVersion GETs the version file and returns the trimmed content.
// Returns ("", false) on any failure.
func fetchRemoteVersion(url string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", false
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return "", false
	}

	v := strings.TrimSpace(string(body))
	if !semver.IsValid(v) {
		return "", false
	}

	return v, true
}
