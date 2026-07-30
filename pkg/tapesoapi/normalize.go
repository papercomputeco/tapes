package tapesoapi

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
)

// methods is the set of HTTP methods OpenAPI defines an operation slot for.
// TRACE is included because 3.0 defines it; CONNECT is not, because neither
// version does.
var methods = map[string]struct{}{
	http.MethodGet:     {},
	http.MethodPut:     {},
	http.MethodPost:    {},
	http.MethodDelete:  {},
	http.MethodOptions: {},
	http.MethodHead:    {},
	http.MethodPatch:   {},
	http.MethodTrace:   {},
}

// normalizeMethod uppercases a method and rejects one OpenAPI has no slot for.
func normalizeMethod(method string) (string, error) {
	upper := strings.ToUpper(strings.TrimSpace(method))
	if _, ok := methods[upper]; !ok {
		return "", fmt.Errorf("method %q has no OpenAPI operation slot", method)
	}

	return upper, nil
}

var templateParam = regexp.MustCompile(`\{([^{}/]*)\}`)

// normalizePath canonicalizes a path so two spellings of the same route cannot
// both appear in one document.
//
// Trailing slashes are trimmed (except on the root) and the path must be
// absolute. Parameter names are left alone: `{id}` and `{userId}` are different
// paths to OpenAPI even when they route identically, and silently unifying them
// would publish an operation nobody wrote.
func normalizePath(path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", errors.New("empty path")
	}
	if !strings.HasPrefix(trimmed, "/") {
		return "", fmt.Errorf("path %q must begin with /", path)
	}
	if strings.ContainsAny(trimmed, " \t\n") {
		return "", fmt.Errorf("path %q contains whitespace", path)
	}
	if idx := strings.IndexAny(trimmed, "?#"); idx >= 0 {
		return "", fmt.Errorf("path %q must not carry a query string or fragment", path)
	}
	if len(trimmed) > 1 {
		trimmed = strings.TrimSuffix(trimmed, "/")
	}
	for _, match := range templateParam.FindAllStringSubmatch(trimmed, -1) {
		if strings.TrimSpace(match[1]) == "" {
			return "", fmt.Errorf("path %q has an unnamed template parameter", path)
		}
	}
	if strings.Count(trimmed, "{") != strings.Count(trimmed, "}") {
		return "", fmt.Errorf("path %q has unbalanced template braces", path)
	}

	return trimmed, nil
}

// PathParams returns the template parameter names in a path, in order.
func PathParams(path string) []string {
	matches := templateParam.FindAllStringSubmatch(path, -1)
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		names = append(names, match[1])
	}

	return names
}

// joinPath mounts a path under a prefix, keeping exactly one separator.
func joinPath(prefix, path string) string {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return path
	}
	if path == "/" {
		return prefix
	}
	if !strings.HasPrefix(path, "/") {
		return prefix + "/" + path
	}

	return prefix + path
}

// statusOrder sorts response keys so numeric statuses come out ascending and
// "default" sorts last, which is how a reader expects to see them.
func statusOrder(keys []string) []string {
	sorted := append([]string(nil), keys...)
	sort.Slice(sorted, func(i, j int) bool {
		left, right := sorted[i], sorted[j]
		if left == defaultResponseKey {
			return false
		}
		if right == defaultResponseKey {
			return true
		}

		return left < right
	})

	return sorted
}
