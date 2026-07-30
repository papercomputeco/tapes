package tapesoapi

import "strings"

// SynthesizeOperationID derives an operationId from a method and an OpenAPI
// path.
//
// It is exported because two callers need to agree on the answer. The Fiber
// adapter names an undocumented route with it, and an aggregate names an
// ingested operation that arrived without an id with it — and if those two ever
// disagreed, the same route would be called one thing in core's own contract and
// another in the aggregate that republishes it.
//
// Deterministic, so a compiled document does not churn between builds: same
// method and path, same id, always.
//
//	GET /v1/sessions/{id} → getV1SessionsId
func SynthesizeOperationID(method, openAPIPath string) string {
	var b strings.Builder
	b.WriteString(strings.ToLower(method))
	for segment := range strings.SplitSeq(openAPIPath, "/") {
		// The path characters that are legal in a URL but not in a generated
		// identifier all come out; a `{id}` keeps its name because "which
		// parameter" is the informative part.
		cleaned := strings.NewReplacer("{", "", "}", "", ".", "", "-", "", "_", "").Replace(segment)
		if cleaned == "" {
			continue
		}
		b.WriteString(strings.ToUpper(cleaned[:1]))
		b.WriteString(cleaned[1:])
	}

	return b.String()
}

// prefixOperationIDs gives every operation in the fragment a prefixed id.
//
// Synthesize-then-prefix, in that order, because the goal is one id per
// operation that no other document can collide with, and an operation that
// arrived without an id has nothing to prefix. Skipping those would leave the
// aggregate holding anonymous operations — which is the same defect the prefix
// is here to prevent, just spelled as absence rather than as a duplicate.
func prefixOperationIDs(fragment *Fragment, prefix string) {
	for _, path := range fragment.paths() {
		item := fragment.Paths[path]
		if item == nil {
			continue
		}
		for _, method := range item.Methods() {
			operation := item.Operations[method]
			if operation == nil {
				continue
			}
			id := strings.TrimSpace(operation.OperationID)
			if id == "" {
				id = SynthesizeOperationID(method, path)
			}
			operation.OperationID = prefix + id
		}
	}
}
