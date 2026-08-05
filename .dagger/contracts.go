package main

import (
	"dagger/tapes/internal/dagger"
)

// Contracts compiles both published OpenAPI documents and returns them as a
// directory: tapes-api.yaml and tapes-ingest.yaml.
//
// These are the documents consumers vendor. Nothing in this repository reads
// them, and they are not checked in — a contract file is a copy of what the
// server already states, and a copy can go stale. What is checked in is the
// fingerprint of each (api/CONTRACT, ingest/CONTRACT), which is a copy that
// cannot go stale quietly because a spec recompiles and compares it.
//
// Prose is INCLUDED here, the opposite of what the seals cover. The two want
// opposite things for the same reason: a seal should fire only when the shape
// changes, so it excludes comments; a vendored document is read by a person
// writing a client, so it carries every field's prose. `--docs-root .` is what
// folds those comments in, and it needs a checkout — which is why this runs in
// the source container rather than being something a released binary could do.
//
// The documents are CORE only. Neither is the aggregate a running server
// publishes at /openapi, which is this document with each mounted cassette's
// spec merged in and therefore a function of deployment rather than of release.
func (t *Tapes) Contracts() *dagger.Directory {
	return t.goContainer().
		WithExec([]string{"mkdir", "-p", "/out"}).
		WithExec([]string{
			"go", "run", "./cli/tapes", "dev", "openapi", "api",
			"--docs-root", ".", "--out", "/out/tapes-api.yaml",
		}).
		WithExec([]string{
			"go", "run", "./cli/tapes", "dev", "openapi", "ingest",
			"--docs-root", ".", "--out", "/out/tapes-ingest.yaml",
		}).
		Directory("/out")
}
