package main

import (
	"context"
	"fmt"

	"dagger/tapes/internal/dagger"
)

const golangciLintVersion = "v2.8.0"

// lintOpts returns the common GolangcilintOpts used by both CheckLint and FixLint.
// It layers golangci-lint on top of goContainer() so the sqlite dev headers,
// CGO, and Go caches are already in place.
func (t *Tapes) lintOpts() dagger.GolangcilintOpts {
	base := t.goContainer().
		WithExec([]string{
			"go",
			"install",
			fmt.Sprintf("github.com/golangci/golangci-lint/v2/cmd/golangci-lint@%s", golangciLintVersion),
		})

	return dagger.GolangcilintOpts{
		BaseCtr: base,
		Config:  t.Source.File(".golangci.yml"),
	}
}

// CheckLint runs golangci-lint against the tapes source code without applying fixes.
func (t *Tapes) CheckLint(ctx context.Context) (string, error) {
	return dag.Golangcilint(t.Source, t.lintOpts()).Check(ctx)
}

// FixLint runs golangci-lint against the tapes source code with --fix, applying
// automatic fixes where possible, and returns the modified source directory.
func (t *Tapes) FixLint(ctx context.Context) *dagger.Directory {
	return dag.Golangcilint(t.Source, t.lintOpts()).Lint()
}
