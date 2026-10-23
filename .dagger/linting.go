package main

import (
	"context"

	"dagger/tapes/internal/dagger"
)

// lintOpts returns the common GolangcilintOpts used by both CheckLint and FixLint.
// It uses the daggerverse module's default golangci-lint image.
func (t *Tapes) lintOpts() dagger.GolangcilintOpts {
	return dagger.GolangcilintOpts{
		Source: t.Source,
		Config: t.Source.File(".golangci.yml"),
	}
}

// CheckLint runs golangci-lint against the tapes source code without applying fixes.
//
// +check
func (t *Tapes) CheckLint(ctx context.Context) (string, error) {
	return dag.Golangcilint(t.lintOpts()).Check(ctx)
}

// FixLint runs golangci-lint against the tapes source code with --fix, applying
// automatic fixes where possible, and returns the modified source directory.
func (t *Tapes) FixLint(ctx context.Context) *dagger.Directory {
	return dag.Golangcilint(t.lintOpts()).Lint()
}
