package main

import (
	"context"
	"errors"
	"fmt"

	"dagger/tapes/internal/dagger"
)

// CheckGoModTidy runs "go mod tidy" and fails if it produces any changes to
// go.mod or go.sum, indicating that the caller forgot to tidy before committing.
//
// +check
func (t *Tapes) CheckGoModTidy(ctx context.Context) (string, error) {
	out, err := t.goContainer().
		WithExec([]string{"cp", "go.mod", "go.mod.HEAD"}).
		WithExec([]string{"cp", "go.sum", "go.sum.HEAD"}).
		WithExec([]string{"go", "mod", "tidy"}).
		WithExec([]string{
			"sh", "-c",
			"diff -u go.mod.HEAD go.mod && diff -u go.sum.HEAD go.sum",
		}).
		Stdout(ctx)

	var e *dagger.ExecError
	if errors.As(err, &e) {
		return "", fmt.Errorf(
			"go.mod or go.sum are not tidy: run 'go mod tidy' and commit the changes\n\n%s",
			e.Stdout,
		)
	} else if err != nil {
		return "", fmt.Errorf("unexpected error: %w", err)
	}

	return fmt.Sprintf("go.mod and go.sum are tidy: %s", out), nil
}
