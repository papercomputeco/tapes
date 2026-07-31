package main

import (
	"context"
	"errors"
	"fmt"

	"dagger/tapes/internal/dagger"
)

// CheckParity runs the contract-fixture gates: the envelope corpus parity gate
// (seal, schema, coverage), the parser oracle that holds this repo's reader to
// that corpus, and the envelope validation rules the corpus's error cases
// declare.
//
// It exists as a check of its own rather than as part of Test for two reasons.
//
// The first is that it needs nothing. No Postgres, no Ollama, no services at
// all — it is fixtures and pure functions. Test binds a database because most
// of the suite needs one, and that cost is why Test is not on the PR path
// today; CI runs TestE2E, Build and Smoke. That left the envelope contract —
// which is the agreement between capture paths in *different repositories* —
// gated by nothing on a pull request, even though a corpus edit is exactly the
// kind of change whose blast radius reaches other repos. A check with no
// service dependencies can run on every PR in seconds.
//
// The second is what a failure here means. A red CheckParity says the shared
// contract moved, and the fix usually involves another repository: re-sync a
// vendored copy, or land a parser change beside the fixture change. Keeping it
// separate makes that legible from the check name alone, instead of arriving
// as one failed assertion inside a full unit-test run.
//
// +check
func (t *Tapes) CheckParity(ctx context.Context) (string, error) {
	out, err := t.goContainer().
		WithExec([]string{
			"go", "test", "-count=1", "./pkg/backfill/...", "./pkg/sessions/...",
		}).
		Stdout(ctx)

	var e *dagger.ExecError
	if errors.As(err, &e) {
		return "", fmt.Errorf(
			"envelope contract fixtures are failing.\n\n"+
				"If the corpus changed on purpose, copy the digest the gate prints into\n"+
				"fixtures/envelope/DIGEST and re-sync every vendored copy from this commit.\n\n%s\n%s",
			e.Stdout, e.Stderr,
		)
	} else if err != nil {
		return "", fmt.Errorf("unexpected error: %w", err)
	}

	return fmt.Sprintf("envelope contract fixtures pass:\n%s", out), nil
}
