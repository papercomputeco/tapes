---
name: dagger-update
description: Updates this repository's Dagger CLI, Engine compatibility contract, generated Go SDK files, Nix lock, CI runtime, and pinned daggerverse modules as one verified change. Use whenever upgrading Dagger or diagnosing Dagger version drift.
---

# Update Dagger

The locked `dagger/nix` input is the source of the executable used locally and
in CI. `dagger.json` is the module API compatibility contract. This repository
intentionally keeps them equal, but they remain different concepts.

Never hand-edit `.dagger/go.mod` or `.dagger/go.sum`; `dagger develop` owns them.
Never replace an immutable daggerverse module reference with a branch.

## 1. Inspect the current state

Start from the repository root and require a clean worktree unless the existing
changes are part of this update.

```bash
make help
git status --short
nix eval --raw .#dagger.version
nix develop --command dagger version
sed -n 's/.*"engineVersion": "v\{0,1\}\([^"]*\)".*/\1/p' dagger.json
rg -n 'DAGGER_VERSION|dagger-for-github|daggerverse/.+@|engineVersion|dagger.io/dagger' \
  dagger.json .dagger .github flake.nix flake.lock
```

The first three versions must match before an ordinary change. CI must obtain
Dagger through `.github/actions/setup-dagger`, not an independently declared
`DAGGER_VERSION`.

## 2. Select the target and its Nix revision

Set the release without a leading `v`:

```bash
TARGET=0.21.7
```

`github:dagger/nix` has no release tags. Find an immutable commit whose
`pkgs/dagger/default.nix` packages the target:

```bash
tmp="$(mktemp -d)"
git clone --filter=blob:none https://github.com/dagger/nix.git "$tmp/dagger-nix"
NIX_REV="$({
  git -C "$tmp/dagger-nix" rev-list origin/main -- pkgs/dagger/default.nix |
    while read -r rev; do
      version="$({
        git -C "$tmp/dagger-nix" show "$rev:pkgs/dagger/default.nix" 2>/dev/null || true
      } | sed -n 's/^[[:space:]]*version = "\([^"]*\)";.*/\1/p')"
      if [ "$version" = "$TARGET" ]; then
        echo "$rev"
        break
      fi
    done
} | head -1)"
rm -rf "$tmp"
test -n "$NIX_REV"
printf 'Dagger %s is packaged by dagger/nix commit %s\n' "$TARGET" "$NIX_REV"
```

Do not blindly run `nix flake update`: the head of `dagger/nix` may already
package a later release, and updating every input creates unrelated churn.

## 3. Update only Dagger and regenerate the module

```bash
nix flake lock --override-input dagger "github:dagger/nix/$NIX_REV"
test "$(nix eval --raw .#dagger.version)" = "$TARGET"
nix develop --command dagger develop
```

`dagger develop` updates `dagger.json` and any tracked generated SDK dependency
files required by that release. Review all of them:

```bash
git diff -- flake.lock dagger.json .dagger/go.mod .dagger/go.sum
nix develop --command make dagger-version-check
```

Confirm regeneration is idempotent without comparing the expected upgrade to
`HEAD`:

```bash
before="$(mktemp)"
after="$(mktemp)"
git diff --binary -- dagger.json .dagger > "$before"
nix develop --command dagger develop
git diff --binary -- dagger.json .dagger > "$after"
cmp "$before" "$after"
rm -f "$before" "$after"
```

## 4. Audit external Dagger modules

Every remote module used from CI must have an immutable commit after `@`.
A floating daggerverse branch can begin requiring a newer Engine without any
change in this repository.

```bash
rg -n 'dagger (call|check).* -m github.com/papercomputeco/daggerverse|daggerverse/' \
  dagger.json .github makefile
```

When updating a module pin:

1. Choose an immutable daggerverse commit.
2. Read that module's `dagger.json` at the commit.
3. Verify its `engineVersion` is not newer than `$TARGET`.
4. Put the commit in the module reference or `pin` field.
5. Never combine a module update with a floating `@main` reference.

The `source` label in a `dagger.json` dependency may mention `@main`; its
immutable `pin` field is what makes resolution reproducible.

## 5. Validate

Use the locked shell for every Dagger command:

```bash
nix flake check
nix develop --command make dagger-version-check
nix develop --command make format
nix develop --command make check
nix develop --command make test
nix develop --command make e2e-test
```

Database-backed tests require the repository's pinned Postgres image. Use
`make test-db-up` or `make test-local`; never start an ad-hoc Postgres image.

Finally verify that no independent CI version declaration was reintroduced:

```bash
if rg -n 'DAGGER_VERSION|dagger/dagger-for-github' .github/workflows; then
  echo 'CI must use .github/actions/setup-dagger and the locked flake package' >&2
  exit 1
fi

git status --short
git diff --check
```

Report the target version, selected `dagger/nix` revision, changed generated
files, pinned daggerverse commits, validation commands, and any failures.
