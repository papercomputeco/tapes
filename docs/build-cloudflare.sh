#!/usr/bin/env bash
#
# This script is used by cloudflare's Page deploy step to:
# 1) Download mdBook from the GitHub release
# 2) Verify the release
# 3) Build the mdbook docs in ./docs/ to ./docs/book

set -euo pipefail

version="0.5.4"
target="x86_64-unknown-linux-gnu"
sha256="3f28de05dafca9d0f2eab99c662116b0e37b89b1d96a08f8f430b9eeae958cd7"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

archive="$tmp/mdbook.tar.gz"
url="https://github.com/rust-lang/mdBook/releases/download/v${version}/mdbook-v${version}-${target}.tar.gz"

curl --fail --silent --show-error --location "$url" --output "$archive"
printf '%s  %s\n' "$sha256" "$archive" | sha256sum --check

tar -xzf "$archive" -C "$tmp" mdbook

cd "$repo_root"
PATH="$tmp:$PATH" make docs-build
