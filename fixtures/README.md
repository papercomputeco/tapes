# tapes fixtures

This is the home and **governance root** for the tapes fixture corpus.
`manifest.json` is the index: it catalogs every fixture family with its location,
provenance, scrub attestation, generator, consumers, and the projection schema and
harness matrix the cut was taken against. Read the manifest first.

## The four-layer pyramid

Everything is generated downhill from one recording:

```
L0  Envelope fixtures      fixtures/envelope/     header set -> expected parsed envelope (+ errors)
L1  Wire recordings        fixtures/recordings/   turn-*/ bundles — verbatim transport bytes (placeholder)
L2  Corpus (.jsonl.gz)     pkg/seed/corpus/       raw_turns rows — derive gates, seed
L3  Rendered API fixtures  (generated downstream)  via `tapes dev trace-fixtures`
```

One clean-room capture session -> the wire-trace recorder emits **L1** -> ingest +
`tapes dev dump-corpus` emits **L2** -> `tapes dev trace-fixtures` emits **L3**. The
**L0** envelope cases are synthesized directly from the header contract, not captured.

## Why some families live outside this directory

`manifest.json` is the point of consolidation; not every family is physically under
`fixtures/`, and the manifest records where each one lives and why:

- **Envelope (L0)** and **recordings (L1)** live here (`fixtures/envelope`,
  `fixtures/recordings`).
- **Derive corpus (L2)** lives under `pkg/seed/corpus/`. The demo seed `go:embed`s the
  corpora, and a `go:embed` directive cannot reference a path above its own package —
  so moving the ~26 MB of corpora under a shared top-level `fixtures` package would
  either be impossible (the seed couldn't embed them) or would pull 26 MB into every
  binary that imported that package. Homing them next to the seed keeps the embed
  isolated to the seed path. The derive **goldens** live next to the derive tests that
  regenerate them (`pkg/derive/testdata/`).
- **Capture reducer fixtures (L2-adjacent)** are exported as the Go package
  `pkg/capture/fixtures` (an `embed.FS`) so every consumer that embeds those reducers
  exercises the same bytes.
- **Rendered API fixtures (L3)** are generated for downstream client/UI test suites and
  stored in the consuming project so those tests stay language-independent of the
  deriver.

## Layout

```
fixtures/
  manifest.json      ← the per-cut index (start here)
  README.md          ← this file
  envelope/          ← L0 synthetic header<->envelope cases
    README.md
    cases/*.json
  recordings/        ← L1 placeholder
    README.md
```

## Versioned cuts (future)

This is an unversioned working-tree cut. Tagged fixture releases (a tarball plus a
`.sha256` and this manifest) let consumers pin an exact version+hash and upgrade
deliberately. When that lands, `manifest.json`'s `cut` block carries the release id.
