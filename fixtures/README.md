# tapes fixtures

This is the home and **governance root** for the tapes fixture corpus.
`manifest.json` is the index: it catalogs every fixture family with its location,
provenance, scrub attestation, generator, consumers, and the projection schema and
harness matrix the cut was taken against. Read the manifest first.

## The four-layer pyramid

Everything is generated downhill from one recording:

```
L0  Envelope fixtures      fixtures/envelope/     header set -> expected parsed envelope (+ errors)
L1  Wire recordings        fixtures/recordings/   turn-*/ bundles — verbatim transport bytes
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
  recordings/        ← L1 wire recordings, one directory per capture set
    README.md
    claude-20260727-019fa57e/
      turn-*/{request.json, response.sse, meta.json}
```

## What test data is for

Test data serves distinct purposes, and conflating them is how a public
repository ends up holding something it shouldn't.

1. **Contract fixtures — pin behavior.** The envelope cases, the deriver
   goldens, the console trace fixtures. Their job is to pin a contract and
   exercise edge cases deterministically, in CI, in a public repo. They stay
   **synthetic**: real data is a liability here — nondeterministic, churn-prone,
   and a PII risk. Grow them by *synthesizing* the edge cases real data reveals,
   never by importing a real session.
2. **Wire recordings — pin the transport.** Verbatim bytes for a turn, which
   cannot be synthesized without becoming a guess about what the provider sends.
   These are real captures, and they are admissible only against the
   fixture-grade bar below.
3. **Dev seed data.** Enough realistic data to work against a local clearing.
   May be real and scrubbed, and belongs in a **private** store the clearing
   loads externally — never baked into a public binary or mixed in with (1).
4. **Demo data.** Curated sessions for showing `tapes` off. Hand-picked and held
   to a higher bar than dev seed, because it is shown outside.

Categories (3) and (4) reach real data through a scrubber. The honest limit of a
scrubber is that free-text prompts can leak disclosures no regex will catch —
acceptable for private data, not for public. That limit is the entire reason (1)
stays synthetic, and the reason (2) is a *clean room* rather than a scrub: the
bytes are real, but the session that produced them was constructed to contain
nothing worth scrubbing.

Recordings are their own category precisely because they are the exception. Do
not read "we commit real captures now" as license to relax (1).

### The fixture-grade bar

A recording is committable when it is **synthetic by construction**: captured in
a clean room, credential- and PII-free in headers *and* bodies, and reviewed.
`manifest.json` states the bar; each set records how it met it under
`scrub_detail`.

Review alone is not the bar, because review alone has already failed. The first
pass over the current recordings caught credentials and missed a machine
fingerprint and an account id nested inside a JSON-encoded metadata field —
things a human reading base64 will not reliably see. The bar is the executable
gate (`pkg/backfill/recordings_scrub_test.go`), which matches identifier *forms*
so it fails on the next capture's leak rather than re-attesting this one.

## Versioned cuts (future)

This is an unversioned working-tree cut. Tagged fixture releases (a tarball plus a
`.sha256` and this manifest) let consumers pin an exact version+hash and upgrade
deliberately. When that lands, `manifest.json`'s `cut` block carries the release id.
