# Content-encoding fixtures — the captured-body decode contract

L0-layer fixtures, sibling to `fixtures/envelope/` and `fixtures/thread/`: small,
synthetic, language-neutral JSON cases that pin how a captured body's
`Content-Encoding` is undone before the bytes reach a reducer.

Where the envelope corpus pins a *header* contract, this one pins a **capture
policy**: which codings are readable, how stacked layers compose, how much
output is allowed, and what happens when a stream is corrupt or arrives half
finished. Those are decisions, not encodings — and they are currently made
independently in three places across two languages:

| Implementation | Where |
| --- | --- |
| Go, reference | `pkg/capture/contentencoding.go` (`DecodeContentEncoding`) |
| Go, gateway lane | `extproc/` — calls the same function, deliberately |
| Rust, client | `tapesctl` `crates/tapesctl/src/start/content_encoding.rs` |

Their agreement is load-bearing: capture fidelity is supposed to be identical
whether a session went through Paper's cloud or through `tapesctl start`. Until
this corpus existed that agreement was a point-in-time claim by whoever last read
both files, and the decay had already cost a bug — **PCC-1126**, where the
client dropped every `content-encoding: zstd` request body (all of Codex/pi's
traffic) while the gateway route decoded the same bytes fine. Nothing was red.

`pkg/capture/contentencoding.go` is the **reference implementation**: where a
case and the prose disagree, the case records what the reference actually does,
and says so.

## Layout

```
fixtures/content-encoding/
  README.md          ← this file
  DIGEST             ← seal over cases/, recomputed by consumers
  cases/*.json       ← one case per file; consumers glob this directory
```

## How cases carry bytes

Encoded bodies are binary, where the envelope corpus is pure JSON. Two forms are
allowed, and which one a case uses is itself a statement about what the case
asserts:

* **`build`** — a *recipe*: a plaintext, the layers to apply to it, and an
  optional truncation. The consumer compresses locally.
* **`bytes_b64`** — the literal body, base64 (standard alphabet, padded).

Recipes are the default, and deliberately so. Compressed output is not stable
across implementations — Go's `compress/gzip` and Rust's `flate2` do not emit the
same bytes for the same input, and neither do `klauspost/compress/zstd` and
`libzstd`. A corpus that pinned compressed bytes would be asserting *compressor
identity*, which is not the policy and which no consumer can satisfy. It would
also be unreviewable: a diff of base64 blobs says nothing about what changed.
With a recipe, the reviewable artifact is the plaintext and the layer list, and
the assertion is the one that matters — that a gzip stream of X decodes to X.

`bytes_b64` is kept for the cases where the exact bytes **are** the assertion:
hand-built frames (`limit-zstd-window-*`), deliberately corrupt input, and the
empty body. Those are short enough to annotate byte by byte in `notes`, and a
recipe could not express them.

Bodies larger than a few hundred bytes are never committed in either form. The
cap cases decode to 32 MiB; a blob that size is not reviewable and its compressed
form is compressor-specific, so it is a recipe with a `count`.

## Case schema

Each `cases/*.json` file is one object.

| field | required | meaning |
| --- | --- | --- |
| `name` | yes | stable case id (matches the filename) |
| `category` | yes | `identity` \| `supported` \| `stacked` \| `salvage` \| `limit` \| `error` |
| `description` | yes | one line on what the case pins |
| `encoding` | yes | the `Content-Encoding` header value, verbatim and un-normalised. `null` means the header is **absent**; `""` means present and empty |
| `body` | yes | exactly one of `bytes_b64` or `build` (below) |
| `expect` | yes | the expected outcome (below) |
| `grounding` | yes | the policy rule the case pins, in behavioral terms |
| `contested` | no | a decision this corpus was written to force; see below |
| `notes` | no | anything a consumer needs to know |

### `body.build`

| field | required | meaning |
| --- | --- | --- |
| `plaintext` | yes | the logical content; exactly one of the plaintext forms below |
| `layers` | yes | codings to apply, **left-to-right**, in the same order the header lists them. `[]` means the body is the plaintext |
| `members` | no | encode the plaintext as this many independently-encoded, concatenated streams instead of one. Default `1` |
| `truncate` | no | exactly one of `{"drop_tail_bytes": n}`, `{"keep_head_bytes": n}` or `{"keep_head_ratio": [num, den]}`, applied to the **encoded** bytes after all layers |

`members` splits the **plaintext** into that many near-equal chunks (remainder
on the last), runs each through `layers` on its own, and concatenates the
results. Splitting the plaintext rather than the encoded stream is what makes
the recipe compressor-independent: the member boundary is at the same logical
offset whoever compressed it, so the case can still assert equality with the
*whole* plaintext. A split of the encoded bytes could not — it would be cutting
at an offset only one encoder's output has.

Both codings this corpus decodes allow it. A gzip stream is a series of members
(RFC 1952 §2.2) and zstd frames may be concatenated, and a streaming compressor
that flushes mid-body produces exactly this. Every chunk must be non-empty, so
`members` may not exceed the plaintext length.

`keep_head_ratio` is integer arithmetic: keep `len * num / den` bytes, truncating
the division. The encoded length differs per compressor, so a ratio-truncated
case can only assert a property of its output, never a length.

The three forms are not interchangeable, because each one holds a different
thing fixed across compressors:

* `drop_tail_bytes` fixes what is **missing** — a gzip stream short its 8-byte
  trailer is short its trailer whoever compressed it.
* `keep_head_ratio` fixes a **fraction**, so the surviving prefix is
  compressor-specific and the case can only assert a property of the output.
* `keep_head_bytes` fixes an **absolute prefix**, and is the only form that can
  express a cut point derived from the container format rather than from one
  encoder's output. Use it when the case asserts what is *not* yet possible at
  that offset — `salvage-refused-when-nothing-produced` keeps 9 bytes because
  that is the most a zstd frame can carry before any encoder's first byte of
  block content, so "no output is producible" holds for all of them rather than
  for the compressor that happened to build the fixture.

Plaintext forms:

| form | meaning |
| --- | --- |
| `{"utf8": "…"}` | the string's UTF-8 bytes |
| `{"repeat_utf8": {"text": "…", "count": n}}` | that string's bytes, `n` times |
| `{"repeat_byte": {"byte": b, "count": n}}` | the single byte value `b` (0–255), `n` times |

### `expect`

`outcome` is one of:

| outcome | meaning |
| --- | --- |
| `decoded` | clean decode; the implementation reports no truncation |
| `salvaged` | decode succeeded only by accepting partial output; the implementation **must** report the truncation |
| `error` | the body was refused |

For `decoded` and `salvaged`, `expect.decoded` holds exactly one of:

| form | meaning |
| --- | --- |
| `{"equals_plaintext": true}` | byte-equal to `body.build.plaintext` |
| `{"bytes_b64": "…"}` | byte-equal to these literal bytes |
| `{"nonempty_prefix_of_plaintext": true}` | non-empty, and a prefix of the plaintext — the only assertion a ratio-truncated salvage can make |

For `error`, `expect.error` holds:

| field | required | meaning |
| --- | --- | --- |
| `class` | yes | `unsupported` \| `oversize` \| `undecodable` |
| `message_contains` | no | substrings the error message must contain. **Asserted** — but used only where the message content is itself the contract |
| `detail` | no | free text naming the sub-reason. **Never asserted**; it is for the reader |

## The failure taxonomy

Three classes, and only three:

* **`unsupported`** — no decoder for the named coding (`br`, `deflate`). Nothing
  was read; the bytes are irrelevant.
* **`oversize`** — the decoded output crossed the per-layer cap. The bomb guard.
* **`undecodable`** — the stream could not be read to a keepable result: corrupt
  frame, bad header, a resource bound refused up front, or an early end that
  produced nothing.

The three exist because both implementations already distinguish them
structurally — Rust as `DecodeError::{Unsupported, TooLarge, Read}`, Go by
message — so pinning them costs nothing and catches a real class of drift: a
decoder that starts passing an unknown coding through instead of refusing it
still fails, because `unsupported` is not `decoded`.

`undecodable` is deliberately coarse. Corrupt-body and window-bound-refused are
separate things to a human and the same thing to both implementations, so the
corpus records the difference in `detail` (unasserted) rather than inventing a
distinction neither side carries. Splitting it is a change to both
implementations first and to this corpus second, never the reverse.

**This is not the drop-reason enum.** `extproc`'s `DropResponseDecode` collapses
all three into one metrics label. That is a separate decision one layer up, and
it stays as it is; this corpus does not touch it. It does make splitting it cheap
if anyone wants to — the classes are already named and already asserted.

## `contested`

A few cases carry a `contested` object. These are the ones the corpus was written
to force: places where the two implementations already diverged, or could, and
where the divergence was invisible because nothing compared them. The object is
prose — no consumer asserts it — and records `question`, `decision`, and whatever
else the decision needs (`rationale`, `open`, `caller_precondition`,
`suspected_bug`).

It is deliberately part of the case rather than of this README, so that the
reasoning travels with the bytes into every vendored copy, and so that the next
person to change the case finds the argument before they change it.

Three cases carry one today: `limit-zstd-window-over-cap`,
`contested-empty-body-under-gzip`, and `contested-empty-body-under-zstd`.

The last of those is the corpus working as intended, end to end. It was authored
recording the reference implementation's *observed* behaviour — an empty body
under zstd succeeded with zero bytes while the same body under gzip errored — and
flagged as a suspected bug rather than promoted to a rule, with an open question
asking a second implementation to report what it did. The Rust consumer answered
(it errors under both codings), which made the pair a genuine cross-language
divergence, and it was resolved where the case argued it should be: in the
reference implementation, not by bending either test. The case now pins the
agreed rule and keeps the argument (PCC-1134).

## Consumers

Each implementation table-tests over `cases/*.json`: build the body from
`body`, call its decoder with `encoding`, and assert the outcome.

In this repository:

* `pkg/capture/contentencoding_corpus_test.go` — the reference oracle over
  `DecodeContentEncoding`, and the authored-home gate (DIGEST + policy
  coverage).

Elsewhere, via a vendored copy:

* `tapesctl` — `crates/tapesctl/src/start/content_encoding.rs`. Not yet wired up;
  the corpus is the specification it should be wired to.

## `DIGEST`

Same sealing rule as `fixtures/envelope/DIGEST` and `fixtures/thread/DIGEST`: for
each `cases/*.json`, sorted by base name, feed
`"<basename>  <sha256-hex-of-file-bytes>\n"` into a SHA-256; the digest is
`"sha256:" + hex` of that hash. Consumers vendor `DIGEST` alongside `cases/` and
recompute it in their own suite, so a stale or hand-edited copy fails in the
consumer's own CI.

`DIGEST` makes a corpus change **visible**, not impossible. It is recomputed
from whatever cases exist, so deleting a case and re-sealing is a legal, green
two-line diff — which is why the coverage gate below names the case that pins
each rule rather than trusting the seal to notice.

## Coverage

The authored home also asserts, rule by rule, that the case pinning each policy
rule is still there and still pins it. That table is closed in both directions:
a rule whose case is gone fails, and a case no rule names fails. So a case
cannot be deleted, gutted in place, or renamed without the diff saying which
policy rule moved — and a new case cannot be added without recording what it is
for.

## Adding a case

1. Write `cases/<name>.json`; `name` must match the filename.
2. Prefer a `build` recipe. Use `bytes_b64` only when the exact bytes are the
   assertion, and annotate them in `notes`.
3. Fill in `grounding` — the policy rule the case pins, in behavioral terms.
4. If the case encodes a decision rather than a settled rule, add `contested`.
5. Add the rule the case pins to the coverage table in the authored-home gate,
   naming this case. The gate fails on any case no rule names.
6. Run `go test ./pkg/capture/`, copy the new digest it prints into `DIGEST`,
   and commit both.
7. Re-sync any vendored copy from the same commit, together with whatever
   implementation change the new case forces.
