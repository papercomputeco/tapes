# Envelope fixtures — the `X-Tapes-*` header ↔ envelope contract

This is the **L0** layer of the fixture pyramid: tiny, synthetic, language-neutral
JSON cases that pin the session-envelope contract carried on the `X-Tapes-*` (and
the server-trusted `X-Paper-Auth-*`) HTTP headers.

The contract has two sides that live apart and are easy to drift:

- a **producer** that turns a session's identity into the on-wire header set — applying
  percent-encoding, the session-name byte cap, base64url metadata, and the header byte
  budgets; and
- a **parser** that reads that header set back into a session envelope.

They may be written in different languages and shipped in different services. These
fixtures make their agreement executable: one set of cases both sides test against.

## Layout

```
fixtures/envelope/
  README.md          ← this file
  DIGEST             ← seals the case set; vendored copies recompute and compare
  cases/*.json       ← one case per file; consumers glob this directory
```

**Identity values are synthetic placeholders**, not real credentials: `org_id`s are
placeholder UUIDs (`00000000-…`), `auth_subject`s are obviously-fake strings
(`user_synthetic_fixture_subject`), and session ids are repeated-digit UUIDs. No real
WorkOS user/org ids or customer session ids appear anywhere in this corpus.

## Case schema

Each `cases/*.json` file is one object:

| field         | required | meaning |
|---------------|----------|---------|
| `name`        | yes | stable case id (matches the filename) |
| `category`    | yes | `valid` \| `percent-encoding` \| `budget` \| `unknown` \| `error` |
| `harness`     | yes | `claude` \| `codex` \| `pi` \| `unknown` |
| `description` | yes | one line on what the case pins |
| `direction`   | yes | `roundtrip` \| `decode` \| `encode` — which conversions this case is authoritative for (see below) |
| `headers`     | yes | the on-wire header set: lower-cased header name → raw ASCII value, exactly as an HTTP/2 intermediary would carry it |
| `envelope`    | yes | the expected parsed envelope (`decode(headers)`) — see the field mapping below |
| `encode_from` | no  | present only for **lossy** cases: the logical envelope a producer would `encode` to produce `headers`. When absent, `encode_from == envelope` (the case round-trips). |
| `error`       | no  | for `error` cases: `{field, rule, disposition}` where `disposition` is `reject-400` (the ingest boundary rejects it) or `drop-field` (the parser drops just that field, non-fatally) |
| `grounding`   | yes | the contract rule the case pins, in behavioral terms |
| `notes`       | no  | anything a consumer needs to know |

### Directions

- **`roundtrip`** — `encode(envelope) == headers` **and** `decode(headers) == envelope`.
  The default; most `valid` / `percent-encoding` cases.
- **`encode`** — a lossy producer transform (truncation, oversize-drop). `encode(encode_from)
  == headers`, and `decode(headers) == envelope` where `envelope` reflects the loss.
- **`decode`** — parser-only cases a well-behaved producer would never emit (malformed
  input, missing/empty required headers). Only `decode(headers) == envelope` (or the
  `error`) is asserted.

## Header ↔ envelope field mapping

| header                                 | envelope field              | transform on the wire |
|----------------------------------------|-----------------------------|-----------------------|
| `x-tapes-harness-id`                   | `harness_id`                | verbatim; missing/empty → `"unknown"` |
| `x-tapes-harness-session-id`           | `harness_session_id`        | verbatim |
| `x-tapes-harness-version`              | `harness_version`           | verbatim |
| `x-tapes-cwd`                          | `cwd`                       | producer percent-encodes UTF-8; **reader percent-decodes**, then applies the control-byte guard |
| `x-tapes-session-name`                 | `name`                      | producer percent-encodes UTF-8 (capped 256 raw bytes); **reader percent-decodes**, then applies the control-byte guard |
| `x-tapes-parent-harness-session-id`    | `parent_harness_session_id` | verbatim; **an empty header is dropped by the reader** (omit it) |
| `x-tapes-harness-metadata`             | `harness_metadata`          | **base64url(no-pad) of a JSON value**, raw ≤ 4 KiB; reader retains any valid JSON, validation requires an **object** |
| `x-paper-auth-org-id`                  | `org_id`                    | server-trusted (set from a validated JWT claim); UUID or empty |
| `x-paper-auth-subject`                 | `auth_subject`              | server-trusted (set from a validated JWT claim) |

## Encoding rules (how the `headers` values were derived)

The header values are byte-exact and auditable — reproduce them from these rules:

- **Percent-encoding set**: C0 controls `0x00–0x1F`, `0x7F` (DEL), space, `%`, `"`, `\` —
  plus every non-ASCII byte (`≥ 0x80`) is always encoded as `%XX` per UTF-8 byte.
  Everything else passes through verbatim. Applied to `cwd` and `session-name`. Examples:
  space→`%20`, `"`→`%22`, `é`→`%C3%A9`, `松`→`%E6%9D%BE`, newline→`%0A`, `ก`→`%E0%B8%81`.
- **Session-name cap**: 256 raw bytes, truncated at a UTF-8 codepoint boundary *before*
  encoding (`session-name-truncated-utf8`: 100×`ก` = 300 B → 85 codepoints / 255 B).
- **Metadata**: `base64url(no-pad)` of the compact JSON object; dropped whole if the
  raw JSON exceeds 4 KiB (`metadata-oversize-dropped`). Compare metadata as a decoded
  **object**, not by base64 string equality — JSON key ordering is not part of the
  contract.
- **Total budget**: 8 KiB across all `X-Tapes-*` headers; the metadata header is
  dropped first when exceeded.

## Reader behavior the cases pin

The `envelope` in each case is exactly what the reader produces from `headers` — not an
idealized inverse of the producer. Every parser of this corpus must agree on all of it;
these are the spots that are easy to get wrong:

- **Both `cwd` and `name` are percent-decoded**, with the same RFC 3986 path-segment
  rules (`PathUnescape`, not `QueryUnescape` — the latter would mistranslate a literal
  `+` into a space). The stored value is the *logical* value; percent-encoding is
  transport framing that dies at the parser, so `/Users/松本/code` is stored and
  displayed as itself. A malformed encoding is non-fatal: the raw header value is kept
  so the row still records something recognisable.
- **A decoded value carrying control bytes is refused, not stored.** After decoding, if
  `cwd` or `name` contains any C0 control (`< 0x20`) or DEL (`0x7F`), the parser logs
  and stores the **empty** string — it never persists the raw bytes. That is why
  `cwd-control-bytes-escaped` is `direction: encode`: the producer can encode such a
  path, but no reader will store it, so the case stays lossy and keeps its
  `encode_from`. Escaping stops a control byte from forging a header *on the wire*; the
  guard stops it from reaching *storage*. The injection defense therefore lives in
  validation rather than in representation, which is what lets the stored field stay a
  logical path instead of an encoded one that every consumer would have to decode.
- **Non-object metadata is retained, then rejected.** The reader accepts any valid-JSON
  metadata (arrays included); object-ness is enforced by envelope validation, so
  `error-metadata-not-object` is `reject-400`, not a silent drop. Metadata that isn't
  valid base64url *is* dropped (`error-metadata-invalid-base64`). An empty parent header
  is dropped by the reader (`error-parent-empty` → `drop-field`); the reject-empty rule
  only guards an explicit empty in a JSON ingest body.

## Consuming

Both sides table-test over `cases/*.json`:

- **Parser**: for each case, build the header set, parse, and assert the parsed envelope
  equals `envelope` (or that validation yields `error`). Skip `encode`-only assertions.
- **Producer**: for each `roundtrip`/`encode` case, encode `encode_from`/`envelope` and
  assert the emitted header set equals `headers`.

The tapes reader already does this: `pkg/backfill/envelope_fixtures_test.go` runs every
case through `sessionEnvelopeFromHeaders` + `Validate` and asserts the declared `envelope`
/ `error`. Keep it green — it is what stops these fixtures from silently drifting from the
parser. Vendor this directory into other consumers (a small sync script keeps one copy
authoritative) so every side tests against identical bytes.

## `DIGEST` — what makes "identical bytes" checkable

Conformance to a vendored copy only proves parity if every copy is the same corpus.
Nothing about vendoring guarantees that: a hand-edit to one copy leaves two
implementations testing against different bytes while both stay green.

`DIGEST` closes that. It is a single line, `sha256:<hex>`, over the case set:

> for each `cases/*.json`, sorted by base name, feed `"<basename>  <sha256-hex-of-file-bytes>\n"`
> into a SHA-256; the digest is the hex of that hash.

Deliberately trivial, so a consumer in any language can reimplement it from that
sentence without a canonical-JSON library. It hashes **raw bytes**, not parsed JSON,
because the sync script copies bytes — a reformat is drift too. It covers **names as
well as contents**, so an addition, a deletion, and a rename are all caught rather than
only an edit to a file that already existed.

Consumers vendor `DIGEST` alongside `cases/` and recompute it in their own test suite.
A stale or locally-edited copy then fails in the consumer's own CI, with no cross-repo
checkout needed. `pkg/backfill/envelope_corpus_test.go` is this repo's copy of that
check, and it prints the new value when the corpus legitimately changes.

## Adding a case

1. Write `cases/<name>.json`. `name` must match the filename — consumers key skips and
   deviation entries off it. Use the synthetic identities above; never a real org,
   subject, or session id.
2. Pick `direction` honestly. `encode` is the lossy direction and **must** carry
   `encode_from`; `roundtrip` and `decode` must not (it would duplicate `envelope`).
3. Derive `headers` from the encoding rules above by hand, so the bytes stay auditable.
4. Fill in `grounding` — the rule the case pins, in behavioral terms. A case that cannot
   say which rule it pins cannot be reviewed, and the gate rejects it.
5. Run the corpus gate, copy the new digest it prints into `DIGEST`, and commit both.
6. Re-sync every vendored copy from the same commit, and land any parser change the new
   case forces in the same PR — the corpus is the contract, so both halves travel
   together.

`pkg/backfill/envelope_corpus_test.go` also asserts that the corpus still **covers** the
rules this README claims, stated as properties rather than case names. Deleting the only
case that exercises a rule is otherwise invisible: every remaining case passes and the
contract quietly shrinks. If that gate says a rule is uncovered, add a case rather than
relaxing the rule.
