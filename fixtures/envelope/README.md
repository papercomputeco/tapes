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
  cases/*.json       ← one case per file; consumers glob this directory
```

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
| `x-tapes-cwd`                          | `cwd`                       | producer percent-encodes UTF-8; **reader stores it verbatim (does NOT decode)** |
| `x-tapes-session-name`                 | `name`                      | producer percent-encodes UTF-8 (capped 256 raw bytes); **reader percent-decodes** |
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
idealized inverse of the producer. Two spots are deliberately asymmetric and easy to get
wrong:

- **`cwd` is stored verbatim** (still percent-encoded), while **`name` is percent-decoded**.
  So the non-ASCII / control-byte `cwd` cases are `direction: encode` with a lossy
  round-trip: `encode_from` holds the logical path, but decoding the header yields the
  encoded string. (That the reader decodes `name` but not `cwd` is a standing asymmetry
  worth revisiting in the reader, not in these fixtures.)
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
