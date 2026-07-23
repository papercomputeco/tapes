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
| `x-tapes-cwd`                          | `cwd`                       | **percent-encoded UTF-8** |
| `x-tapes-session-name`                 | `name`                      | **percent-encoded UTF-8**, capped 256 raw bytes |
| `x-tapes-parent-harness-session-id`    | `parent_harness_session_id` | verbatim; empty is invalid (omit instead) |
| `x-tapes-harness-metadata`             | `harness_metadata` (object) | **base64url(no-pad) of the JSON object**, raw JSON ≤ 4 KiB |
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

## Consuming

Both sides table-test over `cases/*.json`:

- **Parser**: for each case, build the header set, parse, and assert the parsed envelope
  equals `envelope` (or that validation yields `error`). Skip `encode`-only assertions.
- **Producer**: for each `roundtrip`/`encode` case, encode `encode_from`/`envelope` and
  assert the emitted header set equals `headers`.

Vendor this directory into each side (a small sync script keeps one copy authoritative)
so producer and parser test against identical bytes.
