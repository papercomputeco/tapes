# Thread fixtures — the harness-native sub-thread header ↔ `thread_id` contract

L0-layer fixtures, sibling to `fixtures/envelope/`: tiny, synthetic,
language-neutral JSON cases that pin how a request's **harness-native** thread
headers resolve to the captured turn's `meta.thread_id`.

Unlike the envelope corpus, none of these headers are `X-Tapes-*` framing.
They are the harness's own, addressed to the model provider — capture only
*observes* them and never strips them. There is no producer side to this
contract, only independent readers, which is exactly what makes it easy to
drift: the same resolution is implemented today in **four** places across two
languages.

## The canonical vocabulary lives in tapes-harnesses

The header spellings and per-harness resolution rules are harness knowledge,
and their canonical home is the `tapes-harnesses` crate:

* `src/envelope.rs` — `CLAUDE_THREAD_ID_HEADERS`, `CODEX_THREAD_ID_HEADER`,
  `CODEX_SESSION_ID_HEADER`, and the per-harness rule table
  `HARNESS_THREAD_ID_RULES` consumed by `envelope::thread_id`.
* `src/attribution/codex_app/mod.rs` — the lifecycle-boundary spelling of the
  same identities (`session_id` = the root session, `agent_id` = the child
  thread — the lifecycle counterpart of a sub-thread request's `thread-id`
  header).

This corpus is the executable form of that correspondence for readers that
cannot link the crate. A rename or rule change on either side must land here
too, so the other side's consumer fails on its next sync instead of drifting
silently.

## The rules the cases pin

* **Claude Code — presence list.** `x-claude-code-agent-id` is stamped only on
  calls made from a subagent context, so presence of a non-empty value is the
  whole signal; the value maps verbatim.
* **Codex — divergent pair.** Codex stamps `thread-id` and `session-id` on
  *every* call. An **equal** pair is a root turn (the root guard: a non-empty
  thread id on a root turn misroutes the root spine in derive and silently
  degrades the session's derived status). A **divergent** pair resolves to the
  `thread-id` value. A **lone** member of the pair is not a recognised Codex
  shape and resolves to nothing.
* **Precedence.** The Claude list is tried before the Codex pair. Only
  observable when one request carries evidence for both harnesses at once.

## Case schema

Each `cases/*.json` file is one object:

| field         | required | meaning |
|---------------|----------|---------|
| `name`        | yes | stable case id (matches the filename) |
| `harness`     | yes | `claude` \| `codex` — the harness whose rule the case pins |
| `description` | yes | one line on what the case shows |
| `headers`     | yes | the on-wire request header set: lower-cased header name → raw value, as an HTTP/2 intermediary would carry it |
| `thread_id`   | yes | the resolved sub-thread id; `""` for a main-thread call |
| `grounding`   | yes | the contract rule the case pins, in behavioral terms |
| `notes`       | no  | anything a consumer needs to know |

Identity values are synthetic placeholders (repeated-digit UUIDs, obviously
fake agent ids) — never real session or agent ids.

## Consumers

Every reader table-tests over `cases/*.json`: build the header set in its own
transport shape, resolve, and assert the result equals `thread_id`.

In this repository (three independent readers of the same traffic):

* `extproc/headers/thread_corpus_test.go` — extproc's `ThreadID` over ext_proc
  `HttpHeaders`. Also the authored-home gate: it recomputes `DIGEST` and
  checks the corpus still covers the rules above.
* `proxy/header/thread_corpus_test.go` — the live proxy's `ThreadID` over a
  `fiber.Ctx`.
* `pkg/backfill/wiretrace_threadid_corpus_test.go` — the recording backfill's
  `threadIDFromHeaders`.

In `tapes-harnesses`: `envelope::thread_id` / `HARNESS_THREAD_ID_RULES`. Vendor
this directory there next to `vendor/tapes-envelope-fixtures/` (same sync
mechanism, same `DIGEST` seal) so the crate's oracle and these readers test
against identical bytes.

## `DIGEST`

Same sealing rule as `fixtures/envelope/DIGEST`: for each `cases/*.json`,
sorted by base name, feed `"<basename>  <sha256-hex-of-file-bytes>\n"` into a
SHA-256; the digest is the hex of that hash. Consumers vendor `DIGEST`
alongside `cases/` and recompute it in their own test suite, so a stale or
hand-edited copy fails in the consumer's own CI.

## Adding a case

1. Write `cases/<name>.json`; `name` must match the filename.
2. Use synthetic identities only.
3. Fill in `grounding` — the rule the case pins, in behavioral terms.
4. Run `go test ./extproc/headers/`, copy the new digest it prints into
   `DIGEST`, and commit both.
5. Re-sync any vendored copy from the same commit, together with whatever
   reader change the new case forces.
