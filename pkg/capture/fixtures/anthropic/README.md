# Anthropic capture fixtures

Inputs to the `pkg/capture` Anthropic reducer tests. These are hand-crafted
against the [Messages API streaming specification](https://docs.anthropic.com/en/api/messages-streaming)
— **they are not recorded production traffic**. The tests here validate
that the state machine honors the wire format as documented; they do not
validate that the wire format is what Anthropic actually sends.

Closing that gap means recording real SSE streams against a sandbox key and
diffing them against these files. The streaming-capture canary provides the
production signal; a deliberate fixture refresh against a sandbox is a
manual follow-up when someone has access.

## What each fixture is

| File | Stream? | Covers |
|---|---|---|
| `messages_oneshot.json` | no | Non-streaming text response; reducer's JSON path. |
| `messages_stream.sse` | yes | Multi-chunk streaming text. Token-by-token `content_block_delta`. |
| `messages_tool_use.sse` | yes | Streaming `tool_use` block with `input_json_delta` fragments reassembling into a parsed object. |
| `messages_thinking.sse` | yes | Streaming extended-thinking block: `thinking_delta` + `signature_delta`. |
| `messages_error_mid_stream.sse` | yes | `error` event mid-turn; reducer must return a partial `ChatResponse` without erroring. |
| `messages_truncated_stream.sse` | yes | Stream ends before `message_stop`; reducer must surface what it has. |
| `canonical_equivalence/turn_01_oneshot.json` | no | Paired fixture. Parsed via `anthropic.ParseResponse`. |
| `canonical_equivalence/turn_01_stream.sse` | yes | Same turn as above, streamed. Reducer output must byte-match the parsed oneshot after canonical encoding. |
| `canonical_equivalence/turn_02_tool_use_{oneshot,stream}.*` | paired | Same property for a `tool_use` turn. |
| `canonical_equivalence/turn_03_thinking_{oneshot,stream}.*` | paired | Same property for an extended-thinking turn. |

The `canonical_equivalence/` pairs drive the golden test: the reducer's
output for a streamed turn must be byte-identical to `ParseResponse`'s
output for the oneshot form of the same turn. That's the property that
keeps the content-addressed DAG dedup correct across capture shapes.

## Placeholder values

Every fixture uses stable placeholders in positions where Anthropic emits
non-deterministic values, so any diff against these files is meaningful:

- `msg_FIXTURE0000000000000000` — Anthropic message IDs
- `toolu_FIXTURE0000000000000` — Tool-use IDs
- `2026-01-01T00:00:00Z` — timestamps (where present)

When replacing with recorded traffic, apply the same placeholder substitutions
before committing.
