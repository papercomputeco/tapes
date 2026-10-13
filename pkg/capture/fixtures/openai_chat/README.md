# OpenAI-compatible Chat Completions capture fixtures

Inputs to the `pkg/capture` Chat Completions reducer tests. These are
hand-crafted against DeepSeek's documented [reasoning model](https://api-docs.deepseek.com/guides/reasoning_model)
wire shape and vLLM's OpenAI-compatible server output — **they are not
recorded production traffic**. Older vLLM and SGLang reasoning parsers emit
DeepSeek's `reasoning_content` field; newer vLLM and OpenRouter emit
`reasoning`, and servers mid-migration send both with the same text.

## What each fixture is

| File | Stream? | Covers |
|---|---|---|
| `deepseek_oneshot.json` | no | `message.reasoning_content` alongside answer text, a tool call, and DeepSeek's extended usage (`completion_tokens_details.reasoning_tokens`, `prompt_cache_*_tokens`). |
| `deepseek_stream.sse` | yes | The same turn streamed: `delta.reasoning_content` fragments (with `content: null`), then text deltas (with `reasoning_content: null`), a fragmented tool call, a `: keep-alive` comment, and usage on the finishing chunk. |
| `vllm_reasoning_oneshot.json` | no | vLLM `message.reasoning` alongside answer text and a tool call, with vLLM's explicit `null` optional fields. |
| `vllm_reasoning_stream.sse` | yes | The same turn streamed with `delta.reasoning`, and usage on a trailing empty-choices chunk. |
| `vllm_both_fields_stream.sse` | yes | The same stream with every reasoning delta carrying identical `reasoning` and `reasoning_content`; must reduce without doubling the trace. |

## Placeholder values

- `00000000-0000-4000-8000-00000000000N`, `chatcmpl-FIXTURE…N` — completion IDs
- `call_00_FIXTURE00000000000000`, `chatcmpl-tool-FIXTURE…1` — tool-call IDs
- `fp_FIXTURE000_prod0000` — system fingerprint
