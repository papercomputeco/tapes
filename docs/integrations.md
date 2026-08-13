---
title: Agent integrations
description: The harnesses Tapes captures, which lane each one uses, and how to point a generic provider client at the transparent proxy.
sidebar:
  order: 4
---

Tapes captures an agent in one of two ways. The client launches the agent under
a just-in-time capture proxy that dies with the process, or — for an agent that
launches itself — the client binds the address the agent was installed against
and captures whatever runs in that window.

| Harness | Lane | Plugin needed first |
| --- | --- | --- |
| `claude` | `tapesctl start claude` | none |
| `codex` | `tapesctl start codex` | none |
| `pi` | `tapesctl start pi` | `tapesctl plugin install pi` |
| `codex-app` | `tapesctl capture codex-app` | `tapesctl plugin install codex-app` |

Capture commands address the **private ingest API**, `:8082` by default — not
the read API on `:8081`. A capture pointed at the read port reports success and
stores nothing.

Start the server and its local dependencies before these examples:

```bash
tapes local up
tapes serve
```

Then install the client:

```bash
curl -sSfL https://download.tapes.dev/tapesctl/install | bash
```

The agent brings its own provider credentials. `tapes auth` stores credentials
for the server-side features that call a provider themselves — span embedding
and skill generation — and is not part of capturing an agent.

## Claude Code

```bash
tapesctl start claude --tapes-url http://localhost:8082
```

`tapesctl` starts a loopback capture proxy, sets Claude Code's `ANTHROPIC_BASE_URL` to it, launches `claude`, and ships the captured turns to the server. Pass Claude flags after `--`:

```bash
tapesctl start claude --tapes-url http://localhost:8082 -- --worktree
```

Claude sessions also produce transcripts on disk, which carry the subagent
structure the wire traffic alone cannot show. `start` tails them live. For a
session no capture was running for, sweep them afterwards:

```bash
tapesctl sync --tapes-url http://localhost:8082
```

`sync` sweeps the last seven days by default; `--since-days 0` sweeps
everything. Re-pushing is safe — the server deduplicates.

For a manually managed, fixed-port proxy instead of the just-in-time one:

```bash
tapes serve --provider anthropic --upstream https://api.anthropic.com
ANTHROPIC_BASE_URL=http://localhost:8080 claude
```

## Codex

The terminal CLI is launched like Claude:

```bash
tapesctl start codex --tapes-url http://localhost:8082
```

The ChatGPT desktop app launches itself, so it is captured through lifecycle
hooks instead. Install the plugin once, then run a capture window and start a
session in the app:

```bash
tapesctl plugin install codex-app
tapesctl capture codex-app --tapes-url http://localhost:8082
```

`plugin install codex-app` writes the handoff file and points the app's Codex
configuration at the capture address; `capture` reads that handoff and binds it.
Running `capture` first fails and tells you to install. Unlike `start`, `capture`
prints no turn counts when it stops — it reports the number of sessions it saw.

`plugin uninstall codex-app` removes the provider entry and the handoff, but the
plugin stays registered with Codex; the command prints the `codex plugin remove`
line that finishes the job.

## pi

pi is captured by an installed extension, so the install is a prerequisite
rather than a convenience:

```bash
tapesctl plugin install pi
tapesctl start pi --tapes-url http://localhost:8082
```

`start pi` refuses to run when the extension is absent, before anything is bound
or launched. pi redirects several providers to one endpoint, so it is the one
harness that takes `--schema`:

```bash
tapesctl start pi --tapes-url http://localhost:8082 --schema openai
```

`--schema` on `claude` or `codex` is an error rather than a silent no-op: each
speaks exactly one schema, taken from the harness.

## Ollama and generic clients

With default configuration, Tapes forwards Ollama-compatible traffic to `http://localhost:11434`:

```bash
tapes serve
curl http://localhost:8080/api/chat \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen3-coder:30b","messages":[{"role":"user","content":"hello"}],"stream":false}'
```

Pull a chat model separately; `tapes local up` pulls the embedding model, not every completion model:

```bash
ollama pull qwen3-coder:30b
```

For another Anthropic-, OpenAI-, or Ollama-compatible application, configure its base URL as `http://localhost:8080` and run `tapes serve` with the matching `--provider` and `--upstream`. Preserve the path convention expected by the client and provider.

## Verify and stop

The read API health endpoint is separate from the proxy and from ingest:

```bash
curl http://localhost:8081/ping
tapes status
tapesctl sessions list --tapes-url http://localhost:8081
```

A captured session appears in that list. `start` prints the harness's own
session id on exit, which is a different id from the one `tapesctl sessions get`
takes — find the session in the list rather than pasting the printed id.

Stop the foreground `tapes serve` process with `Ctrl-C`. `tapes local down` removes bootstrap containers but keeps PostgreSQL data unless `--wipe` is supplied.
