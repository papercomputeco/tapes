# Agent integrations

The proxy is transparent: point a supported provider client at it and Tapes forwards the request while recording the completed turn. For supported coding agents, `tapes start` is safer than editing configuration by hand.

Start local dependencies before these examples:

```bash
tapes local up
```

## Claude Code

```bash
tapes auth anthropic   # optional when ANTHROPIC_API_KEY is already set
tapes start claude
```

Tapes starts its agent runtime on automatically selected loopback ports, sets Claude Code's `ANTHROPIC_BASE_URL` to its agent-scoped proxy route, launches `claude`, and tags captured traffic as Claude. Pass Claude flags after `--`:

```bash
tapes start claude -- --worktree
```

For a manually managed, fixed-port proxy:

```bash
tapes serve --provider anthropic --upstream https://api.anthropic.com
ANTHROPIC_BASE_URL=http://localhost:8080 claude
```

## OpenCode

```bash
tapes start opencode
```

On first use, choose Anthropic, OpenAI, or Ollama and a model. The choice is saved as `opencode.provider` and `opencode.model`. You can be explicit:

```bash
tapes start opencode --provider anthropic --model claude-sonnet-4-5
tapes start opencode --provider openai --model gpt-5.2-codex
tapes start opencode --provider ollama --model glm-4.7-flash
```

The launcher merges the user's existing OpenCode configuration into a temporary config, installs provider-specific proxy URLs, injects stored credentials, and restores normal behavior by deleting that temporary config at exit. It also selects the model on the command line. Switching provider or model inside OpenCode is not captured by the configured route; start a separate Tapes session instead.

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

For another Anthropic-, OpenAI-, or Ollama-compatible application, configure its base URL as `http://localhost:8080` and run `tapes serve` with the matching `--provider` and `--upstream`. Preserve the path convention expected by the client and provider. Verify capture with:

```bash
tapes status
tapes sessions
```

## OpenClaw

Tapes has no OpenClaw-specific launcher or configuration code. OpenClaw can only be treated as a generic supported-provider client: if the installed OpenClaw version offers a provider base-URL setting, point that setting at `http://localhost:8080` while Tapes runs with the matching provider/upstream. Consult the OpenClaw documentation for the setting's current name and shape rather than copying old `openclaw.json` examples.

This limited integration claim is intentional: device pairing, channels, and OpenClaw deployment behavior are outside Tapes and are not configured by this repository.

## Verify and stop

The read API health endpoint is separate from the proxy:

```bash
curl http://localhost:8081/ping
tapes sessions
```

Stop the foreground `tapes serve` process with `Ctrl-C`. `tapes local down` removes bootstrap containers but keeps PostgreSQL data unless `--wipe` is supplied.
