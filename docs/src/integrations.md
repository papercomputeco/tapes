# Agent integrations

The proxy is transparent: point a supported provider client at it and Tapes forwards the request while recording the completed turn. For supported coding agents, launching through [`tapesctl`](https://github.com/papercomputeco/tapesctl) is safer than editing configuration by hand — it wires the agent to a just-in-time capture proxy that dies with the process.

Start the server and its local dependencies before these examples:

```bash
tapes local up
tapes serve
```

Then install the client:

```bash
curl -sSfL https://download.tapes.dev/tapesctl/install | bash
```

## Claude Code

```bash
tapes auth anthropic   # optional when ANTHROPIC_API_KEY is already set
tapesctl start claude --tapes-url http://localhost:8081
```

`tapesctl` starts a loopback capture proxy, sets Claude Code's `ANTHROPIC_BASE_URL` to it, launches `claude`, and ships the captured turns to the server. Pass Claude flags after `--`:

```bash
tapesctl start claude -- --worktree
```

For a manually managed, fixed-port proxy:

```bash
tapes serve --provider anthropic --upstream https://api.anthropic.com
ANTHROPIC_BASE_URL=http://localhost:8080 claude
```

## OpenCode

```bash
tapesctl start opencode --tapes-url http://localhost:8081
```

Switching provider or model inside OpenCode is not captured by the configured route; start a separate capture session instead.

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
tapesctl sessions list --tapes-url http://localhost:8081
```

## OpenClaw

Tapes has no OpenClaw-specific launcher or configuration code. OpenClaw can only be treated as a generic supported-provider client: if the installed OpenClaw version offers a provider base-URL setting, point that setting at `http://localhost:8080` while Tapes runs with the matching provider/upstream. Consult the OpenClaw documentation for the setting's current name and shape rather than copying old `openclaw.json` examples.

This limited integration claim is intentional: device pairing, channels, and OpenClaw deployment behavior are outside Tapes and are not configured by this repository.

## Verify and stop

The read API health endpoint is separate from the proxy:

```bash
curl http://localhost:8081/ping
tapesctl sessions list --tapes-url http://localhost:8081
```

Stop the foreground `tapes serve` process with `Ctrl-C`. `tapes local down` removes bootstrap containers but keeps PostgreSQL data unless `--wipe` is supplied.
