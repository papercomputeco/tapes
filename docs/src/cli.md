# CLI reference

`tapes` is the server. It runs the services, owns the database, and carries the operator tooling around them. Capturing a session and reading one back are client concerns and live in [`tapesctl`](https://github.com/papercomputeco/tapesctl) — see [The client CLI](#the-client-cli) below.

Run `tapes <command> --help` for the complete, version-matched flag list.

| Command | Use |
| --- | --- |
| `tapes init [--preset ...]` | Create a local `.tapes/` configuration directory. |
| `tapes local [up|status|down]` | Manage local PostgreSQL and Ollama dependencies. |
| `tapes serve` | Run proxy, read API, private ingest API, derive worker, and optional embed worker together. |
| `tapes status` | Show active config, provider/upstream, API reachability, and capture summary. |
| `tapes auth` | Store OpenAI or Anthropic credentials in `.tapes/credentials.toml`. |
| `tapes config get|set|list` | Manage persistent scalar settings. |
| `tapes raw equivalence` | Prove stored capture bytes re-reduce to the stored reduction. See [Proving the capture ratchet](#proving-the-capture-ratchet). |
| `tapes version` | Print version information. |

## Running services

The common local command is:

```bash
tapes serve
```

It accepts provider/upstream, PostgreSQL, listening, embedding, and project flags. Useful examples:

```bash
tapes serve --provider anthropic --upstream https://api.anthropic.com
tapes serve --api-web-ui
tapes serve --embed-spans=false
```

For split deployments, service subcommands are available:

```bash
tapes serve proxy
tapes serve api
tapes serve derive-worker
tapes serve embed-worker
tapes serve ingest
```

The last three are operator-oriented: the derive worker projects dirty sessions, the independent embed worker populates search vectors, and the private ingest sidecar receives completed turns from a trusted gateway. See [HTTP APIs](./apis.md) before exposing any endpoint.

## The client CLI

Launching an agent under capture, listing sessions, exporting one, and seeding demo data are all client operations against a running server. They live in `tapesctl`:

```bash
curl -sSfL https://download.tapes.dev/tapesctl/install | bash
```

```bash
tapesctl start claude --tapes-url http://localhost:8081
tapesctl sessions list --tapes-url http://localhost:8081
tapesctl export <session-id> --detail spans -o session.jsonl
tapesctl seed --tapes-url http://localhost:8081
tapesctl skill sync <name> --claude
```

Every `tapesctl` command takes `--tapes-url`, falling back to `TAPES_URL`. Arguments after `--` go directly to the agent. See [Agent integrations](./integrations.md) and the [`tapesctl` README](https://github.com/papercomputeco/tapesctl) for the full surface.

## Proving the capture ratchet

`raw_turns` keeps two views of the same upstream response: `raw_response`, the bytes exactly as they arrived, and `response`, the reduced turn a capture adapter produced from them. The reduction is lossy, and while a second reducer runs inside the capture adapter, two capture paths can reduce the same traffic differently.

The fix is to have exactly one reducer, server-side. Getting there is a deliberate three-step ratchet, configured on the capture adapter:

| Mode | What the adapter sends | Stored fidelity |
| --- | --- | --- |
| `off` | its reduction only | `reduced` |
| `dual` | its reduction **and** the verbatim bytes | `raw` |
| `raw` | verbatim bytes only; tapes reduces | `raw` |

`dual` exists to make the middle step provable. It changes nothing an operator sees — ingest keeps the adapter's reduction — while putting the bytes in the database next to it. That makes the question "would `raw` have produced this same row?" answerable offline, over real traffic:

```bash
tapes raw equivalence --since 24h --limit 5000
```

For each wire turn in the window that has both halves, the command decodes the stored bytes, re-reduces them through the same server-side path `raw` would run, and compares the result against the stored reduction. It exits non-zero if anything diverged or failed to reduce, so it can gate the step in CI.

Run it inside the cluster against a tenant's database:

```bash
kubectl exec -n <tenant-ns> deploy/tapes-api -- \
  tapes raw equivalence --since 24h --limit 5000
```

or locally against a forwarded database, with `--json` for machine consumption:

```bash
tapes raw equivalence \
  --postgres "postgres://user:pass@127.0.0.1:15432/tapes" \
  --since 24h --json
```

The comparison is read-only, and it never prints response content — a difference is reported as a JSON path plus the shape of what differs, because these are real prompts.

### Reading the result

Every examined turn lands in exactly one class. `equivalent` is the one that supports a ratchet step. `divergent`, `undecodable`, `unreducible` and `no_reducer` all block it: the last three are worse than a divergence, because under `raw` those rows would carry no reduction at all. The `skipped_*` classes describe turns the flip does not affect — no bytes were captured, the bytes were withheld or dropped over a limit, or the turn was already captured raw-only.

Two fields are excluded from the comparison, and the report always prints them:

- **`created_at`** — reducers stamp it at reduction time, so two reductions of identical bytes taken at different instants differ by construction.
- **`usage.total_duration_ns`** — the wall clock from request to fully-assembled response. Only the party that watched the stream can measure it; a reduction of stored bytes cannot.

Everything else is compared strictly, so a third difference is reported rather than absorbed.

Because both excluded fields are ones `raw` restores from the capture adapter's `meta` block rather than from the bytes, the report also counts which stamps would actually have been available. **A window can be perfectly equivalent and still lose data on the flip**: if `usage.total_duration_ns` shows `fallback`, those turns carry no usable `meta.elapsed_seconds`, and under `raw` their duration — and the derived span's `duration_ns` — would land empty. Check that line before ratcheting, not just the verdict.

## Commands not intended as everyday workflow

`backfill` is for replaying existing capture artifacts into a deployment. `dev` contains developer maintenance utilities. Consult their `--help` only when operating those workflows.

Tapes no longer provides `chat` or `checkout` commands. It captures external agents; it does not host a chat client or expose history branching.

The `start`, `export`, `seed`, `sessions`, and `skill sync` commands have moved to `tapesctl`; `tapes` keeps the server and the operator tooling.
