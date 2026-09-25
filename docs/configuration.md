---
title: Configuration
description: How the server resolves its .tapes/ directory and settings, and how the client resolves its one server URL.
sidebar:
  order: 3
---

The server and the client keep separate configuration with different resolution
rules. Read the section for the binary you are configuring; they do not share a
precedence chain.

## The server's configuration

Tapes stores configuration and credentials in a `.tapes/` directory. Resolution order is:

1. `--config-dir <directory>`;
2. `.tapes/` in the current directory;
3. `~/.tapes/`;
4. built-in defaults if none exists.

Create a project-local directory with defaults or a provider preset:

```bash
tapes init
tapes init --preset anthropic
# presets: openai, anthropic, ollama
```

A project-local `.tapes/` takes precedence over the home directory. This is useful for per-project provider and database settings, but is also the first place to check when an expected global setting appears ignored.

### Precedence

For commands that bind a setting, precedence is:

1. CLI flag;
2. `TAPES_...` environment variable;
3. `config.toml` value;
4. built-in default.

Dots become underscores in environment names, for example `TAPES_PROXY_LISTEN` and `TAPES_STORAGE_POSTGRES_DSN`.

### Manage settings

```bash
tapes config list
tapes config get proxy.provider
tapes config set proxy.provider anthropic
tapes config set proxy.upstream https://api.anthropic.com
```

Useful supported keys include:

| Key | Purpose | Default |
| --- | --- | --- |
| `storage.postgres_dsn` | Capture and derived PostgreSQL database | unset |
| `proxy.provider` | `anthropic`, `openai`, or `ollama` | `ollama` |
| `proxy.upstream` | Upstream provider base URL | `http://localhost:11434` |
| `proxy.listen` | Proxy listen address | `:8080` |
| `proxy.project` | Session project tag | auto-detected from Git when unset |
| `api.listen` | Read API listen address | `:8081` |
| `api.web_ui` | Minimal browser UI at `/` | `false` |
| `api.read_deadline` | Deadline on every read API request; `0` disables (see [Read guards](#read-guards)) | `20s` |
| `api.payload_concurrency` | Concurrent full-payload reads per replica; `0` disables (see [Read guards](#read-guards)) | `4` |
| `ingest.listen` | Private ingest listen address | `:8082` |
| `client.proxy_target` | Proxy URL used by clients | `http://localhost:8080` |
| `client.api_target` | API URL used by clients | `http://localhost:8081` |
| `vector_store.target` | pgvector PostgreSQL DSN | primary PostgreSQL DSN when unset |
| `embedding.provider` | `ollama` or `openai` | `ollama` |
| `embedding.target` | Embedding service URL | `http://localhost:11434` |
| `embedding.model` | Embedding model | `embeddinggemma` |
| `embedding.dimensions` | Vector dimensions | `768` |
| `opencode.provider` / `opencode.model` | Saved OpenCode choice | unset |
| `telemetry.disabled` | Disable CLI usage telemetry | `false` |
| `update.disabled` | Disable update checks | `false` |

`cassettes = ["https://host/openapi"]` is a top-level array for operator-managed cassette OpenAPI URLs; it is not a dotted `config set` field. See [Cassettes](./cassettes.md) for the manifest, deployment responsibilities, and runtime behavior.

### The internal listener

The API server can run a second listener carrying one endpoint, `GET
/internal/readiness/evidence`. It reports what this process loaded and what
admitting that configuration produced: the instance's identity, a digest of the
cassette source list in effect, and each source's admission result with its
manifest and OpenAPI digests. It publishes no configuration value and no
secret.

It answers a question the read API cannot. `/ping` returns `pong`
unconditionally, and cassette discovery resolves asynchronously after the
process is already serving, so a healthy probe does not mean the cassettes are
admitted yet. An orchestrator that needs to know a configuration actually took
has to ask each serving instance, and has to be told admission results rather
than intent.

It is a separate listener rather than a path on the API server because a
deployment may put a gateway in front of the API that rewrites a public path
prefix onto its root — which would make any path added there publicly
reachable. Expose this one as a container port and keep it off the Service.

| Variable | Purpose |
| --- | --- |
| `TAPES_INTERNAL_LISTEN` | Internal listener address. Unset, no listener runs. Use `:8092`. |
| `TAPES_INTERNAL_TOKEN` | Bearer token every request must present. |

Both are read from the environment only — never from `config.toml` and never
from a flag, because a token in a config file outlives the process that needed
it and one in a flag is readable from the host's process table. Setting
`TAPES_INTERNAL_LISTEN` without `TAPES_INTERNAL_TOKEN` fails startup rather
than serving the endpoint unauthenticated. A request without the token gets a
`401` with no body.

The instance block is filled in from the environment too, and each field is
simply absent when unset. In Kubernetes these come from the downward API:

| Variable | Field |
| --- | --- |
| `TAPES_POD_NAME` | `metadata.name` |
| `TAPES_POD_UID` | `metadata.uid` |
| `TAPES_POD_IP` | `status.podIP` |
| `TAPES_NODE_NAME` | `spec.nodeName` |
| `TAPES_REPLICA_SET` | owning ReplicaSet name, when the deployment can supply it |
| `TAPES_IMAGE_DIGEST` | the running image's digest |

### Read guards

The read API runs every request under a deadline and caps how many
payload-bearing reads one replica serves at once. Both are per-process
settings, with the usual precedence; the flag is spelled one way on
`tapes serve api` and with the `api-` prefix on the bundled `tapes serve`:

| Setting | `tapes serve api` | `tapes serve` | Default |
| --- | --- | --- | --- |
| `api.read_deadline` | `--read-deadline` | `--api-read-deadline` | `20s` |
| `api.payload_concurrency` | `--payload-concurrency` | `--api-payload-concurrency` | `4` |

**The deadline** bounds the request's context. When it elapses, storage
calls still running are cancelled — a cancelled query aborts on the
database side rather than running to completion for a client that has
already given up — and a response still streaming is cut short: the client
sees a truncated document, not a hang. It defaults to 20 seconds, under the
30-second gateway timeout, so a read the gateway has answered with a 504 is
not left running underneath the retry. `0` installs no deadline. The MCP
endpoint (`/v1/mcp`) is exempt: it runs cassette tool calls under its own
30-second timeout, which a shorter read deadline would cut short. So are the
operator jobs under `/v1/admin/` (the demo seed, the derive run, attribution
repair): they are writes that run for as long as the corpus takes, not reads
a client is waiting on.

**The concurrency cap** applies only to the routes whose responses carry
span payloads: `GET /v1/sessions/{id}/traces`, `GET /v1/traces/{trace_id}`,
and `GET /v1/traces/{trace_id}/spans/{span_id}`, and there only to
full-payload reads: a `payload=preview` read of the composite or of a
trace page serves stored previews and never touches a payload column, so
it passes uncounted. The span drill-in has no preview mode and always
counts. A payload read past the cap is not queued; it is answered immediately with `503`, a
`Retry-After: 1` header, and the body
`{"error":"too many concurrent payload reads"}`, so a client retries a
second later against a replica that is still standing. Every other route —
the sessions list, the trace summaries, stats — keeps answering while a
replica is shedding, so a console under load degrades rather than going
dark. A streamed read holds its slot until its stream has been written.
`0` disables the cap.

The gauge `tapes_apiserver_payload_reads_inflight` on `/metrics` reports how
many payload reads the replica is serving at the moment, whether or not the
cap is enabled; a replica sitting at its cap is one whose gauge reads
`api.payload_concurrency`.

### Memory in a container

`tapes serve api` and `tapes serve derive-worker` each set a soft heap limit
at startup, derived from the container's cgroup memory limit: 90% of the
cgroup value becomes the Go runtime's `GOMEMLIMIT`, and the rest is headroom
for stacks, the database driver, and the runtime itself. A large session
allocates transiently far above its live set — the worker re-parses the whole
conversation per turn, the API builds the session's traces or export bundle in
one response — and under the default GC pacing that spike can cross the
container limit and get the process OOM-killed even though its steady state
fits. The soft limit makes the GC pace against the budget instead, trading
some GC CPU for a bounded heap.

There is no setting for this. It tracks whatever memory limit the
orchestrator already gives the container, so there is nothing to keep in
sync. Two things turn it off:

- setting `GOMEMLIMIT` in the environment, which the process honors as-is —
  including `GOMEMLIMIT=off`, which is an explicit opt-out;
- running without a cgroup memory limit (local development, an unconstrained
  container, a non-Linux host), where the Go default stays in place.

Each process logs what it did at startup: `soft memory limit applied` with
the cgroup and soft values, or `honoring GOMEMLIMIT from environment`.

### Example

```toml
version = 0

[storage]
postgres_dsn = "postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable"

[proxy]
provider = "anthropic"
upstream = "https://api.anthropic.com"
listen = ":8080"

[api]
listen = ":8081"
read_deadline = "20s"
payload_concurrency = 4

[client]
proxy_target = "http://localhost:8080"
api_target = "http://localhost:8081"

[vector_store]
target = "postgres://tapes:tapes@localhost:5432/tapes?sslmode=disable"

[embedding]
provider = "ollama"
target = "http://localhost:11434"
model = "embeddinggemma"
dimensions = 768
```

Store provider secrets with `tapes auth openai` or `tapes auth anthropic`, or use the provider's environment variable. Do not put API keys in `config.toml`.

## The client's configuration

`tapesctl` keeps API and ingest URLs in `~/.tapes/config.toml`:

```toml
# ~/.tapes/config.toml
api-url = "http://localhost:8081"
ingest-url = "http://localhost:8082"
```

Write them with:

```bash
tapesctl config set api-url http://localhost:8081
tapesctl config set ingest-url http://localhost:8082
tapesctl config path
```

Three differences from the server's rules are worth stating outright:

- **There is no project-local layer.** The client always reads
  `~/.tapes/config.toml`, whatever directory you run it from. A `.tapes/` in the
  current directory configures the server and is invisible to the client.
- **The API default is explicit.** `--api-url`, `TAPES_API_URL`, and
  `api-url` configure reads; absent all three it uses `http://localhost:8081`.
- **Ingest has its own setting.** `--ingest-url`, `TAPES_INGEST_URL`, and
  `ingest-url` configure capture; absent all three it uses
  `http://localhost:8082`.

`config set` edits the file in place, so comments, ordering, and keys the client
does not know about survive it. `config get` lists only keys it knows *and* that
are set, so empty output does not mean an empty file.

The file sits beside `~/.tapes/logs`, `~/.tapes/skills`, and `~/.tapes/codex-app`
rather than under `$XDG_CONFIG_HOME`, so one directory holds everything the
client writes.
