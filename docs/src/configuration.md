# Configuration

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

## Precedence

For commands that bind a setting, precedence is:

1. CLI flag;
2. `TAPES_...` environment variable;
3. `config.toml` value;
4. built-in default.

Dots become underscores in environment names, for example `TAPES_PROXY_LISTEN` and `TAPES_STORAGE_POSTGRES_DSN`.

## Manage settings

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

## Example

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
