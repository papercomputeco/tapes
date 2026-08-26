# Native Ollama Compose stack

This stack runs Tapes, PostgreSQL, and the bundled cassettes in Docker, but
sends Ollama traffic to the server running on the host. It is most useful on
macOS, where native Ollama can use Metal while an Ollama container cannot use
the Apple GPU.

## Prerequisites

Install and start [Ollama](https://ollama.com/), then verify its API before
starting Compose:

```bash
curl -fsS http://localhost:11434/api/tags >/dev/null && echo "Ollama is ready"
```

On Linux, Ollama must listen on an address containers can reach. For example:

```bash
OLLAMA_HOST=0.0.0.0:11434 ollama serve
```

The Compose stack maps `host.docker.internal` to Docker's host gateway. Ensure
that local firewall rules permit connections to port `11434`.

## Start the stack

From this directory:

```bash
docker compose config --quiet
docker compose up --build
```

The `ollama-pull` service is a second preflight from inside Docker. It waits for
the native server and ensures `embeddinggemma` is installed before background
span embedding starts. If startup remains at that step, inspect it with:

```bash
docker compose logs ollama-pull
```

To verify that macOS is using the GPU while work is active:

```bash
ollama ps
```

## Stop the stack

```bash
docker compose down
```

Volumes are preserved. Add `--volumes` only when intentionally deleting the
local Tapes database.
