---
title: Running a cassette locally
description: Walk the hello-world cassette end to end on one machine, from compose up to driving its methods with the client.
sidebar:
  order: 7
---

This walkthrough proves the cassette path end to end on one machine, with
nothing but this repository, Docker, and `tapesctl`: a standalone Tapes admits
the bundled hello-world cassette, republishes its API, and `tapesctl` turns the
discovered surface into commands. No orchestrator or platform deployment is
involved — the registration mechanism is ordinary operator configuration
(`--cassettes`), so everything here works against any Tapes you run yourself.

For the cassette contract itself — the manifest schema, admission rules, and
deployment responsibilities — see [Cassettes](./cassettes.md). This page is the
follow-along companion to it.

## What you need

- Docker with Compose;
- a checkout of [`tapes`](https://github.com/papercomputeco/tapes);
- [`tapesctl`](https://github.com/papercomputeco/tapesctl):

```bash
curl -sSfL https://download.tapes.dev/tapesctl/install | bash
```

## Start the stack

The runnable example at `pkg/cassette/examples/hello-world` ships its own
deployment, because Tapes does not start cassettes — something else always has
to. Its `compose.yaml` runs three services:

- **postgres**, provisioning the cassette's role at first initialization;
- **hello-world**, the cassette, published on `127.0.0.1:9999`;
- **tapes**, the API server on `127.0.0.1:8081`, started with
  `--cassettes http://hello-world:9999/openapi`.

```bash
cd pkg/cassette/examples/hello-world
docker compose up --build -d
```

Both images build from source for your machine's native architecture, which is
what you want. Do not force `--platform linux/amd64` on an Apple Silicon host:
the Go toolchain is unreliable under QEMU emulation and the build can crash.
Cross-building, when actually needed, belongs in a builder that compiles
natively and targets `GOOS`/`GOARCH` — not in this walkthrough.

Tapes retries the cassette source through startup and on every refresh, so the
ordering of the three containers does not matter; give it a few seconds after
`up` returns.

## Verify admission

Discovery lists the admitted cassette, its manifest digest, and any rejected
sources:

```bash
curl -s localhost:8081/v1/cassettes | jq
```

```json
{
  "contract_version": "v1",
  "cassettes": [
    {
      "name": "hello-world",
      "version": "0.0.1",
      "route_prefix": "/v1/cassettes/hello-world",
      "openapi_path": "/v1/cassettes/hello-world/openapi.json",
      "openapi_status": "fresh",
      "manifest_digest": "sha256:8171d476..."
    }
  ],
  "problems": []
}
```

The cached per-cassette document and the aggregate document both republish the
cassette's paths under the Tapes namespace:

```bash
curl -s localhost:8081/v1/cassettes/hello-world/openapi.json | jq '.paths | keys'
# ["/v1/cassettes/hello-world/hello"]

curl -s localhost:8081/openapi | jq '.paths | keys | map(select(startswith("/v1/cassettes")))'
# ["/v1/cassettes", "/v1/cassettes/hello-world/hello"]
```

And the proxied API round-trips — the cassette serves `/api/hello-world/hello`
on its own listener, but clients only ever see the rewritten public path:

```bash
curl -s -X POST localhost:8081/v1/cassettes/hello-world/hello
# {"id":1,"hello":"hello","world":"world","created_at":"..."}

curl -s localhost:8081/v1/cassettes/hello-world/hello
# {"cassette":"hello-world","greeting":"Hello","message":"Hello world",
#  "rows":[{"id":1,...}],"store":"postgres"}
```

`"store": "postgres"` confirms the deployment-supplied credential worked; run
the compose file with an empty `HELLO_WORLD_DATABASE_URL` to watch the same
cassette fall back to memory and say so.

## Drive it with tapesctl

`tapesctl` reads the same discovery surface and generates a subcommand per
cassette, with a method per OpenAPI operation. Because the nouns have to exist
before the command line is parsed, point discovery at the server with the
`TAPES_API_URL` environment variable (the `--api-url` flag also works, on any
subcommand):

```bash
export TAPES_API_URL=http://localhost:8081

tapesctl cassettes                 # `hello-world` has appeared
tapesctl cassettes hello-world --help
```

```text
Commands:
  create-hello  Write one row to the hello table
  get-hello     Greet, and read back every stored row
```

Those names are the cassette's own `operationId`s, kebab-cased, taken from the
document Tapes cached — this binary has never heard of hello-world:

```bash
tapesctl cassettes hello-world create-hello
tapesctl cassettes hello-world get-hello
```

Each method's help names the route it calls, which is the one thing a generated
command name cannot tell you.

The discovered surface is cached per server and revalidated with `ETag`, so
`--help` stays instant and works offline once seen.

## Optional: capture a session beside the cassette

The example composes only the API server, because none of the cassette surface
needs anything else. But the stack it runs is a complete standalone Tapes: add
the ingest server and the derive worker — from the image the compose build
already produced — and it captures real sessions too.

```bash
docker run -d --name hw-ingest --network tapes-hello-world_default \
  -p 127.0.0.1:8082:8082 tapes-hello-world-tapes:latest \
  serve ingest --listen 0.0.0.0:8082 \
  --postgres 'postgres://tapes:tapes@postgres:5432/tapes?sslmode=disable'

docker run -d --name hw-derive --network tapes-hello-world_default \
  tapes-hello-world-tapes:latest \
  serve derive-worker \
  --postgres 'postgres://tapes:tapes@postgres:5432/tapes?sslmode=disable'
```

Run a harness under capture, pointed at the ingest server:

```bash
tapesctl start --ingest-url http://localhost:8082 claude -- -p "Reply with exactly: ok"
```

After the derive worker's debounce (about twenty seconds), the session, its
trace, and its spans are readable from the same API that serves the cassette:

```bash
tapesctl sessions list          # TAPES_API_URL still points at :8081
curl -s "localhost:8081/v1/sessions?limit=5" | jq '.items[].display_title'
```

## Tear it down

```bash
docker rm -f hw-ingest hw-derive   # only if you ran the optional step
docker compose down -v
```

The `-v` removes the Postgres volume, so the cassette role is provisioned again
on the next `up`.

## Where to go from here

Everything the example does — the manifest, the OpenAPI extension, the
provisioning, the admission rules it satisfies — is specified in
[Cassettes](./cassettes.md). Start from the example's source in
[`pkg/cassette/examples/hello-world`](https://github.com/papercomputeco/tapes/tree/main/pkg/cassette/examples/hello-world)
when building your own.
