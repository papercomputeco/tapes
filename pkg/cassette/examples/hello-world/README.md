# Hello world cassette

The minimum viable tapes cassette. It is its own Go module and imports nothing
from tapes: a cassette talks to core over HTTP and to Postgres when its
deployment supplies a DSN.

## Run it

Tapes does not start cassettes, so the example ships the deployment that starts
both. `compose.yaml` runs Postgres, the tapes API, and this cassette:

```sh
docker compose up --build -d
```

A cassette is only observable through core, which is why all three are here.
Once tapes has resolved the source, on the tapes API port:

```sh
curl localhost:8081/v1/cassettes                          # discovery
curl localhost:8081/v1/cassettes/hello-world/hello        # the cassette's API
curl localhost:8081/v1/cassettes/hello-world/openapi.json # its spec, from core's cache
curl localhost:8081/openapi                               # core's surface with it merged in
```

Write a row and read it back, to prove the table is a table:

```sh
curl -X POST localhost:8081/v1/cassettes/hello-world/hello
curl localhost:8081/v1/cassettes/hello-world/hello
```

The cassette is also published on `127.0.0.1:9999`, which is worth a look
precisely because it does not know core exists:

```sh
curl localhost:9999/api/hello-world/hello
```

Tear it down. The `-v` matters if you want the database provisioned again:

```sh
docker compose down -v
```

To point a tapes you already run at the cassette, configure the exact URL of its
metadata-bearing OpenAPI document:

```toml
# .tapes/config.toml
cassettes = ["http://127.0.0.1:9999/openapi"]
```

or without editing the config file:

```sh
tapes serve --cassettes=http://127.0.0.1:9999/openapi
```

## The two manifests

The same declaration is published twice, to two readers, at two different times.

`cassette.toml` is the publishable manifest: what a registry or an orchestrator
reads *before* anything is running, to decide whether it can run this cassette
at all — which image, which port, which contract of tapes, what to provision in
Postgres. Tapes never reads it.

The `x-tapes-cassette` extension inside `/openapi` is the same document, embedded
in the spec so that core has one artifact to fetch and one thing to configure.
Core reads only this one.

They are not two schemas. `cassette/manifest.ParseTOML` transcodes TOML into
JSON and runs the same strict parser and validator core runs, so a field that
does not exist is refused identically in both.

Nor are they two documents. Both canonicalize to the same bytes, so both hash to
the same digest — the one core publishes for the copy it fetched over HTTP:

```sh
curl -s localhost:8081/v1/cassettes | jq -r '.cassettes[0].manifest_digest'
# sha256:8171d476...  the digest of cassette.toml
```

If those ever disagree, the running cassette is not the one the manifest
describes.

## Contract

| Endpoint | Purpose |
|---|---|
| `GET /ping` | Health anchor (`api.health`) |
| `GET /openapi` | API contract and versioned `x-tapes-cassette` metadata |
| `GET,POST /api/hello-world/hello` | API under `api.prefix_path` |

The cassette serves `/api/hello-world/hello`; clients call
`/v1/cassettes/hello-world/hello`. Core owns that path rewrite and aggregates
the cassette's OpenAPI document.

## Postgres

Without a database the example keeps rows in memory and says so in the `store`
field of every response. Its deployment may supply an already-provisioned
cassette DSN, which is what `compose.yaml` does — `provision.sql` creates the
role the manifest derives:

```
role   = "cassette_" + name  ->  cassette_hello-world
schema = name                ->  hello-world
```

The cassette creates its own schema and runs its own migration. Core does not
create roles, grant access, inject credentials, or run cassette migrations; the
manifest's `tables` and `depends.views` declare desired deployment state and
nothing more.

To run against a database you provisioned yourself:

```sh
HELLO_WORLD_DATABASE_URL='postgres://...' docker compose up --build -d
```

## Configuration

The deployment supplies environment values. Tapes only publishes their schema.

| Variable | Meaning |
|---|---|
| `CASSETTE_GREETING` | Declared by `x-tapes-cassette.config` |
| `CASSETTE_NAME` | Identity: route, schema, and served prefix all derive from it |
| `CASSETTE_LISTEN` | Process listen address |
| `TAPES_DATABASE_URL` | Pre-provisioned cassette database credential |
