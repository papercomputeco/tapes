# MCP tool cassette

A minimal cassette that advertises one MCP tool. The operation-level
`x-tapes-mcp` declaration publishes the tool as `mcp-tool.ping`; calling it with
`{"ping":"ping"}` returns `{"pong":"pong"}`.

Run the cassette:

```sh
make run
```

Point Tapes at its OpenAPI document:

```sh
tapes serve api --cassettes=http://127.0.0.1:9999/openapi
```

Then connect an MCP client to Tapes at `http://127.0.0.1:8081/v1/mcp`. The cassette
also works directly over HTTP:

```sh
curl -X POST http://127.0.0.1:9999/api/mcp-tool/ping \
  -H 'Content-Type: application/json' \
  -d '{"ping":"ping"}'
# {"pong":"pong"}
```

The example uses only Go's standard library. Its OpenAPI 3.1 document contains
both the cassette manifest and the MCP tool declaration.
