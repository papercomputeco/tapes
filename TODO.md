# TODO: span-model proof of concept

- [x] Inspect current ingest/API/storage architecture and identify minimal POC seams.
- [x] Design span/session/turn schema that can coexist with current code while de-emphasizing Merkle nodes.
- [x] Implement DB migrations and storage interfaces for sessions/turns/spans/span_links.
- [x] Implement ingest path that writes span-model records from existing turn payloads.
- [x] Expose read APIs for span sessions/traces.
- [x] Add Pi extension that registers/selects a `tapes` proxy provider, routes provider base URLs through `TAPES_PROXY_URL`, creates trace/span ids, and injects them into proxy-bound provider requests.
- [x] Add proxy header bridge that captures extension-provided trace/span ids and strips them before upstream.
- [x] Add local Ollama OpenAI-compatible upstream mapping and provider instantiation so Pi's OpenAI-compatible provider can traverse the Tapes proxy instead of calling OpenAI directly.
- [x] Add tests for ingest/proxy/API behavior.
- [x] Run formatting and focused tests.
- [x] Summarize implementation and remaining migration work.

## Validation run

- `GOEXPERIMENT=jsonv2 go test ./api ./ingest ./proxy ./proxy/worker ./pkg/storage/inmemory ./cmd/tapes/serve ./cmd/tapes/serve/proxy`
- `GOEXPERIMENT=jsonv2 go test ./... -run '^$'`

Full Postgres integration tests still require `TEST_POSTGRES_DSN` / Dagger, matching the existing test harness behavior.
