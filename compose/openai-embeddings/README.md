# OpenAI embeddings Compose stack

This stack sends background span embeddings to OpenAI instead of computing
them with local Ollama. This avoids sustained local CPU usage after a captured
session finishes. Ollama still runs in Docker for synchronous local operations,
such as skill generation, where lower local performance is an expected
tradeoff.

Only the search cassette receives the OpenAI key.

## Configure the key

Copy the example environment file and set its empty value:

```bash
cp .env.example .env
$EDITOR .env
```

Alternatively, export the key in the current shell:

```bash
export OPENAI_API_KEY=sk-...
```

Do not commit `.env` or an API key.

## Start the stack

From this directory:

```bash
docker compose config --quiet
docker compose up --build
```

The stack uses `text-embedding-3-large` with a shortened 768-dimensional
output, matching the default pgvector schema. Switching embedding providers or
models causes existing spans to be embedded again and incurs OpenAI API usage.

## Stop the stack

```bash
docker compose down
```

Volumes are preserved. Add `--volumes` only when intentionally deleting the
local Tapes database.
