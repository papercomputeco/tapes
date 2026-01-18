# Tapes Demo

Tamper-proof telemetry for AI agents via reverse proxy.

## Quick Start

```bash
# Build and run
make build
./build/tapesprox -debug -upstream http://localhost:11434

# Or run the interactive demo
make demo
```

## What It Does

```
Agent → Tapes Proxy → LLM API
            ↓
       Merkle DAG
```

**Zero code changes.** Just point your LLM calls at the proxy.

## Try It

### 1. Send a request

```bash
curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"model": "llama3.2", "messages": [{"role": "user", "content": "Hello"}], "stream": false}'
```

### 2. Check the DAG

```bash
curl http://localhost:8080/dag/stats | jq
# {"total_nodes": 2, "root_count": 1, "leaf_count": 1}
```

### 3. View the audit trail

```bash
curl http://localhost:8080/dag/history | jq '.histories[0].messages'
```

Each message has a cryptographic hash. The hash chain is tamper-proof.

## Key Properties

- **Content-addressable**: Identical content = identical hash = automatic deduplication
- **Hash chain**: Each message links to its parent via hash
- **Tamper-proof**: Modify any message and all subsequent hashes change

## API

| Endpoint | Description |
|----------|-------------|
| `POST /api/chat` | Proxy to upstream LLM |
| `GET /dag/stats` | Node counts |
| `GET /dag/history` | All conversations |
| `GET /dag/history/:hash` | Single conversation chain |
| `GET /dag/node/:hash` | Single node |
