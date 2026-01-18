# Test Plan

## Automated Tests

```bash
make test-integration  # Full test suite
make demo              # Interactive demo
make unit-test         # Unit tests
```

## Manual Verification

### 1. Basic proxy

```bash
./build/tapesprox -debug -upstream http://localhost:11434

curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"model": "llama3.2", "messages": [{"role": "user", "content": "Hi"}], "stream": false}'
```

### 2. DAG inspection

```bash
curl http://localhost:8080/dag/stats | jq
curl http://localhost:8080/dag/history | jq
```

### 3. Deduplication

Send same system prompt twice, verify shared node.

### 4. History chain

```bash
HEAD=$(curl -s http://localhost:8080/dag/history | jq -r '.histories[0].head_hash')
curl http://localhost:8080/dag/history/$HEAD | jq
```

## Pass Criteria

- [ ] Proxy forwards requests correctly
- [ ] DAG stores messages with hashes
- [ ] Identical content produces identical hashes
- [ ] History chain is reconstructable
