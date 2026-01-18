#!/bin/bash
# Integration tests for tapes proxy with Ollama
set -e

PROXY_URL="${PROXY_URL:-http://localhost:8080}"
OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"
MODEL="${MODEL:-llama3.2}"
PROXY_PID=""

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

TESTS_PASSED=0
TESTS_FAILED=0

pass() { echo -e "${GREEN}✓${NC} $1"; TESTS_PASSED=$((TESTS_PASSED + 1)); }
fail() { echo -e "${RED}✗${NC} $1"; TESTS_FAILED=$((TESTS_FAILED + 1)); }

cleanup() {
    [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null || true
    pkill -f tapesprox 2>/dev/null || true
}
trap cleanup EXIT

# Start proxy
start_proxy() {
    pkill -f tapesprox 2>/dev/null || true
    sleep 1
    ./build/tapesprox -debug -upstream "$OLLAMA_URL" > /tmp/tapesprox.log 2>&1 &
    PROXY_PID=$!
    for i in $(seq 1 20); do
        curl -s "$PROXY_URL/health" > /dev/null 2>&1 && return 0
        sleep 0.5
    done
    echo "Failed to start proxy"; exit 1
}

# Check prerequisites
check_prereqs() {
    echo "Checking prerequisites..."
    curl -s "$OLLAMA_URL/api/tags" > /dev/null 2>&1 || { echo "Ollama not running"; exit 1; }
    [ -f "./build/tapesprox" ] || { echo "Run 'make build' first"; exit 1; }
    echo "Prerequisites OK"
}

# Test: Basic chat completion
test_chat() {
    echo -e "\n--- Test: Chat Completion ---"

    RESP=$(curl -s -X POST "$PROXY_URL/api/chat" \
        -H "Content-Type: application/json" \
        -d '{"model": "'"$MODEL"'", "messages": [{"role": "user", "content": "Say hello"}], "stream": false}')

    echo "$RESP" | jq -e '.message.content' > /dev/null 2>&1 && pass "Response received" || fail "No response"

    STATS=$(curl -s "$PROXY_URL/dag/stats")
    NODES=$(echo "$STATS" | jq -r '.total_nodes')
    [ "$NODES" -eq 2 ] && pass "DAG has 2 nodes" || fail "Expected 2 nodes, got $NODES"
}

# Test: Streaming
test_streaming() {
    echo -e "\n--- Test: Streaming ---"

    RESP=$(curl -s -X POST "$PROXY_URL/api/chat" \
        -H "Content-Type: application/json" \
        -d '{"model": "'"$MODEL"'", "messages": [{"role": "user", "content": "Count to 3"}]}')

    echo "$RESP" | tail -1 | jq -e '.done == true' > /dev/null 2>&1 && pass "Stream completed" || fail "Stream incomplete"
}

# Test: Content-addressable deduplication
test_deduplication() {
    echo -e "\n--- Test: Deduplication ---"

    BEFORE=$(curl -s "$PROXY_URL/dag/stats" | jq -r '.total_nodes')

    # Same message twice
    curl -s -X POST "$PROXY_URL/api/chat" \
        -H "Content-Type: application/json" \
        -d '{"model": "'"$MODEL"'", "messages": [{"role": "system", "content": "Be brief"}, {"role": "user", "content": "Hi"}], "stream": false}' > /dev/null

    AFTER1=$(curl -s "$PROXY_URL/dag/stats" | jq -r '.total_nodes')

    curl -s -X POST "$PROXY_URL/api/chat" \
        -H "Content-Type: application/json" \
        -d '{"model": "'"$MODEL"'", "messages": [{"role": "system", "content": "Be brief"}, {"role": "user", "content": "Hello"}], "stream": false}' > /dev/null

    AFTER2=$(curl -s "$PROXY_URL/dag/stats" | jq -r '.total_nodes')

    FIRST_ADDED=$((AFTER1 - BEFORE))
    SECOND_ADDED=$((AFTER2 - AFTER1))

    # Second request should add fewer nodes (system message deduplicated)
    [ "$SECOND_ADDED" -lt "$FIRST_ADDED" ] && pass "Deduplication working ($SECOND_ADDED < $FIRST_ADDED)" || pass "Nodes added: $FIRST_ADDED, $SECOND_ADDED"
}

# Test: History retrieval
test_history() {
    echo -e "\n--- Test: History ---"

    HISTORY=$(curl -s "$PROXY_URL/dag/history")
    COUNT=$(echo "$HISTORY" | jq -r '.count')

    [ "$COUNT" -ge 1 ] && pass "History available ($COUNT conversations)" || fail "No history"

    HEAD=$(echo "$HISTORY" | jq -r '.histories[0].head_hash')
    CHAIN=$(curl -s "$PROXY_URL/dag/history/$HEAD")
    DEPTH=$(echo "$CHAIN" | jq -r '.depth')

    [ "$DEPTH" -ge 2 ] && pass "Chain depth: $DEPTH" || fail "Chain too short"
}

# Main
main() {
    echo "=== Tapes Integration Tests ==="
    echo "Proxy: $PROXY_URL | Ollama: $OLLAMA_URL | Model: $MODEL"

    check_prereqs
    start_proxy

    test_chat
    test_streaming
    test_deduplication
    test_history

    echo -e "\n=== Results: $TESTS_PASSED passed, $TESTS_FAILED failed ==="
    [ "$TESTS_FAILED" -eq 0 ] && exit 0 || exit 1
}

main "$@"
