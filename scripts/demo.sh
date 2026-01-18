#!/bin/bash
# Interactive demo of tapes proxy
set -e

PROXY_URL="${PROXY_URL:-http://localhost:8080}"
OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"
MODEL="${MODEL:-llama3.2}"
PROXY_PID=""

cleanup() {
    [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null || true
    pkill -f tapesprox 2>/dev/null || true
}
trap cleanup EXIT

pause() {
    [ "${AUTO:-}" = "1" ] && sleep 1 && return
    echo -e "\nPress Enter to continue..."
    read -r
}

# Setup
setup() {
    curl -s "$OLLAMA_URL/api/tags" > /dev/null 2>&1 || { echo "Start Ollama first: ollama serve"; exit 1; }
    [ -f "./build/tapesprox" ] || make build

    pkill -f tapesprox 2>/dev/null || true
    sleep 1
    ./build/tapesprox -debug -upstream "$OLLAMA_URL" > /tmp/tapesprox.log 2>&1 &
    PROXY_PID=$!
    sleep 2
}

# Demo sections
demo_intro() {
    clear
    echo "=== TAPES DEMO ==="
    echo "Tamper-proof telemetry for AI agents"
    echo ""
    echo "Key features:"
    echo "  • Zero code changes - just a reverse proxy"
    echo "  • Cryptographic verification via Merkle DAG"
    echo "  • Automatic deduplication"
    echo "  • Tamper-proof audit trails"
    pause
}

demo_request() {
    clear
    echo "=== 1. Send a Request ==="
    echo ""
    echo "$ curl -X POST localhost:8080/api/chat ..."
    echo ""

    RESP=$(curl -s -X POST "$PROXY_URL/api/chat" \
        -H "Content-Type: application/json" \
        -d '{"model": "'"$MODEL"'", "messages": [{"role": "user", "content": "What is 2+2?"}], "stream": false}')

    echo "Response: $(echo "$RESP" | jq -r '.message.content' | head -c 100)"
    echo ""
    echo "DAG stats:"
    curl -s "$PROXY_URL/dag/stats" | jq .
    pause
}

demo_history() {
    clear
    echo "=== 2. View Audit Trail ==="
    echo ""
    echo "Every message is stored with a cryptographic hash:"
    echo ""

    HISTORY=$(curl -s "$PROXY_URL/dag/history")
    HEAD=$(echo "$HISTORY" | jq -r '.histories[0].head_hash')
    curl -s "$PROXY_URL/dag/history/$HEAD" | jq '.messages[] | {hash: .hash[0:16], role, content: .content[0:40]}'
    pause
}

demo_dedup() {
    clear
    echo "=== 3. Automatic Deduplication ==="
    echo ""

    BEFORE=$(curl -s "$PROXY_URL/dag/stats" | jq -r '.total_nodes')
    echo "Nodes before: $BEFORE"
    echo ""

    echo "Sending two requests with same system prompt..."
    curl -s -X POST "$PROXY_URL/api/chat" \
        -H "Content-Type: application/json" \
        -d '{"model": "'"$MODEL"'", "messages": [{"role": "system", "content": "Be concise"}, {"role": "user", "content": "Hi"}], "stream": false}' > /dev/null

    curl -s -X POST "$PROXY_URL/api/chat" \
        -H "Content-Type: application/json" \
        -d '{"model": "'"$MODEL"'", "messages": [{"role": "system", "content": "Be concise"}, {"role": "user", "content": "Hello"}], "stream": false}' > /dev/null

    AFTER=$(curl -s "$PROXY_URL/dag/stats" | jq -r '.total_nodes')
    echo "Nodes after: $AFTER"
    echo ""
    echo "System message node is shared (content-addressable)"
    pause
}

demo_summary() {
    clear
    echo "=== Summary ==="
    echo ""
    curl -s "$PROXY_URL/dag/stats" | jq .
    echo ""
    echo "• Every conversation cryptographically linked"
    echo "• Identical content automatically deduplicated"
    echo "• Full audit trail reconstructable from any node"
    echo ""
    echo "Demo complete."
}

main() {
    setup
    demo_intro
    demo_request
    demo_history
    demo_dedup
    demo_summary
}

main "$@"
