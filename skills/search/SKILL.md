---
name: tapes-search
description: Search over stored LLM sessions using semantic search. Use when you need to find relevant conversations, recall previous session context, or search through historical LLM interactions stored in the tapes system.
---

# Tapes Search Skill

Search over stored LLM sessions using semantic search via the `tapes search` CLI command.

## When to Use

Use this skill when you need to:
- Find relevant past conversations or sessions
- Recall context from previous LLM interactions
- Search through historical data stored in the tapes telemetry system
- Locate specific discussions or topics from past sessions

## Prerequisites

The tapes search requires:
1. A running vector store (e.g., Chroma) with indexed session data
2. An embedding provider (e.g., Ollama) to convert queries to vectors
3. A SQLite database with the session storage

## Quick Start

```bash
tapes search "<your query>" \
  --vector-store-provider chroma \
  --vector-store-target http://localhost:8000 \
  --embedding-provider ollama \
  --embedding-target http://localhost:11434 \
  --embedding-model nomic-embed-text \
  --sqlite ./tapes.sqlite
```

## Command Reference

### Basic Usage

```bash
tapes search <query> [flags]
```

### Required Flags

| Flag | Description | Example |
|------|-------------|---------|
| `--vector-store-provider` | Vector store type | `chroma` |
| `--vector-store-target` | Vector store URL | `http://localhost:8000` |
| `--embedding-provider` | Embedding provider type | `ollama` |
| `--embedding-target` | Embedding provider URL | `http://localhost:11434` |
| `--embedding-model` | Embedding model name | `nomic-embed-text` |
| `--sqlite`, `-s` | Path to SQLite database | `./tapes.sqlite` |

### Optional Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--top`, `-k` | Number of results to return | `5` |
| `--debug` | Enable debug logging | `false` |

## Examples

### Search for Configuration Discussions

```bash
tapes search "how to configure logging" \
  --vector-store-provider chroma \
  --vector-store-target http://localhost:8000 \
  --embedding-provider ollama \
  --embedding-target http://localhost:11434 \
  --embedding-model nomic-embed-text \
  --sqlite ./tapes.sqlite
```

### Get More Results

```bash
tapes search "error handling patterns" \
  --top 10 \
  --vector-store-provider chroma \
  --vector-store-target http://localhost:8000 \
  --embedding-provider ollama \
  --embedding-target http://localhost:11434 \
  --embedding-model nomic-embed-text \
  --sqlite ./tapes.sqlite
```

### Debug Mode

```bash
tapes search "authentication flow" \
  --debug \
  --vector-store-provider chroma \
  --vector-store-target http://localhost:8000 \
  --embedding-provider ollama \
  --embedding-target http://localhost:11434 \
  --embedding-model nomic-embed-text \
  --sqlite ./tapes.sqlite
```

## Output Format

The search results display:
1. **Rank and Score**: Position and similarity score (higher = more relevant)
2. **Hash**: The unique content-addressable hash of the matched message
3. **Role**: Whether the match is from a user or assistant message
4. **Preview**: A snippet of the matched content
5. **Session History**: The full conversation context from root to matched message

Example output:

```
Search Results for: "how to configure logging"
============================================================

[1] Score: 0.8542
    Hash: abc123
    Role: assistant
    Preview: To configure logging in your application ...

    Session (3 turns):
    |-- [user] How do I set up logging?
    |-- [assistant] You can configure logging by ...
    `-> [user] What about debug mode?
```

## Tips

1. **Be specific**: More detailed queries yield more relevant results
2. **Use natural language**: The semantic search understands context and meaning
3. **Adjust top-k**: Increase `-k` if you need more results to find what you're looking for
4. **Check the session context**: The full ancestry helps understand the conversation flow
