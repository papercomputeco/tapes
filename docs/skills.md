---
title: Skills
description: Extract a reusable skill from captured sessions with an LLM, list what you have authored, and install one into an agent.
sidebar:
  order: 9
---

Tapes can extract reusable patterns from derived session transcripts with an LLM, store them under `~/.tapes/skills/`, and sync them into agent skill directories.

A generated transcript follows the main conversation spine: turn-level prompts and responses from traces/spans, excluding harness shadow calls such as permission checks, title generation, and injected context.

All three commands live in the client,
[`tapesctl`](https://github.com/papercomputeco/tapesctl). `generate` reads
session data from the read API; `list` and `sync` touch no server at all.

## Generate

A name in kebab-case is required:

```bash
tapesctl skill generate <session-id> --name debug-react-hooks
```

Use multiple session IDs, or find matching sessions through span search:

```bash
tapesctl skill generate <session-a> <session-b> --name retry-patterns
tapesctl skill generate --search "gum glow charm" \
  --search-top 3 --name charm-cli-patterns
```

Sessions named positionally take priority over `--search`.

Other useful controls are:

```bash
tapesctl skill generate <session-id> --name morning-work \
  --since 2026-02-17 --until 2026-02-17T17:00:00Z \
  --type workflow --preview
```

Skill types are `workflow`, `domain-knowledge`, and `prompt-template`.

Two servers are involved and they are not the same one. `--tapes-url` addresses
the read API that supplies the transcript. `--provider`, `--model`, and
`--api-key` address the LLM that does the extraction:

| Provider | Default model | Key read from | Key required |
| --- | --- | --- | --- |
| `openai` (default) | `gpt-4o-mini` | `OPENAI_API_KEY` | yes |
| `anthropic` | `claude-haiku-4-5-20251001` | `ANTHROPIC_API_KEY` | yes |
| `ollama` | `llama3.2` | either, if set | no |

Prefer the environment variable over `--api-key`: a key passed on the command
line is visible in the process list and in shell history for as long as the
command runs.

## List

```bash
tapesctl skill list
tapesctl skill list --type workflow
```

## Sync

Installing a skill into an agent's skills directory is a local file copy with no
server involved.

By default, sync writes to the global, agent-neutral `~/.agents/skills/`:

```bash
tapesctl skill sync debug-react-hooks
```

Choose project-local or Claude Code paths explicitly:

```bash
tapesctl skill sync debug-react-hooks --local             # .agents/skills/
tapesctl skill sync debug-react-hooks --claude            # ~/.claude/skills/
tapesctl skill sync debug-react-hooks --claude --local    # .claude/skills/
tapesctl skill sync debug-react-hooks --dry-run
```

Generated skills are files you can inspect and version like other agent instructions. Preview generation and use `--dry-run` before syncing when the source session contains project-specific assumptions.
