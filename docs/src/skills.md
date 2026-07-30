# Skills

Tapes can extract reusable patterns from derived session transcripts with an LLM, store them under `~/.tapes/skills/`, and sync them into agent skill directories.

A generated transcript follows the main conversation spine: turn-level prompts and responses from traces/spans, excluding harness shadow calls such as permission checks, title generation, and injected context.

## Generate

A name in kebab-case is required:

```bash
tapes skill generate <session-id> --name debug-react-hooks
```

Use multiple session IDs, or find matching sessions through span search:

```bash
tapes skill generate <session-a> <session-b> --name retry-patterns
tapes skill generate --search "gum glow charm" \
  --search-top 3 --name charm-cli-patterns
```

Other useful controls are:

```bash
tapes skill generate <session-id> --name morning-work \
  --since 2026-02-17 --until 2026-02-17T17:00:00Z \
  --type workflow --preview
```

Skill types are `workflow`, `domain-knowledge`, and `prompt-template`. Generation defaults to the OpenAI provider; select `--provider openai|anthropic|ollama`, plus `--model` or `--api-key` when required. It may connect to the API with `--api-target` or use `--postgres` for a local in-process API.

## List

```bash
tapes skill list
tapes skill list --type workflow
```

## Sync

By default, sync writes to the global, agent-neutral `~/.agents/skills/`:

```bash
tapes skill sync debug-react-hooks
```

Choose project-local or Claude Code paths explicitly:

```bash
tapes skill sync debug-react-hooks --local             # .agents/skills/
tapes skill sync debug-react-hooks --claude            # ~/.claude/skills/
tapes skill sync debug-react-hooks --claude --local    # .claude/skills/
tapes skill sync debug-react-hooks --dry-run
```

Generated skills are files you can inspect and version like other agent instructions. Preview generation and use `--dry-run` before syncing when the source session contains project-specific assumptions.
