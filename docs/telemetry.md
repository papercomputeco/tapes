---
title: Telemetry
description: What the server's CLI reports to PostHog, how to disable it, and why the client reports nothing.
sidebar:
  order: 14
---

This page is about the `tapes` CLI. The client,
[`tapesctl`](https://github.com/papercomputeco/tapesctl), has no telemetry: no
usage events, no PostHog client, and no environment variable to disable,
because there is nothing to disable.

## The server CLI

Release builds can send CLI usage events to PostHog when built with a PostHog project key. A source build without that injected key creates no PostHog client.

The implementation uses a random persistent UUID as the PostHog distinct ID. It stores that UUID and a first-run timestamp in `telemetry.json` under the resolved `.tapes/` directory. Common event properties include CLI version, operating system, architecture, and `$lib = tapes-cli`.

Event-specific properties in the current client include command name, init preset, provider for session creation, search result count, server mode, MCP tool name, and error command/type. The code also defines install and sync events without additional properties.

> These are implementation-level event fields, not a claim that the surrounding network or PostHog service cannot observe other transport metadata. Review the code and your PostHog deployment requirements before relying on telemetry privacy properties.

## Disable telemetry

Use any one of:

```bash
tapes --disable-telemetry status
```

```bash
export TAPES_TELEMETRY_DISABLED=true
```

```toml
[telemetry]
disabled = true
```

The CLI also disables telemetry when it detects common CI environment variables, including `CI`, `GITHUB_ACTIONS`, `GITLAB_CI`, `CIRCLECI`, `TRAVIS`, `JENKINS_URL`, `BUILDKITE`, and `CODEBUILD_BUILD_ID`.

The global flag must precede the subcommand, as in `tapes --disable-telemetry serve`.
