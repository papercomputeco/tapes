# Changelog

Kept by hand in [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) style,
grouped under Added / Changed / Removed / Fixed. Entries are written for
somebody deciding whether to upgrade a tapes deployment: each says what changed
for an operator or an API consumer, and any step the upgrade needs, rather than
which commit changed it.

## [Unreleased]

### Upgrading

- **Re-derive to reclassify existing data.** The call-kind changes under
  *Changed* apply to calls derived after the upgrade. Run `tapes dev rederive`
  (or `POST /v1/admin/derive/run`) to apply them to sessions already captured;
  the re-derive also prunes the reclassified calls from search results and
  from main-token accounting.
- **The trusted subject header is now opt-in, with no default.** `tapes serve
  ingest` and `tapes-extproc` used to read the caller's subject from one
  built-in header name. They now read only the header named by
  `TAPES_TRUSTED_SUBJECT_HEADER`, and trust none when it is unset. A deployment
  whose gateway stamps a subject header must set
  `TAPES_TRUSTED_SUBJECT_HEADER` to that header's name on **both** processes
  before upgrading; otherwise new captures keep the identity the capture client
  reported instead of the one the gateway vouched for. Sessions already stored
  are unaffected. See [Configuration](./docs/configuration.md#trusted-subject-header).
- **Derive runs add two tables.** Migration `1781590000` creates
  `derive_runs` and `derive_run_sessions`; it applies with the other
  migrations at startup.

### Added

- `GET /v1/sessions/{id}/transcript` serves a session as one readable
  conversation: rows of `user` and `assistant` text with tool calls and tool
  results attached and harness tags stripped from user text. Filter with
  `role=user|assistant|all`, `q` (case-insensitive substring), and
  `subagents=true` to include subagent threads (root thread only by default);
  page with `limit` (default 200, max 1000) and `cursor`. See
  [APIs](./docs/apis.md#session-transcripts).

- The MCP endpoint (`/v1/mcp`) gains a core `transcript` tool that returns the
  same JSON as one transcript page. See [MCP](./docs/mcp.md).


- `GET /v1/subjects?q=&limit=` lists each distinct `auth_subject` on captured
  sessions with its `session_count` and `last_seen_at`, most recently active
  first, as `{"items": [...]}`. `q` keeps ids containing the substring. tapes
  stores only the opaque id; mapping it to a person is left to whatever fronts
  tapes. Pair it with the existing exact `GET /v1/sessions?auth_subject=<id>`.
  See [APIs](./docs/apis.md#subjects).

- `TAPES_TRUSTED_SUBJECT_HEADER` configures the trusted subject header on
  ingest and `tapes-extproc` (see *Upgrading*).

- Derive runs: `POST /v1/admin/derive/runs` starts a whole-store re-derive as a
  background job and returns `202` with the run; `GET
  /v1/admin/derive/runs/{id}` reports its state and progress, `GET
  /v1/admin/derive/runs` lists recent runs, and `POST .../{id}/cancel` stops
  one. Progress is recorded per session, so a run interrupted by a restart
  resumes at its first unfinished session, and at most one run is active at a
  time. Prefer it to the synchronous `POST /v1/admin/derive/run`, which lasts
  only as long as the request carrying it. Postgres only. See
  [APIs](./docs/apis.md#derive-runs).

- `TAPES_ADMIN_LISTENER` chooses where the `/v1/admin/` routes are served:
  `api` (the default, unchanged) or `internal`, which serves them only on the
  token-guarded internal listener and answers `404` for them on the API
  listener, so operator actions sit beside the readiness evidence and the API
  listener carries reads only. See
  [Configuration](./docs/configuration.md#the-internal-listener).

### Changed

- Two forked side calls the harness makes against the live conversation are no
  longer typed `main`: the few-word progress label requested from a running
  subagent is now `offshoot:progress-label`, and the recap requested when a
  user returns to an idle session is now `offshoot:away-summary`. They leave
  the conversation spine, search, and main-token accounting; a subagent's
  label stays under that subagent's agent span.
- A subagent compacting its own context now keeps `offshoot:compaction` on its
  span instead of being normalized to `main`, so the summarize instruction and
  the summary no longer land on the subagent's spine. Such a call still never
  starts a session-level compaction seam.

### Removed

- The built-in trusted subject header name (see *Upgrading*).
