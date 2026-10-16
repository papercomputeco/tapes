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
