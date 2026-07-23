# Wire recordings (L1) — placeholder

**No bundles are committed here yet.** This directory reserves the L1 slot in the
fixture pyramid for the first *fixture-grade* wire recordings.

## Format (when bundles land)

A recording is verbatim transport bytes for a single turn, shared between the
wire-trace recorder (on the capture side) and the replay tooling (on the consuming
side):

```
turn-<ns:020>-<seq:08>/
  request.json    request headers + base64 body peek
  response.sse    verbatim response bytes
  meta.json       status / encoding / timings / finalized_by
```

Because a corpus row can carry the raw response bytes, one L2 corpus row is a superset
of an L1 recording — so once raw-response capture is in place, a single artifact can
regenerate every layer and recording<->corpus conversion is a trivial transform.

## Why it's empty

Committing real recordings requires clearing the **fixture-grade** provenance bar
(clean-room, credential-redacted, reviewed — see `../manifest.json`). Until bundles
land, the wire-capture fidelity tests that would assert against them stay skipped and
cross-transport parity is checked only opportunistically.
