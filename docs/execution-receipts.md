---
title: Execution receipts
description: Bounded per-attempt evidence from the final dispatch capture point.
sidebar:
  order: 12
---

The separate `tapes-extproc` process can attach an initial execution receipt to
responses. Set `TAPES_FINAL_DISPATCH_RECEIPTS=true` only on a filter chain verified
to capture the final outbound request after every gateway translation and model
or settings override. The default is disabled. A deployment without that evidence
must leave receipts disabled; a header carrying a predicted route does not prove
final settings.

The capture filter requires trusted `x-vsr-selection` and `x-paper-gateway-id`
request headers. The router selection is base64url JSON version 1 containing the
request's config hash, decision, immutable destination ID and optional selected
model/backend/protocol. It reports intent.

A named recipe adds six fields together inside that same version-1 selection:
`recipe`, `configurationId`, `configurationRevision`, `shape`, `backendName` and
`backendGeneration`. `configHash` remains the exact leased runtime generation.
`backendName` is the live org binding's logical name, while `backendGeneration`
identifies its contents resolved into that runtime. Backend edits can change the
latter and `configHash` without changing the saved configuration revision. Shape
comes directly from the selected policy decision's target; decision names such
as `sensitive` do not imply a shape. A no-match default has an empty `decision`
and an explicit `shape`.

The parser accepts legacy selections without any of these fields. Supplying any
recipe field requires all six, valid bounded identifiers and one of the supported
shapes (`frog`, `rabbit`, `fox`, `dragon`). Partial, empty or null context, duplicate
JSON names, case-folded aliases and unknown fields suppress the receipt. The
selection payload is limited to 4 KiB before base64 encoding; the complete receipt
retains its 8 KiB encoded-header limit. No signal arrays are included.

For recipe context, the compiler must set the router backend reference name
(the selection's existing `backend` field) to the final generated AIServiceBackend
name. It is distinct from both the destination ID and the org binding name.
Conflicting trusted final backend metadata suppresses that recipe receipt,
preventing a backend generation from describing a different physical route.
Missing dispatch evidence leaves the outcome `selected`. Legacy selections keep
their existing intent-versus-actual behavior. Final model and settings still come
from the captured outgoing body, even when they differ from router intent.

On managed routes, actual backend
identity comes only from Envoy's `io.envoy.ai_gateway` dynamic metadata `backend_name` (the
AIServiceBackend segment of the composite route identity); header copies are not
accepted as backend evidence. The forwarded model and scalar
settings come from the final captured JSON body. The capture filter strips the
selector headers before forwarding and removes provider-supplied receipt and
`x-vsr-*` response headers before stamping its own receipt. The outer gateway must
also strip caller-supplied selectors and overwrite gateway identity. Configure
`metadata.accessibleNamespaces` to include `io.envoy.ai_gateway` on the capture
filter. Native subscription routes instead expose `io.paper.native_dispatch` through
`metadata_options.forwarding_namespaces.untyped`. The final loopback listener
stamps `backend_name`, `destination_id` and `service` from the exact selected
route's static metadata before capture. This adapter accepts only `anthropic` or
`chatgpt`, a plain backend ID, and a destination matching the router selection.
Malformed native metadata or simultaneous native/AI Gateway metadata suppresses
the receipt for that attempt. Caller header copies cannot substitute for this
metadata. Direct routes without an authoritative backend metadata adapter remain
unqualified. A separate provider-side response sanitizer must strip receipt
namespaces before the capture filter runs in the encode direction, including
when capture fails open.

Backend metadata is observed again at response headers, before the initial
receipt is emitted. Envoy AI Gateway v1.1.0 publishes `backend_name` in its
cluster's upstream ext_proc filter, after the downstream HTTP connection manager
(HCM) request filters have run. A complete, successfully decoded request capture
can therefore be paired with backend evidence first received on the response
path. Missing backend evidence leaves the outcome `selected`; an incomplete,
over-budget or undecodable request does not establish forwarded settings.
Conflicting backend identities or native/managed metadata authorities suppress
the receipt for that attempt.

This timing does not establish that an HCM request capture contains the final
provider bytes. AI Gateway's upstream filter can still translate or mutate the
body after that capture, even when Tapes follows AI Gateway in the HCM filter
order. Response metadata supplies backend identity, not a replacement request
body. Before enabling receipts, qualify the route's upstream filter configuration
and compare the captured model/settings with provider-side recorded bytes. If
later mutations can change those fields, keep receipts disabled or move capture
after those mutations. Receiving backend metadata on the response path alone
does not qualify a deployment.

`x-paper-execution-receipt` is unpadded base64url JSON `ExecutionReceiptV1` with an
8 KiB **encoded header** limit. The Go owner type is in
`extproc/execution_receipt.go`; its generated structural schema is
`extproc/testdata/execution-receipt-v1.schema.json` (inferred with
`jsonschema.For[ExecutionReceiptV1]` and checked by the owner tests). Runtime
validation additionally restricts the allowed scalar values and header size.
Consumer wire examples are
`extproc/testdata/execution-receipt-v1.json` and
`extproc/testdata/selection-rejected-v1.json`. Named recipe examples are
`extproc/testdata/selection-recipe-v1.json`,
`extproc/testdata/selection-recipe-rejected-v1.json` and
`extproc/testdata/execution-receipt-recipe-v1.json`. The recipe receipt golden is
compared with actual adapter output after normalizing generated receipt/attempt
IDs; selection fixtures mirror the router producer's contract. Each invocation receives fresh
server receipt/attempt IDs independent of browser run IDs. The consumer binds
these to the authenticated invocation and expected gateway. Neither browser IDs
nor receipt IDs are Tapes session/trace IDs.

Receipts contain only allowlisted scalar sampling/output-limit/stream/reasoning
settings. They never include prompt text, tool schemas, arbitrary nested fields,
credentials, signed thinking or encrypted reasoning. Values are `forwarded`,
`omitted-provider-default` (actual provider default unknown), or `unknown`.
An unreadable or null `reasoning` or `thinking` object makes its inspected
nested settings `unknown`; the invalid value is not copied into the receipt.
Absent objects and absent fields inside valid objects remain
`omitted-provider-default`. Other settings are evaluated independently.
Without an original snapshot this producer cannot identify `overridden` or
`removed` values; absence alone never establishes either state. Settings retain
native field names because `max_tokens`, `max_completion_tokens` and
`max_output_tokens` have different protocol meanings.

Initial outcomes are `selected`, `dispatched`, or `failed` with a safe
`upstream-response` failure stage. A `dispatched` receipt establishes forwarding,
never successful inference. The downstream stream adapter explicitly owns
completion, in-stream failure and cancellation, and must correlate those events
with this attempt. Provider model/usage are separate terminal facts and must not
replace the forwarded model. Capture or ingestion outages cannot block streaming;
missing instrumentation leaves evidence unavailable instead of inventing values.

A router rejection before downstream capture may instead carry only
`x-vsr-selection` on its immediate response. Render that as selected but not
served; it is not an execution receipt. The gateway must preserve this trusted
local response while rejecting caller/provider copies. A failure before selection
has no selection evidence. Real-chain tests must verify this local-reply path,
filter order, final bytes, spoof rejection and terminal delivery before enabling
receipts for a route.
