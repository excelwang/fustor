---
version: 1.2.6
---

# L3: Passive source operations

## [interface] OneShotSourceInspection

**Rationale**: Reuse native correctness without constructing another deployed service.

```text
fustor-source-inspect read --kind jsonl|csv|local-fs|s3|mysql|es|pg|graph --config native.json < SourceExecutionEnvelope.json
fustor-source-inspect validate|describe --kind jsonl|csv|local-fs|s3|mysql|es|pg|graph --config native.json|-
fustor-source-inspect budget < BudgetEstimateRequest.json
```

JSON/YAML config and stdin are bounded at 2 MiB each. Configuration resolves native grants and credential references. Version, deadline, epoch, scope and explicit request limit are validated before reads. MessagePack replies are integrity-checked before JSON presentation; receipt byte usage remains native MessagePack bytes. PG/MySQL snapshots and ES PITs close before output and one-shot cursors are not reusable. `describe` projects native status/capability reports without provider I/O. Graph conformance is not a production DataView API.

In independent one-shot deployments the native resource grants are installed
Fustor application read policy, supplied by the deployment owner, not by the read
request or source-registration declaration. The compatibility carrier name
`__cnx_runtime.resource_grants` does not certify that Capanix issued these grants:
its generic host-object grants are separate and imply no database query scope.
Protect the installed policy and credential references from untrusted callers;
each invocation consumes current configured policy and rechecks its epoch. This
enforcement can restrict reads but cannot grant backend privileges. Native
provider access is exercised by the actual read; unverified backend permissions
stay unverified under `CONTRACTS.SOURCE.NATIVE_READ_AUTHORIZATION`. Neither a
successful read nor the configuration digest is a Coox execution-owner proof.

MySQL native endpoints optionally carry `tls: {ca_file?: absolute-path}`. Presence
requires TLS with both certificate-chain and host/IP verification; an empty map
uses normal trusted roots. The optional public CA certificate path is installed
configuration, never a request field or credential value. Configuration parsing
rejects non-map settings, unknown/bypass keys and empty, relative or parent-
traversing CA paths without reading files. At connection time the native driver
loads the configured CA; missing/invalid CA or peer identity fails closed. No
plain connection retry or certificate/hostname bypass is provided. Legacy
`?ssl=true` endpoint syntax remains supported, and its requirement survives a
runtime endpoint override even if the new address omits that query parameter.
This implements `CONTRACTS.SOURCE.SOURCE_CREDENTIAL_AND_SCOPE_ISOLATION` through
the existing source runtime adapter, not a new transport service.
The runtime explicitly enables its MySQL client's TLS implementation; merely
constructing TLS options in a build without that feature is not support. Portable
inspection builds retain the same trust checks through the vendored TLS library.

FS/S3 `sample-jsonl|sample-csv` operations require an exact granted file path/object key (`path`/`prefix`), explicit row limit and no cursor. They return typed `content`, distinct from `entries`/`objects`. Fetch at most min(max_bytes, 4 MiB); validate complete records only, bounded CPU/deadline, reject malformed headers/JSON and revision drift. FS verifies the opened descriptor and restats its version. S3 uses HEAD plus conditional range GET, pins version ID when available, disables automatic retries, and accounts for both calls. ETag is an opaque native version condition, never a content hash. `prefix_sha256` hashes only `bytes_read` bytes from offset zero; `complete=false` cannot assert full-object validity. No format conversion, persistent download or ETL is performed.

Validation accepts proposed config on stdin with `--config -` and does not connect to a source. Existing source error codes remain compatible; optional `budget_exhaustion` identifies only native max_bytes/max_latency_ms exhaustion. Process watchdog and transport failures are not classified as source budget exhaustion by callers.

`validate-scope --kind <kind> --config -` accepts a bounded JSON object with
`configuration` (the proposed native document) and `selection` (native
`object_ref`, `grant_epoch`, plus the source family's exact `path`, `schema/table`,
`index`, `bucket/prefix`, or `configured_query_id`). It reuses owner configuration
parsing and native endpoint/grant scope resolution, rejecting absent/revoked grants,
epoch mismatches, wrong source-family selection fields and out-of-scope objects.
It returns `{valid:true, scopeAuthorized:true, readiness:"not-observed"}` only;
no driver, credential resolution, connection, file content read, receipt, source
revision or admission is involved. This is configuration/scope preflight, not a
lease or complete read-request validation: later reads recheck current authority,
capability, request parameters and all execution bounds. Callers preserve the
native product/runtime grant separation instead of implementing wildcard/grant
semantics or minting new grants while registering narrower object selections.
Failures of this command use `source-scope-invalid`; existing commands retain
`source-inspect-error`. Callers distinguish scope rejection from missing tools,
unsupported preflight commands and transport failure; they never fall back to
format-only validation or claim that unavailable validation proved a denial.

`read-wire` has the same request, native validation, bounds and session closure as
`read`, but returns the original validated MessagePack response bytes on stdout.
This opt-in transport is for source-result admission, not a second read or JSON
re-encoding of a receipt. Errors remain on stderr. Ordinary `read` output is
unchanged and Dashboard must not expose raw carrier bytes in its public API.
JSON `read` and `describe` additionally report `configurationRevision`, the SHA-256
of the exact native configuration bytes parsed for that invocation. This is
configuration-use evidence, not a source revision, receipt field or proof that a
later read uses the same configuration. Older tools lacking the field remain
unverified; callers must not fill it from their own earlier file read.

## [workflow] CallerManagedConfigurationObservation

Implements Components.PassiveSourceInspection and
`CONTRACTS.INSPECTION.NATIVE_INSPECTION_PARITY`,
`CONTRACTS.BOUNDARY.CALLER_AGNOSTIC_TOOLING`.

**Rationale**: Native validation and evidence can support first registration
without making Fustor a caller-specific configuration or workflow service.

**Steps**:
1. Caller prepares a source-native configuration within its deployment policy,
   using declared credential references rather than inline secrets.
2. Caller invokes native `validate --config -` before any business registry entry
   exists. Validation performs no provider I/O, starts no workflow and grants no
   access. Invalid/unsupported configuration returns a native typed failure.
3. Caller-owned configuration tooling persists/registers the valid file and tracks
   its saved revision. Fustor supplies no registry transaction or deployment artifact.
4. A later explicit source read uses current native grants/envelope and reports
   `configurationRevision` from the exact bytes it parsed. Only a successful actual
   content/structure read is evidence of that read consuming the revision; `describe`,
   validation, probing, save or budget estimation is not such evidence.
5. Caller compares native consumption with its saved revision while preserving the
   original source receipt/revision/time. Missing evidence remains unverified and
   a failed newer read does not retimestamp old success.

No Agent principal, instruction reference, package or Trace type is added to the
source request/receipt or DataView schema. A caller may retain its own correlation
metadata around native opaque request ids. Ordinary reads do not automatically
admit results; existing explicit issue/submit or declared-result contracts remain
the only admission paths. PPG receives no special coupling to those paths.

AF-02/AF-06 integration uses the same public native CLI as other callers; tests of
this boundary do not certify the external console's API registration or identity.

## [interface] BoundedSourceResultHandoff

**Rationale**: A one-shot caller needs access to the existing visible-action and
source-result contracts without inventing receipts or authoritative graph facts.

```text
fustor-data-view-inspect action-issue --config native.json < selection.json
fustor-data-view-inspect source-submit --config native.json < carrier.json
```

Implements `Components.BoundedActionRegistry`, `Components.ResultNormalizerRegistry`
and `CONTRACTS.DATA_VIEW.DEMAND_MATERIALIZATION`,
`CONTRACTS.DATA_VIEW.RESULT_NORMALIZATION_OWNERSHIP` through existing runtime APIs.
Owner configuration supplies bounded `source_policies`: profile id, source kind,
source authority and concrete authority object, producer, policy labels/digest
and context lifetime limit.
Principal, scope, grants, discovery evidence and limits remain in the native
scoped request; stdin cannot replace them. Policy lookup, current authority
visibility and grant epoch are checked on both issue and submit.

Issue takes a profile id, an actually visible action id and a bounded lifetime;
Fustor reprojects current configured scope and issues that action's native context.
Submit takes that profile id, exact issued context, submission id and original
MessagePack response as base64. The source-schema adapter verifies the native
receipt and copies its identity, anchors, revision, time and actual usage before
the existing atomic commit gate. The selected source kind determines the builtin
schema and normalizer; callers supply no schema, graph assertions or freshness.
Only a newly observed response inside the issued context may be admitted.

The tool performs no source I/O, workflow execution or implicit materialization
on `scoped`. Missing profiles, unknown/invisible actions, expired/stale contexts,
wrong request/authority/schema, changed bytes or revoked scope fail closed.
State persistence keeps action contexts and original outcomes across invocations;
an identical resubmission returns its original outcome. Public CLI acceptance
uses actual temporary native files and native readers, not constructed receipts,
and checks nonempty normalized relationships, restart, scope reduction and
unchanged state on rejection. Real application acceptance uses existing bounded
sample objects and keeps the external domain graph out of DataView.

## [interface] ReadOnlyDeclaredOutcomeInspection

**Rationale**: Checking whether an external result was admitted must not repeat
the admission or run a projection that registers actions or revision aliases.
Implements Components.DeclaredResultAdmissionRegistry and
`CONTRACTS.DATA_VIEW.DECLARED_EXTERNAL_RESULT_ADMISSION`.

```text
fustor-data-view-inspect declared-inspect --config native.json
  stdin: {profile_id, idempotency_key}
  stdout: native declared-outcome payload with the original outcome or null
```

The query reads one retained, atomically persisted state snapshot, takes no
writer lock and refuses every persistence attempt. Missing state is not
initialized. Owner configuration, not stdin, selects producer, result authority,
current grant and policy. Its authority and required labels must remain visible
and its grant epoch must match the configured current scope. Lookup requires a
retained admission with the exact current policy and idempotency identity, then
returns the original commit outcome without constructing a new one. Unknown,
changed-policy and unauthorized identities produce the same null outcome.
No product bytes, source calls, expiry extension, new action, head change or
scoped revision alias is involved. This proves a retained admission, not that
its result is currently visible, fresh or actionable. Ordinary `scoped` remains
a separate projection operation, not this strict no-write recovery surface.

## [interface] NativeGraphInspection

**Rationale**: A graph is a peer source, not a DataView backend or an application-specific feature.

```text
GraphSourceService(SourceControlState<GraphSourceRuntimeConfig>, GraphSourceDriver)
  handle_event(SourceExecutionEnvelope<GraphSourceRequest>) -> SourceResponseEnvelope<GraphSourcePayload>
```

The `graph-source` owner accepts only `probe-connection` and `configured-query` requests through SourceExecutionEnvelope. Configuration supplies endpoints, environment credential references, exact query IDs/statements/parameter names, result schema refs and hard limits; runtime grants independently constrain object and query scopes and epoch. No caller query text, writes, continuation, automatic query retries or workflow execution is admitted. Fustor's mandatory native authorization includes a conservative read-only syntax gate; backend read-only identities are optional defense in depth. The legacy `read_only_identity` field is an optional declaration, never verified permission evidence or an authorization prerequisite. Scalar/table JSON results, not graph mutation assertions, are normalizable using `fustor.graph.source-payload` v1. Implements CONTRACTS.SOURCE.NATIVE_READ_AUTHORIZATION.

The initial provider is Bolt with an optional PuppyGraph status revision probe. The
endpoint's query credential and optional status credential are independent
deployment-owned references because providers may expose different accounts on the
two protocols; omitting the status reference deliberately reuses the query reference
for backward compatibility. A query with that probe budgets three logical provider
operations: status before, bounded transaction read/rollback, status after. Revision
drift rejects the observation. The schema revision and status-evidence digest do not
prove relational/cache generation alignment. Without a provider revision probe the
source revision is unknown; the adapter must not fabricate it from row hashes.
Each complete status body consumes the request's byte ledger and is read only up to
the smaller of its remaining bytes and 1 MiB. This supports bounded providers whose
schema-rich status exceeds 64 KiB without turning status into an unbounded side read;
an oversized body remains an explicit `max_bytes` exhaustion and no query result is
reported.
Connections and transactions end before one-shot output. PPG-specific status parsing
stays inside the driver, and never creates a special DataView relationship.

The bounded native Bolt transport offers 5.3, 4.4, 4.1 and 4.0 in one handshake.
Version 5.3 separates HELLO (including a non-identifying bolt_agent product)
from basic LOGON; 4.x carries authentication in HELLO. Only an offered exact
version is accepted. A one-shot connection uses BEGIN(mode=r, tx_timeout),
one configured RUN, PULL(n=1), and ROLLBACK before publication. There is no
COMMIT, routing, reconnect, automatic retry, HTTP query fallback or provider
upgrade. `bolt+s` verifies the TLS certificate and hostname without downgrade.
The remaining native deadline covers connection, authentication, transaction,
decoding and rollback; expiration drops the owned socket. Rejected/failed reads
return no partial rows and close the connection even if rollback cannot finish.

PackStream decoding accepts only JSON-compatible scalar/list/map values,
rejects graph structures, duplicate keys/columns, non-finite floats, trailing
bytes and invalid response ordering. Wire input is bounded before allocation
by the remaining result-byte budget plus 64 KiB protocol allowance; each message
has at most 65,536 decoded values and depth 32. Requests have a 256 KiB wire cap.
At most one row beyond max_rows may be read solely to reject truncation; never
publish it. Result bytes retain native accounting, not the protocol allowance.
Handshake success alone is not certification: the public CLI must authenticate,
read real rows and complete rollback. This separates actual PPG 0.118a Bolt 5.3
compatibility from its advertised but failing 4.x authentication path. Failures
disclose only fixed stage labels, never provider error text or credentials.

Protocol references: [Bolt messages](https://neo4j.com/docs/bolt/current/bolt/message/)
and [PackStream](https://neo4j.com/docs/bolt/current/packstream/).

## [algorithm] PassiveBudgetEstimate

**Rationale**: Adapt cost envelopes from evidence without changing authority or query meaning.

```text
estimate_source_budget(context, defaults, hard_caps[], history[], overrides)
 -> {limits, max_latency_ms:{effective,hard_cap,mode,basis,sample_count,insufficient}, max_bytes:{...}}
```

Filter exact caller-supplied comparison key, nonfuture timestamps and preceding 24h; retain newest 100 accesses. Success requires receipt usage without truncation. Under ten successes use defaults; otherwise ceil(1.5 * nearest-rank P95), floor 1000ms/65536bytes. The newest explicit exhaustion adds a 2x failed-allocation floor to that field on the next comparable read; later successes clear it. Auth/network/watchdog failures do not train. Manual per-field override must be positive and within cap. Intersect all hard bounds; reject nonadaptive row/call/fanout/cost excess rather than rewriting query semantics. No retries or writes occur in this component.
