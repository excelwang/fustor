# Fustor DataView Research Boundary

## Research Target

Fustor is a caller-agnostic data tooling layer for private-data discovery and
bounded data interaction. Its current research target is a deployment-global
logical **DataView**: a demand-materialized association view whose current head
can be built incrementally from complete results returned by explicitly executed
bounded source actions.

The canonical functional stack is:

```text
Source -> SourceProjection (optional) -> DataView
                                      -> private discovery access/control plane
```

A `SourceProjection` is source-local, derived, and rebuildable. It may accelerate
an explicitly selected operation, but it does not become a second authority.
Private discovery controls authorized observation and next-action selection over
the DataView; it is not another persistent data layer.

The `union-graph` crate may remain the implementation name. `DataView` is the
public semantic name.

The DataView is valuable because a time-bounded research workflow can reuse
already observed, policy-compatible results across tasks while still preserving
source authority, freshness, evidence, and bounded execution. It is not a data
lake, a proactive replica, an exhaustive catalog, an existence oracle, or a
replacement for source-native truth.

## Fustor Ownership

Fustor owns:

- the deployment-global logical DataView head and its append-only revision,
  payload-version, evidence, invalidation, dependency, and graph-delta history;
- an empty genesis revision and opaque, non-enumerable public revision ids;
- complete bounded-action result payloads together with action identity,
  producer context, source authority, concrete native anchors, source revision,
  schema, policy labels, grants, observation time, freshness, derivation, and
  size/cost accounting;
- a source-specific result-normalizer registry that preserves complete payloads
  while deriving canonical DataView identities and relationship assertions;
- explicit `direct | projection-backed` observation-path validation and
  projection apply evidence without promoting projection revision to source
  revision;
- one-shot `BoundedSourceAction` admission for new results and a separate
  `StreamingSubscription` lease that can refresh, invalidate, or delete only
  already-tracked results;
- atomic evidence-gated update acceptance, safe merge of disjoint observations,
  conflict-version retention, and deterministic head selection;
- source-authority and freshness guards;
- existence non-commitment for unauthorized, invisible, filtered, and missing
  resources;
- `ScopedDataView` projection under tenant, principal, purpose, grants, policy,
  disclosure budget, discovery budget, freshness, and one selected immutable
  DataView revision;
- `latest` current-actionable projection and exact historical-replay projection,
  with current authorization reapplied and no persisted old scoped response;
- non-disclosing coverage states `complete-for-scope`, `partial`, and `unknown`;
- relationship lineage, evidence, freshness, and invalidation through scoped
  projection;
- bounded source actions, discovery budgets, and BFS-first frontier scheduling;
- stale-history retention, policy eviction, and compliance deletion semantics.

Fustor does not select a research conclusion, autonomously choose the next
action, or directly execute source runtimes. A caller selects and executes a
bounded action; Fustor validates the submitted result and returns a bounded,
policy-scoped projection.

Continuous source maintenance does not create new DataView objects. An event
whose source identity is outside the subscription's tracked result set remains
outside the DataView until a later explicit bounded action discovers it.

An exact historical projection pins source-derived DataView state for audit and
comparison, not authority. Fustor recomputes the scoped graph, coverage, and
frontier under the caller's current policy and request. Historical frontier
entries are replay-only: Fustor issues no executable action context, accepts no
branch update, performs no source work, and never restores revoked visibility.

## Source Authority Boundary

Each source remains the unique authority for its current source-native truth.
An optional SourceProjection proves only how far derived source-local state has
applied authoritative changes. DataView entries are source-derived observations.
Reuse is permitted only under current policy and freshness constraints, and
stale or invalidated observations become non-actionable before eviction.

Physical co-residence in the global DataView never grants visibility. A scoped
projection must not reveal hidden names, counts, relationships, evidence,
revision activity, errors, traces, or useful timing distinctions.
Unknown, unauthorized, evicted, and compliance-redacted exact revision refs are
therefore normalized to the same `revision-not-available` result.

## Upper-Layer Boundary

Upper-layer applications may own interaction sessions, lifecycle routes,
callbacks, CLI workflows, reusable experience packages, and caller decisions.
They may retain Fustor `DataViewRef` values and process-local `ScopedDataView`
values, but they must not create another durable global-view payload store,
reimplement Fustor merge policy, or reconstruct relationship semantics.

Agentic Map is one such optional upper layer. The dependency direction remains
one-way: Agentic Map may depend on Fustor contracts; Fustor must not depend on,
detect, or specialize itself for Agentic Map.

Interactive Agentic Map workflows may invoke generic Fustor commands through a
caller-owned thin addon executed by Coox. The addon transports requests and
responses only; validation, normalization, merge, retention, revisions, and
authoritative graph deltas remain Fustor-owned. High-frequency streaming bypasses
Coox, and upper layers observe a changed DataView head on a later read or
reprojection rather than through a Fustor callback.

Capanix remains an app-neutral event, context, deadline, grant, route-evidence,
and transport carrier. It does not own DataView or private-discovery semantics.

## Current Implementation Status

The repository implements the v0.4 reference path across Source, optional
SourceProjection, DataView, and the private-discovery control plane. It includes
the revisioned logical head, complete accepted-result retention, evidence-gated
atomic commit, conflict history, dependency invalidation, opaque latest/exact
projection, scoped completeness, tracked-result streaming, and compliance
lifecycle. The legacy unversioned Union Graph store remains only as an internal
compatibility and adapter-test surface.

PostgreSQL, MySQL, Elasticsearch, S3, Local FS, CSV, and JSONL expose versioned
bounded source envelopes and source-issued observation receipts through
app-neutral bound routes. Stateful database/PIT sessions are capacity-bounded
and actively expired; file scans enforce in-loop deadline, byte, traversal, and
symlink limits. Disposable real-service acceptance verifies the network Source
drivers, source-native revisions and accounting, Fustor-owned normalization,
and atomic DataView commit without using official stable services. Disposable
fs-meta `active-three` and `business-mini-nfs` staging likewise verifies the
real-NFS external-worker path without using official stable services.

The authoritative implementation sequence and acceptance ledger is
[`../TODO.md`](../TODO.md). Historical phase plans and local gate logs are not
research requirements and are intentionally not duplicated here.
