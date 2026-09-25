# Fustor Formal Specs

This directory is the formal specification tree for the caller-agnostic Fustor
data tooling boundary. Product-specific fs-meta specs remain under
`fs-meta/specs/`.

## Formal Entry Points

1. [L0-VISION.md](./L0-VISION.md): user value and exclusions for heterogeneous
   sources including peer read-only graph sources, optional source-local
   acceleration, global DataView, bounded declared external results, opaque
   revision identity, revision-pinned read-only projection, private discovery,
   and boundaries.
2. [L1-CONTRACTS.md](./L1-CONTRACTS.md): testable Source, SourceProjection, DataView, Private Discovery, boundary, and non-goal contracts.
3. [L2-ARCHITECTURE.md](./L2-ARCHITECTURE.md): the only L2 file; defines passive components, ownership, interactions, and dependency direction.
4. [L3-RUNTIME/interfaces.md](./L3-RUNTIME/interfaces.md): public source action,
   declared-result admission, projection, streaming, commit, DataView, and
   scoped-view interfaces.
5. [L3-RUNTIME/algorithms.md](./L3-RUNTIME/algorithms.md): deterministic commit, normalization, streaming, head selection, projection, invalidation, and retention algorithms.
6. [L3-RUNTIME/workflows.md](./L3-RUNTIME/workflows.md): one-shot,
   projection-backed, streaming, latest/exact scoped projection, pull-refresh,
   and recovery workflows.
7. [L3-RUNTIME/decisions.md](./L3-RUNTIME/decisions.md): authority, optional
   projection, demand materialization, normalization, opaque revisions,
   historical replay, orchestration, caller visibility, carrier, and extensibility decisions.
8. [L3-RUNTIME/PASSIVE_OPERATIONS.md](./L3-RUNTIME/PASSIVE_OPERATIONS.md): native
   one-shot validation/reads, configuration-use evidence and caller-managed
   configuration observation, including first registration with no business inventory.

## Traceability

- L0 states why the capability matters without naming implementation packages or upper-layer applications.
- Every L1 contract has Responsibility, Verification, and `Covers L0` metadata.
- L2 defines components by input, output, and boundary rather than class or crate layout.
- L3 defines runtime interfaces, algorithms, workflows, and decisions.
- New v0.4 contracts remain logic-level until source implementation and end-to-end tests prove system conformance.

## Agent-first integration boundary

The 2026-09-12 alignment leaves L0 unchanged. New console use is still ordinary
caller-neutral tooling: proposed native configuration -> validation -> explicit
bounded read -> original evidence. Business registries, machine authentication,
instruction history, package selection and workflow execution remain outside
Fustor. No source provisioning, ETL, Agent models or artifact-based instance
deployment is added. Agentic Map's AF-02/AF-06 integration acceptance is pending;
existing native tests and annotations do not certify the revised caller path.
