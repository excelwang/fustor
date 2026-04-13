---
version: 1.0.0
---

# L3 Governance: Test Code Organization

> Focus: formalize fs-meta test topology so owner-layer red tests stay local, downstream harness checks stay separate, and giant mixed production/test files stop absorbing unrelated seam families.

## [decision] OwnerLayerRedFirst

1. Every fs-meta bug fix MUST first gain or update an owning-layer automated test at the first raw failing boundary before downstream e2e, authority, or cross-repo validation is tightened.
2. Downstream matrix, operational, release-upgrade, and activation-scope scenarios MUST remain downstream confirmation layers; they are not the primary owner seam unless the raw failing boundary is the helper/oracle/test surface itself.
3. Helper/oracle bugs in e2e support code MUST be proven with helper-owned red tests in the helper file or helper test module, not papered over by production code changes.
4. Owner-layer tests for `runtime_app`, `query/api`, `workers/source`, `workers/sink`, `source/mod`, and `sink/mod` MUST stay separate from downstream matrix/operational scenario assertions.

## [decision] InlineTestColocationDiscipline

1. Inline `mod tests` is acceptable only for tightly local seam families that need private visibility from one owner file.
2. Once one file accumulates multiple unrelated seam families, new tests MUST be placed in extracted sibling test modules or spec suites grouped by seam rather than continuing one omnibus inline block.
3. Once a file exceeds either `2000` lines of production logic before `mod tests` or `2000` lines of inline tests after `mod tests`, it becomes a decomposition candidate and new unrelated test families MUST NOT be added inline.
4. Once a file exceeds `4000` total lines, new tests MAY remain inline only for a private local helper seam that would otherwise force widening production visibility solely for testing.
5. Downstream scenario helpers and oracles MUST NOT keep growing inline compatibility branches once a formal owner seam exists in production code or helper code.

## [decision] ProductionAndVerificationSeparation

1. Product runtime files SHOULD align to one dominant owner seam family so that fixes can be localized without re-reading unrelated source/sink/query/facade state machines.
2. Verification files SHOULD align to one dominant scenario family so that failing output identifies one owner-cut at a time.
3. When a production file is already oversized, extracting new test families takes priority over extending its inline `mod tests`.
4. Blind waits and dead waits MUST NOT be used to stabilize oversized mixed-responsibility files; the canonical repair is state-based readiness plus decomposition by seam family.

## [decision] CurrentFsMetaMigrationPriority

1. The current highest-priority fs-meta migration hotspots are:
   - `fs-meta/app/src/runtime_app.rs`
   - `fs-meta/app/src/query/api.rs`
   - `fs-meta/app/src/workers/source.rs`
   - `fs-meta/app/src/workers/sink.rs`
2. New test families for those files SHOULD be extracted into sibling test modules or targeted spec suites unless the seam is unavoidably private and temporary inline placement is the only way to prove the first raw boundary.
3. Existing inline tests in those files are tolerated as legacy debt, but they do not authorize further omnibus growth.

## [decision] ScenarioAndHelperPlacement

1. Matrix-family expectations belong in `tests/e2e/test_http_api_matrix.rs` only when they verify matrix scenario behavior; once the failure localizes to production owner code, the first new red MUST move into the owning production module.
2. Operational and release-upgrade scenarios belong in their respective e2e files only when they verify scenario composition, not when they stand in for owner-layer recovery or routing contracts.
3. Helper/oracle tests belong with the helper they prove, such as matrix oracle, cluster helper, or API client helper surfaces, and MUST NOT be re-expressed as production semantics.
4. Query aggregation, PIT settle, stats rescue, fixed-bind publication, source recovery, sink readiness, and retained-ready restoration each count as separate owner seam families and SHOULD gain separate extracted test groupings as they are touched.
