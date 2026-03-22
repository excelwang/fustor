# Auto Decisions

## Decision 1
Timestamp: 2026-03-22T08:02:23Z
Context: Triage released `SPEC_DRIFT.FORMAL_LAYER_BOUNDARY_GOVERNANCE_DETAIL_LEAK`, which requires formal L1-L3 specs to stop carrying governance, package, file-path, env-var, and bootstrap-helper detail.
Options considered:
- Leave the exact helper/package/file/env details in the formal specs and add more disclaimers.
- Delete the exact details from the formal specs without preserving them elsewhere.
- Move the exact details into engineering-governance material and rewrite the formal specs at the ownership and behavior-seam level.
Chosen option: Move the exact details into `fs-meta/docs/ENGINEERING_GOVERNANCE.md` and keep the formal specs focused on ownership, runtime behavior, and externally relevant seams.
Rationale: This matches the formal-scope boundary already declared in `fs-meta/specs/readme.md` and `L0-VISION.md` while preserving auditability for maintainers.
Affected: `/root/repo/fustor/fs-meta/docs/ENGINEERING_GOVERNANCE.md`, `/root/repo/fustor/fs-meta/specs/L1-CONTRACTS.md`, `/root/repo/fustor/fs-meta/specs/L2-ARCHITECTURE.md`, `/root/repo/fustor/fs-meta/specs/L3-RUNTIME/OBSERVATION_CUTOVER.md`, `/root/repo/fustor/fs-meta/specs/L3-RUNTIME/WORKER_RUNTIME_SUPPORT.md`, `/root/repo/fustor/fs-meta/specs/L3-RUNTIME/WORKFLOWS.md`

## Decision 2
Timestamp: 2026-03-22T08:02:23Z
Context: The repo-local `validate_specs_tree.py` adapter assumed the upstream validator always passed L1 item metadata as a dict, but the current upstream validator now passes a set during the testable-contract check, which broke `./scripts/validate.py specs/`.
Options considered:
- Leave the adapter unchanged and bypass the repo-local validator entrypoint.
- Rework the repository back to section-level L1 testability only.
- Make the adapter accept both call shapes while preserving item-level `CONTRACTS.<SECTION>.<ITEM>` testability.
Chosen option: Make the adapter robust to both dict and set inputs and keep item-level contract IDs testable.
Rationale: Validation is a gate requirement, and the repository still needs item-level L1 coverage semantics after the formal wording cleanup.
Affected: `/root/repo/fustor/fs-meta/scripts/validate_specs_tree.py`

## Decision 3
Timestamp: 2026-03-22T08:02:23Z
Context: The rewritten formal specs intentionally removed exact helper-crate, file-path, env-var, and bootstrap-envelope wording, which caused contract tests to keep asserting superseded strings instead of the new seam-level language plus engineering-governance handoff.
Options considered:
- Reintroduce the old exact implementation wording into the formal specs just to satisfy existing tests.
- Drop the affected tests from the contract workflow.
- Update the tests so they assert the new formal seam language and verify the moved exact details under `ENGINEERING_GOVERNANCE.md` where appropriate.
Chosen option: Update the tests to match the new formal-scope boundary and check governance material for the exact implementation details that were intentionally moved out of L1-L3.
Rationale: This preserves the released defect fix instead of weakening it to satisfy stale assertions, while keeping the verification chain intact.
Affected: `/root/repo/fustor/fs-meta/tests/specs/test_contracts_fs_meta_domain.rs`, `/root/repo/fustor/fs-meta/tests/app_specs/test_contracts_module_boundary.rs`, `/root/repo/fustor/fs-meta/tests/cli_specs/test_contracts_scope.rs`
