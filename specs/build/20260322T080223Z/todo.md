# Fix Gate Todo

Run: `20260322T080223Z`

Scope:
- `SPEC_DRIFT.FORMAL_LAYER_BOUNDARY_GOVERNANCE_DETAIL_LEAK`

Completed:
- Remove exact helper-crate chains, package names, source-file paths, bootstrap envelope names, runtime wrapper API names, and env-var knob names from formal L1-L3 specs.
- Keep only domain/runtime ownership, externally relevant package boundaries, and behaviorally meaningful interaction seams in formal specs.
- Move exact governance and implementation detail into `fs-meta/docs/ENGINEERING_GOVERNANCE.md`.
- Update repo-local validation/test adapters so the gate validates the rewritten formal wording instead of the superseded implementation-detail wording.

Validation:
- `./scripts/validate.py specs/`
- `./scripts/test-workflow.sh contracts`

Validation Results:
- `./scripts/validate.py specs/` -> passed
- `./scripts/test-workflow.sh contracts` -> passed
