# Fix Gate Todo

Run: `20260322T082710Z`

Scope:
- `SPEC_DRIFT.FORMAL_LAYER_BOUNDARY_GOVERNANCE_DETAIL_LEAK`

Completed:
- Remove residual upstream package, source-path, local-dev binary, and runtime-overlay field-name detail from formal L1/L3 specs.
- Keep the formal tree at ownership and behavior-seam level while leaving exact identifiers to engineering governance or executable tests.

Validation:
- `./scripts/validate.py specs/`
- `./scripts/test-workflow.sh contracts`

Validation Results:
- `./scripts/validate.py specs/` -> exit 0 (traceability warnings only)
- `./scripts/test-workflow.sh contracts` -> exit 0
