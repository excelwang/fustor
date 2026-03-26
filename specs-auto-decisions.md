# Specs Auto Decisions

This file records the standing policy for automatic spec decisions during
debugging and repair work in this repository.

## Decision Authority

- Higher-order specs override lower-order specs.
- Earlier specs override later specs.
- `L0` and `L1` are authoritative over `L2` and `L3`.
- When two specs conflict, the lower-level or later spec must be rewritten to
  match the higher-level or earlier spec.

## Legacy Policy

- Do not preserve legacy compatibility just because an older implementation or
  lower-level spec says something different.
- If a lower-level or later spec encodes legacy behavior that conflicts with
  higher-order authority, remove or rewrite the legacy rule.

## Execution Rule

- If debugging proves that a spec change is necessary, make the decision and
  execute the spec update directly.
- Do not pause to ask for approval once the conflict is established.
- Keep the spec update coherent: fix all affected lower-level or later specs in
  the same direction.

## Bug-Fix Interaction

- Specs remain authority during bug-fix work unless a proven conflict requires
  correction.
- When a spec correction is required, update the spec first according to the
  authority rules above, then continue the repair loop from the corrected
  contract.
