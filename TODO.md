# TODO

## fs-meta / capanix Boundary Follow-ups

- Evaluate lifting `fs-meta/app/src/runtime/unit_gate.rs` into a generic `capanix` runtime helper. It is the highest-value reusable mechanism: unit allowlist, activate/deactivate/tick acceptance, generation fencing, and active-scope merge. Keep it out of default SDK authoring surfaces unless `capanix` explicitly blesses a runtime-unit control cache/helper layer.
- Consider splitting the generic shell out of facade pending activation in `fs-meta/app/src/runtime_app.rs`: pending record, retry scheduling, retry error bookkeeping, and exposure-confirmed cutover tracking. Keep `observation_eligible` gating and facade-specific cutover semantics in `fs-meta`.
- Keep query observation evidence assembly in `fs-meta`. `capanix-managed-state-sdk` already owns the shared evaluator; `fs-meta` should continue owning candidate-group selection, degraded/overflow evidence shaping, and source/sink status merge policy.
- Keep source planner and sink runtime-group policy in `fs-meta`. Logical-root fanout, source-primary selection, candidate/draining handoff state, and host-grant-to-runtime-group reconciliation are product semantics, not generic platform policy.
