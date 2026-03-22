# Auto Decisions

## Decision 1
Timestamp: 2026-03-22T08:27:10Z
Context: Triage accepted most of the formal-scope cleanup but released a follow-up batch because a few exact identifiers still remained in the formal tree: an upstream config package name, an app source-path glob, a concrete local-dev binary/package pair, a runtime-overlay field name, and concrete config/daemon surfaces in the API spec.
Options considered:
- Leave these exact identifiers in the formal tree because they are already fewer than before.
- Remove them from the formal tree and stop checking them anywhere.
- Remove them from the formal tree while keeping any needed exact identifiers in engineering governance or executable tests.
Chosen option: Remove the remaining exact identifiers from the formal tree and keep exact implementation naming only in governance material or tests.
Rationale: The released defect is specifically about finishing the governance/formal-scope split. Keeping even a small tail of exact implementation identifiers in L1/L3 would just trigger the same drift again.
Affected: `/root/repo/fustor/fs-meta/specs/L1-CONTRACTS.md`, `/root/repo/fustor/fs-meta/specs/L3-RUNTIME/WORKFLOWS.md`, `/root/repo/fustor/fs-meta/specs/L3-RUNTIME/API_HTTP.md`
