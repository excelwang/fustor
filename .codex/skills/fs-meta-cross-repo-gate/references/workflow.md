# Fs-meta Cross-Repo Workflow

## Scope

Use this workflow for active `fs-meta` cross-repo blockers where `fustor` and `capanix` share responsibility.

The blocker-state authority is one shared document.

In the current local environment, that document defaults to:

- `/root/repo/capanix/todo.md`

Do not overrule that document from memory. Re-read it before every new iteration.

## Governing Specs

Read the spec files that govern the current blocker line before changing code.

For query, materialization, sink, projection, or readiness blockers, start from:

- `/root/repo/fustor/fs-meta/specs/L1-CONTRACTS.md`
- `/root/repo/fustor/fs-meta/specs/L2-ARCHITECTURE.md`
- `/root/repo/fustor/fs-meta/specs/L3-RUNTIME/API_HTTP.md`
- `/root/repo/fustor/fs-meta/specs/L3-RUNTIME/WORKFLOWS.md`

Then narrow to the specific sections named by the blocker-state document or raw failing boundary evidence.

## Validation Order

Use this order unless the blocker-state document proves a narrower sequence:

1. Reproduce the narrowest preserved failing case first.
2. If a repo-local `fustor` bug is identified, add or update the owning red test first and prove it fails.
3. Fix the repo-local defect.
4. Re-run the owning test and directly impacted repo-local verification.
5. Re-run the preserved downstream case that localized the blocker.
6. Only after the preserved case passes, broaden to larger ignored or end-to-end suites.

When a preserved downstream case depends on sibling-repo binaries or environment variables, read the exact invocation from the blocker-state document instead of hardcoding it into this skill.

## Fustor-Side Discipline

- Keep repo-local `fustor` fixes separate from downstream claims about `capanix`.
- If a `fustor` repo-local bug is identified, add or update the owning red test first, prove it fails, then fix, then run impacted verification.
- Capture raw unretried evidence before claiming the boundary moved back to `capanix`.
- Do not update `/root/repo/capanix/todo.md` unless the new state is supported by first-boundary evidence.

## Acceptance

Treat the blocker as closed only when all of these are true:

- the preserved blocker-localizing case passes
- repo-local verification for the owning seam passes
- the blocker-state document no longer shows an unresolved first raw failing boundary on `fustor`

Only after that should broader downstream or end-to-end validation run.
