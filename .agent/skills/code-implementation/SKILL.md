---
name: code-implementation
description: Implement code, clean tests, and manage the "Code-Test-Review" loop. Use when user says "Start ticket", "Implement this", "Fix bug", or "Resume work". Accesses the Executor Persona.
---

# Code Implementation (Executor)

## Instructions

### 1. Workstream Initialization (The Handshake)
- **Goal**: unique git branch for every ticket.
- **Action**:
    1. Check `tickets/active/` for assigned ticket (Get `TICKET_ID`).
    2. Check current branch: `git branch --show-current`.
    3. **CRITICAL**: If branch != `feat/{TICKET_ID}`:
        - `git checkout -b feat/{TICKET_ID}`
        - **DO NOT** work on `master` or `main`.

### 2. Workstream Management
- **Initialization**:
    1. Check `tickets/active/` for assigned ticket.
    2. Create `.agent/workstreams/{branch}/` context if missing.
    3. Copy Ticket content to local context.
- **Persistence**: Update `.agent/workstreams/{branch}/ticket.md` at every key step.

### 3. The Implementation Loop (D-C-R)
> **Goal**: Pass the Review (S3).

1.  **Alignment**: Read `specs/` (The Law) and `tickets/active/` (The Task).
    - **Self-Correction**: If you find Spec ambiguities or logical gaps, **STOP** and switch back to `architectural-design` to update the specs. **Do not guess.**
2.  **Coding (S2)**:
    - Write Code & Unit Tests.
    - **Rule**: Commit logical chunks. Follow `references/git_protocol.md`.
3.  **Testing**:
    - Run Contract Tests (Expect Pass).
    - Run Unit Tests.
4.  **Review (S3)**:
    - **Self-Review Trigger**: Call `code-review` skill.
    - **Input**: `git diff origin/master...HEAD`.
5.  **Decision**:
    - **Fail**: Fix issues -> Go to Step 2.
    - **Pass**: Proceed to Merge & Release.

### 4. Merge & Release
1.  **Cleanup**: Delete `.agent/workstreams/{branch}/`.
2.  **Release**: Move ticket to `tickets/done/`.
3.  **Push**: `git push` and create PR.

### 5. Reflection (Post-Task)
- **Goal**: Capture lessons, patterns, and corrections to improve future performance.
- **Trigger**: At the end of every conversation or significant task completion.
- **Action**:
    1. Review the interaction for valuable insights.
    2. If a new lesson is found:
        - Create a new file `references/LESSON_{Topic}.md` using `references/REFLECTION_TEMPLATE.md`.
        - OR append to an existing relevant lesson file.
    3. Update `specs/` if "laws" were clarified.

### 6. Identity Banner
> **Rule (MANDATORY)**: After "Hi Cortex", EVERY single response in this state MUST start with:
```markdown
> **Cortex Status**: S2 (Coding)
> **Workstream**: $wk-current
> **Persona**: 👷 Executor (Workflow Manager)
> **Ticket**: [Current Ticket ID]
> **Branch**: [Current Branch Name]
```

## References
- **Context Template**: `references/CURRENT_TICKET_TEMPLATE.md`
- **Reflection Template**: `references/REFLECTION_TEMPLATE.md`
- **Git Protocol**: `references/git_protocol.md` (Strict adherence required)

