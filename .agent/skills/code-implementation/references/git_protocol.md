# 30 - Git Protocol (The Version Control Law)

> **Status**: Draft Requirements
> **Version**: 1.0

## 1. Workstream Lifecycle & Isolation
The system uses **Context Isolation** to manage concurrent tasks.

-   **Physical Location**: `.agent/workstreams/{branch_name}/`.
-   **Lifecycle**:
    1.  **Birth**: Created when a Ticket is claimed and a branch is active.
    2.  **Life**: Stores `ticket.md` and `status.json` for context persistence.
    3.  **Death**: **MUST** be deleted (`rm -rf`) before merging to `master` to prevent pollution.

## 2. Atomic Ticket Locking Protocol (Lock/Unlock)
To prevent race conditions, the `tickets/` directory acts as a mutex, managed on the `tracking` branch.

### A. Claiming (The Lock)
1.  **Sync**: `git checkout tracking && git pull origin tracking`.
2.  **Move**: `mv tickets/backlog/TICKET_ID.md tickets/active/TICKET_ID.md`.
3.  **Commit**: `git commit -m "feat(ticket): claim TICKET_ID"`.
4.  **Push**: `git push origin tracking`.
5.  **Bootstrap**: Switch to `feature/TICKET_ID` and initialize `.agent/workstreams/`.

### B. Releasing (The Unlock)
1.  **Sync**: `git checkout tracking && git pull origin tracking`.
2.  **Move**: `mv tickets/active/TICKET_ID.md tickets/done/TICKET_ID.md`.
3.  **Commit**: `git commit -m "feat(ticket): complete TICKET_ID"`.
4.  **Push**: `git push origin tracking`.

## 3. Semantic Commit Protocol
Cortex uses commit messages for Self-Reflection. They must be machine-readable.

-   **Format**: `type(scope): subject`
-   **Body Requirement**:
    > **Rule**: The body **MUST** reference the **Ticket ID** and the **Spec File/Section** being implemented.
-   **Example**:
    ```text
    feat(auth): implement session timeout (TICKET-101)

    - Implements specs/10-DOMAIN_AUTH.md Section 3.2 (Timeout Logic).
    ```
