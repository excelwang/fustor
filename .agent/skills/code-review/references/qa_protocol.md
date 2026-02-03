# 20 - Quality Assurance (The Judiciary Law)

> **Status**: Draft Requirements
> **Version**: 1.0

## 1. Core Review Checklist
The "Judge" (S3) enforces these criteria.

-   **Correctness**: Does implementation match Spec?
-   **Security**: No injections, leaks, or unsafe inputs.
-   **Edges**: Handled NULLs, Timeouts, Race Conditions.
-   **Style**: Clean, idiomatic, follows project norms.
-   **Tests**: No pollution; assertions are valid.

## 2. Review Modes
Different tasks require different scrutiny lenses.

### Mode A: Feature Review (Design Compliance)
-   **Use Case**: New Features, Bugfixes.
-   **Goal**: Ensure code implements the "spirit and letter" of the Spec.
-   **Focus**: Logic correctness, API alignment.

### Mode B: Refactor Review (Feature Parity)
-   **Use Case**: Refactoring, Migrations.
-   **Goal**: Zero functional change.
-   **Protocol**:
    1.  Read Legacy Code (Old Branch).
    2.  Read New Code (New Branch).
    3.  **Assert**: Behavior is identical.

### Mode C: Test Review (Integrity)
-   **Use Case**: CI/CD updates, Test suite changes.
-   **Focus**:
    -   **Determinism**: No `sleep()`. Use `wait_for_condition`.
    -   **Pollution**: Tests must not alter global state permanently.

## 3. Test Strategy
-   **Contract Tests**: `tests/contracts`. Owned by Architect. Checks Spec compliance.
-   **Unit Tests**: `tests/unit`. Owned by Executor. Checks internal logic.
