# Cortex State Machine & Transition Logic

## 1. Intent Analysis table

| Transition | Target State | Target Skill | Context / User Intent | Trigger Keywords |
| :--- | :--- | :--- | :--- | :--- |
| **T1** | S1 (Designing) | `architectural-design` | Unknowns, new requirements, ambiguity, refactoring. | "Design", "Plan", "Refactor", "New Feature", "Fix logic", "Clarify" |
| **T2** | S2 (Coding) | `code-implementation` | Knowns, implementation, execution, bug fixing. | "Start ticket", "Resume", "Implement", "Fix test", "Write code" |
| **T3** | S3 (Reviewing) | `code-review` | Second opinion, safety check, static analysis. | "Review this", "Check my work", "Is this safe?", "Logic check" |
| **T5** | S4 (Diagnosing) | `system-diagnosis` | Something is broken, flaky, or unknown failure. | "Diagnose", "Test failed", "Fix env", "Why is this erroring?" |

## 2. Startup Logic (Context Hydration)

### A. Pre-Flight Check (Self-Reflection)
1.  **Fetch**: Read `last_checked_commit` from `.agent/workstreams/reflection.json`.
2.  **Check Drift**: Run `git log {last_commit}..HEAD -- specs/` to see if "laws" have changed.
3.  **Verdict**:
    -   **Drift Detected**: FORCE T1 (`architectural-design`) to catch up.
    -   **No Drift**: Proceed to Branch Detection.

### B. Branch Detection (Fallback)
1.  **Main/Master**: Auto-activate **T1** (`architectural-design`).
2.  **Feature Branch**:
    -   Read `.agent/workstreams/{branch}/status.json`.
    -   Activate based on "persona" field.
    -   Default: Ask user.
