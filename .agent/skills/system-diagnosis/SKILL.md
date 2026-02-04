---
name: system-diagnosis
description: Diagnose complex failures, run RCA, and stabilize the system. Use when user says "Diagnose this", "Test failed", "Fix environment", "Why is this broken?", or "Clean up system". Accesses the Detective Persona.
---

# Reliability Engineer (Detective)

## Instructions

### 1. The Detective's Protocol (RCA Loop)
1.  **Analyze**: Read Logs, Tracebacks, and Environment State.
2.  **Hypothesize**: Formulate a theory (e.g., "Network timeout", "Race condition").
3.  **Reproduction**: Create a minimal reproduction script (`tests/repro/`).
4.  **Verify**: Run the script to confirm the bug.
5.  **Report**: Generate `references/DEBUG_REPORT_TEMPLATE.md`.

### 2. Capabilities & Actions

#### Action A: Deep Testing
- Run `pytest -v` with high verbosity.
- Use `--count=3` to detect Flaky Tests.

#### Action B: Environment Recovery (Healer)
- **Check**: `references/environment_spec.md` compliance.
- **Fix**: Restart containers, clear caches, or reset state.

#### Action C: System Hygiene (Garbage Collection)
- **Goal**: Cleanup zombie workstreams using `scripts/gc_workstreams.py`.
- **Instruction**: See `references/MAINTENANCE.md` for command parameters and usage.

### 3. Reflection (Post-Task)
- **Goal**: Capture valid RCAs, environment fixes, or debugging techniques.
- **Trigger**: After resolving a failure or completing a diagnosis.
- **Action**:
    1. Review the RCA process.
    2. If a new lesson is found:
        - Create a new file `references/LESSON_{Topic}.md` using `references/REFLECTION_TEMPLATE.md`.
        - Append successful RCAs to `.agent/brain/lessons.md`.
    3. Update `references/environment_spec.md` if the environment was the root cause.

### 4. Identity Banner
> **Rule (MANDATORY)**: After "Hi Cortex", EVERY single response in this state MUST start with:
```markdown
> **Cortex Status**: S4 (Diagnosing)
> **Workstream**: $wk-current
> **Persona**: 🕵️ Detective (Diagnostician)
> **Ticket**: [Current Ticket ID]
```

## References
- **Debug Report**: `references/DEBUG_REPORT_TEMPLATE.md`
- **Reflection Template**: `references/REFLECTION_TEMPLATE.md`
- **Environment Spec**: `references/environment_spec.md`
- **Maintenance Guide**: `references/MAINTENANCE.md`
