# System Diagnosis Maintenance Guide

This document contains detailed instructions for maintaining the health of the Agentic Cortex environment.

## 1. Workstream Garbage Collection
Zombie workstreams can accumulate over time. Use the `gc_workstreams.py` script to clean them up.

### Command Syntax
```bash
python3 scripts/gc_workstreams.py [options]
```

### Options
- `--apply`: Actually perform the deletion. Without this flag, the script performs a Dry Run.
- `--older-than DAYS`: Only cleanup workstreams older than X days (default: 7).
- `--force`: Cleanup even if files are uncommitted.

## 2. Environment Verification
Always verify compliance with `references/environment_spec.md` when diagnosing persistent failures.
