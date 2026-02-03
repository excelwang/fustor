# 99 - Environment Specification (System Laws)

> **Status**: Draft Requirements
> **Version**: 1.0

## 1. Operating Environment
- **OS**: Linux (Ubuntu 22.04+ recommended)
- **Shell**: zsh / bash

## 2. Toolchain Requirements (Mandatory)
- **Git**: 2.34+
- **Python**: 3.10+ (with `uv` or `pip`)
- **Node.js**: 18+ (for web-apps)

## 3. Directory Structure (Laws)
- **`.agent/workstreams/`**: MUST exist for runtime memory.
- **`specs/`**: MUST contain the system "Laws".
- **`skills/`**: MUST contain the persona definitions.

## 4. Environment Variables
- `APP_DIR`: Root path of the project.
- `STORAGE_DIR`: Path for temporary artifacts.

## 5. Verification Command
- `pytest tests/env_check.py` (Draft)
