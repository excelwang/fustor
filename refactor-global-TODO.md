# Global Refactoring TODO

## Terminology Cleanup (Pusher -> Sender)
- [x] Scan codebase for "pusher" and "Pusher" (case insensitive) and remove/replace legacy terms. (Found usage in docs, tests, and legacy configs) <!-- id: 1 -->
  - [x] Update `packages/sender-http/pyproject.toml` (remove legacy entry point) <!-- id: 4 -->
  - [x] Standardize `packages/sender-echo/pyproject.toml` <!-- id: 9 -->
  - [x] Update `agent/README.md` and docs <!-- id: 3 -->
  - [x] Update `agent/tests/conftest.py` <!-- id: 2 -->

## Verification
- [x] Run tests for `packages/sender-echo` <!-- id: 5 -->
- [x] Run tests for `packages/sender-http` <!-- id: 6 -->
- [x] Run tests for `packages/sender-openapi` <!-- id: 7 -->
- [x] Run tests for `agent/` <!-- id: 8 -->
