# Refactored Tests Review: All Packages

## 1. Executive Summary
This review covers the test suites across all packages in the monorepo following the V2 architecture refactoring (Sender/Pipeline). While the core logic and complex components (View-FS) are well-tested, there are **critical gaps** in the test coverage for the new HTTP and OpenAPI sender packages.

## 2. Package-by-Package Review

### 2.1 Core Package (`packages/core`)
- **Status**: ✅ **Passed**
- **Observations**:
    -   Refactoring from `PusherConfig`/`SyncConfig` to `SenderConfig`/`PipelineConfig` is correctly reflected in `test_config.py`.
    -   `test_mapper.py` and `test_mapper_compile.py` correctly test the new pipeline mapping logic.
    -   **Minor Issue**: Some variable names and comments in tests still refer to "pusher" (e.g., `test_app_config_add_get_delete_sender` comment says `# Add pusher`).

### 2.2 View-FS Package (`packages/view-fs`)
- **Status**: ✅ **Excellent**
- **Observations**:
    -   Comprehensive testing of consistency logic in `test_arbitrator.py`.
    -   Correctly uses V2 `MessageSource` enums (REALTIME, SNAPSHOT, AUDIT).
    -   Tests cover complex scenarios like "tombstone protection" and "integrity suspect" management.
    -   Logical clock tests are robust.

### 2.3 Sender Packages
#### `packages/sender-echo`
- **Status**: ⚠️ **Acceptable with Debt**
- **Observations**:
    -   Renamed from `pusher-echo`.
    -   Tests exist and verify functionality.
    -   **Debt**: Heavy use of "pusher" terminology in docstrings and comments (e.g., `test_snapshot_trigger_once_and_only_once` docstring).
    -   **Logic**: Verifies `snapshot_needed` behavior correctly.

#### `packages/sender-http`
- **Status**: ❌ **CRITICAL FAILURE**
- **Observations**:
    -   **NO TESTS FOUND**. The directory `packages/sender-http/tests` does not exist.
    -   This package appears to be the primary implementation for HTTP transport, but it lacks any verification.

#### `packages/sender-openapi`
- **Status**: ❌ **CRITICAL FAILURE**
- **Observations**:
    -   **NO TESTS FOUND**. The directory `packages/sender-openapi/tests` does not exist.

### 2.4 Schema Packages (`packages/schema-fs`)
- **Status**: ✅ **Passed**
- **Observations**:
    -   Tests `test_models.py` verify Pydantic validation rules.
    -   Handles backward compatibility aliases (`file_path` -> `path`) correctly.

### 2.5 Source Packages (`packages/source-*`)
- **Status**: ✅ **Passed**
- **Observations**:
    -   `source-fs` has extensive legacy and new tests.
    -   Validation of source drivers remains robust.

## 3. Global Issues & Recommendations

### 3.1 Critical Gaps (Must Fix)
1.  **Add Tests for `sender-http`**: Create basic unit tests to verify header construction, payload formatting, and error handling.
2.  **Add Tests for `sender-openapi`**: Verify OpenAPI schema generation or client behavior.

### 3.2 Terminology Cleanup
1.  Run a global search-and-replace (carefully) on test files to update comments from "pusher/sync" to "sender/pipeline" to reduce cognitive load for future maintainers.

### 3.3 Test Pollution
- **Finding**: No significant test pollution was observed. The new adapter patterns in `agent` keep concerns separated.

## 4. Action Plan (TODO)
- [ ] Create `packages/sender-http/tests` and add initial unit tests.
- [ ] Create `packages/sender-openapi/tests` and add initial unit tests.
- [ ] Cleanup "pusher" terminology in `packages/sender-echo/tests`.
- [ ] Cleanup "pusher" terminology in `packages/core/tests`.
