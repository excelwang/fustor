# Fustor Monorepo Refactoring Plan

This document outlines the phased plan for refactoring the `fuagent` and `fustor` projects into a unified `fustor` monorepo.

## Overall Goal

To refactor the `fuagent` and `fustor` projects into a unified `fustor` monorepo, following a three-phase plan, with each phase resulting in a runnable system. The monorepo will house top-level services, internal shared libraries, and plugin packages.

### Key Components:
- **Top-level Services**: `registry` (Control Plane), `agent` (Edge Collector), `fusion` (Data Processing & View Engine).
- **Internal Shared Libraries**: `fustor-common`, `fustor-event-model`, `fustor-agent-sdk`, `fustor-fusion-sdk`, `fustor-registry-client`.
- **Plugin Packages**: `packages/sources/*`, `packages/pushers/*`, `packages/parsers/*`.
- **Unified CLI**: A single `fustor` command with subcommands (`fustor agent`, `fustor registry`, `fustor fusion`).
- **Tooling**: `uv` for package management.

## Phase 1: Physical Structure Migration (Completed)

*   [x] Create new monorepo structure.
    *   `fustor_monorepo/` directory with `agent/src`, `fusion/src`, `registry/src`, `packages/core`, `packages/pusher_echo`, `packages/pusher_openapi`, `packages/source_elasticsearch`, `packages/source_fs`, `packages/source_mysql`, `packages/web_ui` subdirectories.
*   [x] Migrate `fuagent` code and tests into `fustor_monorepo/agent` and relevant `packages/`.
*   [x] Migrate `fustor` code and tests into `fustor_monorepo/fusion`, `fustor_monorepo/registry` and relevant `packages/`.
*   [x] Rename and update imports.
    *   All Python and TOML files have undergone find-and-replace for package renaming (e.g., `fuagent` to `fustor_agent`, `ingestor_service` to `fustor_fusion`).
*   [x] Configure `pyproject.toml` files.
    *   Individual `pyproject.toml` files for `agent`, `fusion`, `registry`, and all packages under `fustor_monorepo/packages/` have been created/configured.
    *   Added `[build-system]` table and `[tool.setuptools.packages.find]` to all `pyproject.toml` files for proper editable installation.
*   [x] Verification: Resolve `uv` virtual environment conflict during package installation and service execution.
    *   [x] Removed `fustor_monorepo/.venv` to ensure clean install.
    *   [x] Renamed `fuagent_*` source directories to `fustor_*` within `packages/*/src/` (e.g., `packages/core/src/fuagent_core` to `packages/core/src/fustor_core`).
    *   [x] Re-ran `uv sync` to install all external dependencies and link local packages in editable mode.
    *   [x] Fixed multiple `ImportError: attempted relative import beyond top-level package` errors in `fustor-fusion` by converting relative imports to absolute imports.
    *   [x] Ran individual services (`fustor-agent`, `fustor-fusion`, `fustor-registry`) using direct module invocation (`python -m ...`) to verify functionality.

## Phase 2: Unified CLI (To Do)

*   **Goal**: Create a single, unified `fustor` command-line interface to manage all services and tools within the monorepo.
*   **Tasks**:
    *   Design the top-level `fustor` CLI entry point (e.g., `fustor agent`, `fustor registry`, `fustor fusion`).
    *   Integrate existing CLI functionalities from `fustor-agent`, `fustor-fusion`, and `fustor-registry` as subcommands.
    *   Ensure consistent argument parsing, help messages, and error handling across all subcommands.
    *   Update `pyproject.toml` files to reflect the new CLI entry points.
    *   Develop a clear strategy for managing CLI dependencies.
    *   Update documentation for the new unified CLI.

## Phase 3: Deep Refactoring (To Do)

*   **Goal**: Optimize code structure, reduce redundancy, and enhance maintainability through shared libraries and architectural improvements.
*   **Tasks**:
    *   **Identify and Extract Shared Components**:
        *   Extract common models, utilities, and helper functions into `fustor-common`.
        *   Define and centralize event structures and schemas in `fustor-event-model`.
        *   Create `fustor-agent-sdk` for common agent-specific logic and interfaces.
        *   Create `fustor-fusion-sdk` for common fusion-specific logic and interfaces.
        *   Create `fustor-registry-client` for simplified interaction with the registry service.
    *   **Refactor Existing Code**:
        *   Modify `agent`, `fusion`, and `registry` services to utilize the newly created shared libraries and SDKs.
        *   Refactor plugin packages (`packages/sources/*`, `packages/pushers/*`, `packages/parsers/*`) to conform to standardized interfaces defined in the SDKs.
    *   **Review and Optimize Data Models**:
        *   Ensure consistency and efficiency of data models across all services and libraries.
        *   Implement data validation and serialization best practices.
    *   **Testing and Quality Assurance**:
        *   Implement comprehensive unit and integration tests for all refactored components and new shared libraries.
        *   Set up CI/CD pipelines to enforce code quality, linting, and type-checking (e.g., `ruff check .`, `mypy .`).
    *   **Performance Optimization**:
        *   Identify and address performance bottlenecks.
        *   Optimize resource utilization (CPU, memory, I/O).
    *   **Documentation**: Update all relevant documentation, including API references, developer guides, and architectural overviews.