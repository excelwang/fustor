# fustor-core

This package contains core components, abstractions, and utilities shared across the Fustor ecosystem. It serves as the foundation for both the Agent and Fusion services, as well as all driver extensions.

## Key Components

*   **`pipeline/`**: Core abstractions for the V2 Pipeline architecture (Handlers, Mappers, Context).
*   **`transport/`**: Protocol-agnostic interfaces for `Sender` and `Receiver`.
*   **`common/`**:
    *   `logging_config.py`: Standardized, decoupled logging setup.
    *   `metrics.py`: Unified metrics interface (Counter, Gauge, Histogram).
*   **`models/`**: Pydantic models for `PipelineConfig`, `EventBase`, and system states.
*   **`clock/`**: Logical and Hybrid Clock implementations for data consistency.
*   **`drivers.py`**: Base classes for `SourceDriver` and `SenderDriver`.
*   **`exceptions.py`**: Standardized exception hierarchy for Fustor.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

`fustor-core` is the dependency root for almost all other packages in the monorepo. It ensures that different plugins (Sources/Senders) and core services (Agent/Fusion) speak the same language and follow the same architectural patterns.