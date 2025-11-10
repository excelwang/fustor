# fustor-core

This package contains core components, abstractions, and utilities shared across various Fustor services and plugins within the monorepo. It provides foundational elements such as base classes for drivers, common exceptions, data models, and utility functions.

## Contents

*   `drivers.py`: Defines abstract base classes (ABCs) for `SourceDriver` and `PusherDriver`, establishing the contract for all data source and data pusher implementations.
*   `exceptions.py`: Contains custom exception classes used throughout the Fustor ecosystem for consistent error handling.
*   `models/`: Houses Pydantic models for various data structures, ensuring data validation and serialization.
*   `utils/`: Provides general utility functions that are commonly used by different Fustor components.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

Components from `fustor-core` are imported and utilized by other Fustor services (e.g., `agent`, `registry`, `fusion`) and plugin packages (e.g., `source_mysql`, `pusher_fusion`) to ensure consistency and reusability.

## Note on Package Structure

It has been observed that the `__init__.py` file is missing from the `fustor_core` package directory (`packages/core/src/fustor_core/`). While the package's contents are still accessible, this is an unconventional Python package structure and might lead to issues with package discovery or imports in certain environments. It is recommended to add an empty `__init__.py` file to this directory.