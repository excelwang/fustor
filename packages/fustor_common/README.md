# fustor-common

This package provides common utilities, configurations, exceptions, and data schemas shared across various Fustor services and packages within the monorepo. Its purpose is to promote code reusability and maintain consistency throughout the Fustor ecosystem.

## Features

*   **Logging Configuration**: Standardized logging setup for all Fustor components.
*   **Exceptions**: Custom exception classes for consistent error handling.
*   **Schemas**: Shared Pydantic models for common data structures, ensuring data validation and interoperability.
*   **Enums**: Enumerations for common categorical data, such as user levels.

## Contents

*   `logging_config.py`: Contains the `setup_logging` function for configuring application-wide logging.
*   `exceptions.py`: Defines `ConfigurationError` and other custom exceptions.
*   `schemas.py`: Houses `ConfigCreateResponse` and other shared Pydantic schemas.
*   `enums.py`: Contains `UserLevel` and other shared enumerations.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

Components from `fustor-common` are imported and utilized by other Fustor services (e.g., `agent`, `registry`, `fusion`) and plugin packages to leverage shared functionalities and maintain consistency.

Example:

```python
from fustor_common.logging_config import setup_logging
from fustor_common.exceptions import ConfigurationError
from fustor_common.enums import UserLevel

# Setup logging
setup_logging()

# Raise a custom exception
try:
    raise ConfigurationError("Invalid configuration setting.")
except ConfigurationError as e:
    print(f"Error: {e}")

# Use an enum
if some_user.level == UserLevel.ADMIN:
    print("Admin user detected.")
```

## Dependencies

*   `colorlog`: For colored and formatted console output.
*   `pydantic`: For data validation and settings management.
*   `pydantic-settings`: For managing application settings.
