# fustor-event-model

This package defines the standardized event models and schemas used across various Fustor services and components. It provides a consistent structure for data events, ensuring interoperability and clear communication between different parts of the Fustor ecosystem.

## Features

*   **Standardized Event Models**: Pydantic models for representing different types of data events.
*   **Data Validation**: Leverages Pydantic for automatic data validation and serialization/deserialization.
*   **Interoperability**: Ensures that all Fustor services handle event data in a consistent and predictable manner.

## Contents

*   `models.py`: Contains the core Pydantic models for events, including base event classes and specific event types.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

Other Fustor services and packages import models from `fustor-event-model` to define and process event data.

Example:

```python
from fustor_event_model.models import BaseEvent, DataChangeEvent

# Create an event instance
change_event = DataChangeEvent(
    event_id="12345",
    timestamp=1678886400,
    entity_type="user",
    entity_id="user_abc",
    changes={"name": {"old": "John Doe", "new": "Jane Doe"}}
)

# Access event data
print(f"Event Type: {change_event.event_type}")
print(f"Entity ID: {change_event.entity_id}")
```

## Dependencies

*   `pydantic`: For defining and validating data models.
