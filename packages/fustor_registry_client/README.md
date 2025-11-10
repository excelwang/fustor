# fustor-registry-client

This package provides a Python client for interacting with the Fustor Registry service. It offers a convenient way to programmatically access and manage metadata, storage environments, data stores, users, API keys, and datasets within the Fustor platform.

## Features

*   **Registry Client**: A Python client for making requests to the Fustor Registry API.
*   **Models**: Provides Pydantic data models for requests, responses, and other data structures used by the Fustor Registry API.

## Installation

This package is part of the Fustor monorepo and is typically installed in editable mode within the monorepo's development environment using `uv sync`.

## Usage

Developers can use this client to integrate with the Fustor Registry service from other Fustor components or external applications. It simplifies the process of interacting with the Registry's RESTful API.

Example (conceptual):

```python
from fustor_registry_client.client import RegistryClient
from fustor_registry_client.models import UserCreate, UserUpdate

# Assuming RegistryClient is initialized with the Registry service URL
client = RegistryClient(base_url="http://localhost:8002")

# Example: Create a new user
new_user = UserCreate(username="testuser", email="test@example.com", password="securepassword")
created_user = client.create_user(new_user)
print(f"Created user: {created_user.username}")

# Example: Get a user by ID
user = client.get_user(user_id=created_user.id)
print(f"Retrieved user: {user.email}")
```

## Dependencies

*   `httpx`: A next-generation HTTP client for Python.
*   `pydantic`: For defining and validating data models.
*   `fustor-common`: Provides foundational elements and shared components.
