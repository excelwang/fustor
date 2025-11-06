from fastapi import HTTPException, status

from typing import Optional, Any, Dict

class fustor_agentException(HTTPException):
    """Base exception for fustor_agent, mapping to HTTP exceptions."""
    def __init__(self, status_code: int, detail: Any = None, headers: Optional[Dict[str, Any]] = None):
        super().__init__(status_code=status_code, detail=detail, headers=headers)

class ConfigError(fustor_agentException):
    """Raised when there's an issue with configuration (e.g., not found, invalid)."""
    def __init__(self, detail: str = "Configuration error", headers: Optional[Dict[str, Any]] = None):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail, headers=headers)

class NotFoundError(fustor_agentException):
    """Raised when a requested resource (e.g., config, instance) is not found."""
    def __init__(self, detail: str = "Resource not found", headers: Optional[Dict[str, Any]] = None):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail, headers=headers)

class ConflictError(fustor_agentException):
    """Raised when a resource already exists and cannot be created again."""
    def __init__(self, detail: str = "Resource already exists", headers: Optional[Dict[str, Any]] = None):
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=detail, headers=headers)

class DriverError(fustor_agentException):
    """Raised when there's an issue with a driver (e.g., connection, invalid parameters)."""
    def __init__(self, detail: str = "Driver error", headers: Optional[Dict[str, Any]] = None):
        super().__init__(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail, headers=headers)

class StateConflictError(fustor_agentException):
    """Raised when an operation is attempted in an invalid state."""
    def __init__(self, detail: str = "Operation not allowed in current state", headers: Optional[Dict[str, Any]] = None):
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=detail, headers=headers)

class ValidationError(fustor_agentException):
    """Raised when input validation fails."""
    def __init__(self, detail: str = "Validation error", headers: Optional[Dict[str, Any]] = None):
        super().__init__(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail, headers=headers)

class TransientSourceBufferFullError(Exception):
    """Raised by MemoryEventBus when its buffer is full and the source is transient."""
    pass