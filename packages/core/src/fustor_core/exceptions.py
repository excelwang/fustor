"""
Fustor Core Exceptions.

These are generic exceptions that don't depend on any web framework.
HTTP mapping is done at the API layer (e.g., via FastAPI exception handlers).
"""
from typing import Optional, Any, Dict


class FustorException(Exception):
    """Base exception for all Fustor errors."""
    
    # Default HTTP status code mapping (used by API layers)
    status_code: int = 500
    
    def __init__(self, detail: str = "An error occurred", context: Optional[Dict[str, Any]] = None):
        self.detail = detail
        self.context = context or {}
        super().__init__(detail)


class ConfigError(FustorException):
    """Raised when there's an issue with configuration (e.g., not found, invalid)."""
    status_code = 400
    
    def __init__(self, detail: str = "Configuration error", context: Optional[Dict[str, Any]] = None):
        super().__init__(detail=detail, context=context)


class NotFoundError(FustorException):
    """Raised when a requested resource (e.g., config, instance) is not found."""
    status_code = 404
    
    def __init__(self, detail: str = "Resource not found", context: Optional[Dict[str, Any]] = None):
        super().__init__(detail=detail, context=context)


class ConflictError(FustorException):
    """Raised when a resource already exists and cannot be created again."""
    status_code = 409
    
    def __init__(self, detail: str = "Resource already exists", context: Optional[Dict[str, Any]] = None):
        super().__init__(detail=detail, context=context)


class DriverError(FustorException):
    """Raised when there's an issue with a driver (e.g., connection, invalid parameters)."""
    status_code = 500
    
    def __init__(self, detail: str = "Driver error", context: Optional[Dict[str, Any]] = None):
        super().__init__(detail=detail, context=context)


class StateConflictError(FustorException):
    """Raised when an operation is attempted in an invalid state."""
    status_code = 409
    
    def __init__(self, detail: str = "Operation not allowed in current state", context: Optional[Dict[str, Any]] = None):
        super().__init__(detail=detail, context=context)


class ValidationError(FustorException):
    """Raised when input validation fails."""
    status_code = 422
    
    def __init__(self, detail: str = "Validation error", context: Optional[Dict[str, Any]] = None):
        super().__init__(detail=detail, context=context)


class SessionObsoletedError(FustorException):
    """Raised when a pipeline session is no longer valid (e.g., replaced by a newer one)."""
    status_code = 419
    
    def __init__(self, detail: str = "Session is obsolete", context: Optional[Dict[str, Any]] = None):
        super().__init__(detail=detail, context=context)


class TransientSourceBufferFullError(Exception):

    """Raised by MemoryEventBus when its buffer is full and the source is transient."""
    pass