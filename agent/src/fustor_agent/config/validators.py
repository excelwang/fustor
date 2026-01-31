# agent/src/fustor_agent/config/validators.py
"""
Validators for configuration values.
"""
import re
from typing import List

# URL-safe ID pattern: lowercase letters, digits, hyphens, underscores
# Must start with a letter or digit
URL_SAFE_ID_PATTERN = re.compile(r'^[a-z0-9][a-z0-9_-]*$')


def validate_url_safe_id(value: str, field_name: str = "id") -> List[str]:
    """
    Validate that an ID can be used directly in URL paths without encoding.
    
    Valid characters: a-z, 0-9, -, _
    Must start with a letter or digit.
    
    Args:
        value: The ID value to validate
        field_name: Name of the field for error messages
        
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    if not value:
        errors.append(f"{field_name} cannot be empty")
        return errors
    
    if len(value) > 64:
        errors.append(f"{field_name} must be at most 64 characters")
    
    if not URL_SAFE_ID_PATTERN.match(value):
        errors.append(
            f"{field_name} '{value}' contains invalid characters. "
            f"Only lowercase letters (a-z), digits (0-9), hyphens (-), and underscores (_) are allowed. "
            f"Must start with a letter or digit."
        )
    
    return errors
