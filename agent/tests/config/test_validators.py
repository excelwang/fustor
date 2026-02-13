import pytest
from fustor_agent.config.validators import validate_url_safe_id

def test_validate_url_safe_id_valid():
    """Test valid URL-safe IDs."""
    assert validate_url_safe_id("valid-id-123", "id") == []
    assert validate_url_safe_id("another_id_456", "id") == []
    assert validate_url_safe_id("idonly", "id") == []
    assert validate_url_safe_id("123-id", "id") == []
    assert validate_url_safe_id("a" * 64, "long_id") == []

def test_validate_url_safe_id_empty():
    """Test empty ID."""
    errors = validate_url_safe_id("", "id")
    assert len(errors) == 1
    assert "id cannot be empty" in errors[0]

def test_validate_url_safe_id_too_long():
    """Test ID longer than 64 characters."""
    errors = validate_url_safe_id("a" * 65, "long_id")
    assert len(errors) == 1
    assert "long_id must be at most 64 characters" in errors[0]
    
    # Ensure regex also catches it if it starts with valid char
    errors = validate_url_safe_id("a" * 65, "id")
    assert len(errors) == 1
    assert "id must be at most 64 characters" in errors[0]


def test_validate_url_safe_id_invalid_characters():
    """Test IDs with invalid characters."""
    errors = validate_url_safe_id("invalid ID!", "id")
    assert len(errors) == 1
    assert "id 'invalid ID!' contains invalid characters." in errors[0]

    errors = validate_url_safe_id("id.with.dots", "id")
    assert len(errors) == 1
    assert "id 'id.with.dots' contains invalid characters." in errors[0]

def test_validate_url_safe_id_starts_with_invalid_char():
    """Test IDs starting with an invalid character (e.g., hyphen)."""
    errors = validate_url_safe_id("-starts-with-hyphen", "id")
    assert len(errors) == 1
    assert "id '-starts-with-hyphen' contains invalid characters." in errors[0]
    # The error message from the regex pattern match covers "must start with a letter or digit" implicitly.
