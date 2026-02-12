"""
Tests for validate_url_safe_id configuration validator.
"""
import pytest
from fustor_fusion.config.validators import validate_url_safe_id


def test_valid_ids():
    """Valid URL-safe IDs produce no errors."""
    for valid_id in ["pipe-1", "my_source", "abc123", "a", "1test"]:
        errors = validate_url_safe_id(valid_id)
        assert errors == [], f"Unexpected error for '{valid_id}': {errors}"


def test_empty_id():
    """Empty string produces error."""
    errors = validate_url_safe_id("")
    assert len(errors) == 1
    assert "cannot be empty" in errors[0]


def test_too_long_id():
    """ID longer than 64 chars produces error."""
    long_id = "a" * 65
    errors = validate_url_safe_id(long_id)
    assert any("at most 64" in e for e in errors)


def test_max_length_ok():
    """ID exactly 64 chars is valid."""
    exact_id = "a" * 64
    errors = validate_url_safe_id(exact_id)
    assert errors == []


def test_uppercase_rejected():
    """Uppercase letters are not URL-safe."""
    errors = validate_url_safe_id("MyPipe")
    assert len(errors) > 0
    assert "invalid characters" in errors[0]


def test_special_chars_rejected():
    """Special characters like spaces, dots are rejected."""
    for bad_id in ["my pipe", "pipe.1", "pipe@2", "pipe/3"]:
        errors = validate_url_safe_id(bad_id)
        assert len(errors) > 0, f"Expected error for '{bad_id}'"


def test_starts_with_hyphen_rejected():
    """IDs starting with hyphen are rejected."""
    errors = validate_url_safe_id("-pipe")
    assert len(errors) > 0


def test_custom_field_name():
    """Custom field_name appears in error message."""
    errors = validate_url_safe_id("", field_name="source_id")
    assert "source_id" in errors[0]
