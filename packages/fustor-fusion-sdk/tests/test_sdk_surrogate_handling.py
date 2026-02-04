"""
Test case to verify that the fustor_fusion_sdk correctly handles surrogate characters
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from fustor_fusion_sdk.client import FusionClient, sanitize_surrogate_characters


def test_sanitize_surrogate_characters_string():
    """Test that the sanitize_surrogate_characters function handles strings with surrogate characters."""
    # Test string with surrogate character
    test_str = "valid_text_\udca3_invalid_surrogate"
    result = sanitize_surrogate_characters(test_str)
    
    # The result should not contain the invalid surrogate character
    # Instead it should contain a replacement character (usually )
    assert "" in result  # Replacement character
    assert "_invalid_surrogate" in result  # Valid part remains
    assert "\udca3" not in result  # Original surrogate char is gone


def test_sanitize_surrogate_characters_dict():
    """Test that the sanitize_surrogate_characters function handles dictionaries with surrogate characters."""
    test_dict = {
        "valid_key": "valid_value",
        "key_with_surrogate": "path_with_\udca3_surrogate",
        "another_valid": "value_without_surrogates"
    }
    
    result = sanitize_surrogate_characters(test_dict)
    
    # Valid entries should remain unchanged
    assert result["valid_key"] == "valid_value"
    assert result["another_valid"] == "value_without_surrogates"
    
    # The entry with surrogate should be cleaned
    assert "" in result["key_with_surrogate"]
    assert "_surrogate" in result["key_with_surrogate"]
    assert "\udca3" not in result["key_with_surrogate"]


def test_sanitize_surrogate_characters_nested():
    """Test that the sanitize_surrogate_characters function handles nested data structures."""
    test_data = {
        "level1": {
            "level2": [
                "normal_string",
                "string_with_\udca3_surrogate",
                {"nested_key": "nested_\udcb3_value"}
            ]
        }
    }
    
    result = sanitize_surrogate_characters(test_data)
    
    # Check deeply nested values are cleaned
    assert "" in result["level1"]["level2"][1]  # First surrogate string
    assert "" in result["level1"]["level2"][2]["nested_key"]  # Nested surrogate
    assert "\udca3" not in result["level1"]["level2"][1]
    assert "\udcb3" not in result["level1"]["level2"][2]["nested_key"]
    
    # Valid strings remain unchanged
    assert result["level1"]["level2"][0] == "normal_string"


def test_sanitize_surrogate_characters_no_surrogates():
    """Test that the sanitize_surrogate_characters function doesn't modify strings without surrogates."""
    test_str = "normal_string_without_surrogates"
    result = sanitize_surrogate_characters(test_str)
    assert result == test_str  # Should remain unchanged


def test_sanitize_surrogate_characters_other_types():
    """Test that the sanitize_surrogate_characters function handles non-string types correctly."""
    test_data = {
        "string_val": "with_\udca3_surrogate",
        "int_val": 42,
        "bool_val": True,
        "none_val": None,
        "list_val": ["item1", "item2_\udca4_with_surrogate"]
    }
    
    result = sanitize_surrogate_characters(test_data)
    
    # Non-string types should remain unchanged
    assert result["int_val"] == 42
    assert result["bool_val"] is True
    assert result["none_val"] is None
    
    # Strings should be cleaned
    assert "" in result["string_val"]
    assert "" in result["list_val"][1]


@pytest.mark.asyncio
async def test_push_events_with_surrogate_characters():
    """Test that push_events properly sanitizes events containing surrogate characters."""
    client = FusionClient("http://test.com", "fake-api-key")

    # Mock the HTTP client to avoid actual network calls
    mock_response = AsyncMock()
    # In httpx, raise_for_status is a sync method, so we mock it differently
    mock_response.raise_for_status = lambda: None  # This is a sync method that raises on failure
    mock_response.json.return_value = {"session_id": "test-session"}

    with patch.object(client.client, 'post', return_value=mock_response) as mock_post:
        # Create test events with surrogate characters
        test_events = [
            {
                "event_type": "update",
                "event_schema": "test_schema",
                "table": "test_table",
                "rows": [{"path": "/valid/path/with_\udca3_surrogate/file.txt", "size": 100}]
            }
        ]

        # This should not raise an exception due to surrogate characters
        result = await client.push_events("test_session", test_events, "test_source")

        # Verify that the call was made
        assert result is True
        mock_post.assert_called_once()

        # Get the arguments passed to the post call
        call_args = mock_post.call_args
        payload = call_args[1]['json']  # Get the JSON payload

        # The surrogate character should be removed/replaced in the payload
        event_path = payload['events'][0]['rows'][0]['path']
        assert "" in event_path  # Replacement character should be present
        assert "\udca3" not in event_path  # Original surrogate should be removed



def test_contains_surrogate_characters_detection():
    """Test that contains_surrogate_characters correctly detects surrogate characters."""
    from fustor_fusion_sdk.client import contains_surrogate_characters
    
    # String with surrogate character should return True
    assert contains_surrogate_characters("test_\udca3_string") is True
    
    # String without surrogate characters should return False
    assert contains_surrogate_characters("test_string_without_surrogates") is False
    
    # Empty string should return False
    assert contains_surrogate_characters("") is False