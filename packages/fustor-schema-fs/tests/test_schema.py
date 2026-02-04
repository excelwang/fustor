# packages/schema-fs/tests/test_schema.py
"""
Tests for fustor-schema-fs package.
"""
import pytest
from fustor_schema_fs import (
    SCHEMA_NAME,
    SCHEMA_VERSION,
    REQUIRED_FIELDS,
    validate_row,
    get_path,
    get_mtime,
)


class TestSchemaConstants:
    """Test schema constants."""
    
    def test_schema_name(self):
        assert SCHEMA_NAME == "fs"
    
    def test_schema_version(self):
        assert SCHEMA_VERSION == "1.0"
    
    def test_required_fields(self):
        assert "path" in REQUIRED_FIELDS
        assert "file_name" in REQUIRED_FIELDS
        assert "size" in REQUIRED_FIELDS
        assert "modified_time" in REQUIRED_FIELDS
        assert "is_directory" in REQUIRED_FIELDS


class TestValidateRow:
    """Test validate_row function."""
    
    def test_valid_row(self):
        row = {
            "path": "/test/file.txt",
            "file_name": "file.txt",
            "size": 1024,
            "modified_time": 1234567890.0,
            "is_directory": False,
        }
        assert validate_row(row) is True
    
    def test_missing_required_field(self):
        row = {
            "path": "/test/file.txt",
            "file_name": "file.txt",
            # missing size, modified_time, is_directory
        }
        assert validate_row(row) is False
    
    def test_file_path_alias(self):
        """Test that file_path works as alias for path."""
        row = {
            "file_path": "/test/file.txt",  # Using deprecated alias
            "file_name": "file.txt",
            "size": 1024,
            "modified_time": 1234567890.0,
            "is_directory": False,
        }
        assert validate_row(row) is True
    
    def test_delete_event(self):
        """Test DELETE event only needs path."""
        row = {"path": "/test/deleted.txt"}
        assert validate_row(row, event_type="delete") is True
        
        row_alias = {"file_path": "/test/deleted.txt"}
        assert validate_row(row_alias, event_type="delete") is True
    
    def test_delete_event_empty(self):
        """Test DELETE event fails without path."""
        row = {"file_name": "file.txt"}
        assert validate_row(row, event_type="delete") is False


class TestGetPath:
    """Test get_path function."""
    
    def test_get_path_primary(self):
        row = {"path": "/primary/path.txt", "file_path": "/fallback.txt"}
        assert get_path(row) == "/primary/path.txt"
    
    def test_get_path_fallback(self):
        row = {"file_path": "/fallback.txt"}
        assert get_path(row) == "/fallback.txt"
    
    def test_get_path_empty(self):
        row = {}
        assert get_path(row) == ""


class TestGetMtime:
    """Test get_mtime function."""
    
    def test_get_mtime(self):
        row = {"modified_time": 1234567890.5}
        assert get_mtime(row) == 1234567890.5
    
    def test_get_mtime_missing(self):
        row = {}
        assert get_mtime(row) == 0.0
