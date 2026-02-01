# packages/schema-fs/tests/test_models.py
"""
Tests for fustor-schema-fs Pydantic models.
"""
import pytest
from pydantic import ValidationError
from fustor_schema_fs.models import FSRow, FSDeleteRow


class TestFSRow:
    """Test FSRow Pydantic model."""
    
    def test_valid_row(self):
        row = FSRow(
            path="/test/file.txt",
            file_name="file.txt",
            size=1024,
            modified_time=1234567890.0,
            is_directory=False,
        )
        assert row.path == "/test/file.txt"
        assert row.file_name == "file.txt"
        assert row.size == 1024
        assert row.is_directory is False
    
    def test_missing_required_field(self):
        with pytest.raises(ValidationError):
            FSRow(
                path="/test/file.txt",
                file_name="file.txt",
                # missing size, modified_time, is_directory
            )
    
    def test_file_path_alias(self):
        """Test that file_path works as alias for path."""
        row = FSRow(
            file_path="/test/file.txt",  # Using deprecated alias
            file_name="file.txt",
            size=1024,
            modified_time=1234567890.0,
            is_directory=False,
        )
        assert row.path == "/test/file.txt"
        assert row.get_normalized_path() == "/test/file.txt"
    
    def test_negative_size_rejected(self):
        with pytest.raises(ValidationError):
            FSRow(
                path="/test/file.txt",
                file_name="file.txt",
                size=-100,  # Invalid
                modified_time=1234567890.0,
                is_directory=False,
            )
    
    def test_optional_fields(self):
        row = FSRow(
            path="/test/file.txt",
            file_name="file.txt",
            size=1024,
            modified_time=1234567890.0,
            is_directory=False,
            parent_path="/test",
            parent_mtime=1234567800.0,
            audit_skipped=True,
        )
        assert row.parent_path == "/test"
        assert row.parent_mtime == 1234567800.0
        assert row.audit_skipped is True
    
    def test_from_dict(self):
        data = {
            "path": "/test/file.txt",
            "file_name": "file.txt",
            "size": 1024,
            "modified_time": 1234567890.0,
            "is_directory": False,
        }
        row = FSRow(**data)
        assert row.path == "/test/file.txt"


class TestFSDeleteRow:
    """Test FSDeleteRow Pydantic model."""
    
    def test_valid_delete_row(self):
        row = FSDeleteRow(path="/test/deleted.txt")
        assert row.path == "/test/deleted.txt"
    
    def test_file_path_alias(self):
        row = FSDeleteRow(file_path="/test/deleted.txt")
        assert row.path == "/test/deleted.txt"
        assert row.get_normalized_path() == "/test/deleted.txt"
    
    def test_missing_path(self):
        with pytest.raises(ValidationError):
            FSDeleteRow()
