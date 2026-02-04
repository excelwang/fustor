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


from fustor_schema_fs.models import FSInsertEvent, FSUpdateEvent, FSDeleteEvent
from fustor_core.event import EventType

class TestFSEvents:
    """Test FS Event Pydantic models."""
    
    def test_insert_event(self):
        row = FSRow(
            path="/test/file.txt",
            file_name="file.txt",
            size=102,
            modified_time=100.0,
            is_directory=False
        )
        event = FSInsertEvent(
            rows=[row],
            index=1
        )
        assert event.event_type == EventType.INSERT
        assert event.event_schema == "fs"
        assert event.table == "files"
        assert len(event.rows) == 1
        assert event.rows[0].path == "/test/file.txt"

    def test_update_event(self):
        row = FSRow(
            path="/test/file.txt",
            file_name="file.txt",
            size=2048,
            modified_time=200.0,
            is_directory=False
        )
        event = FSUpdateEvent(rows=[row])
        assert event.event_type == EventType.UPDATE
        assert event.rows[0].size == 2048

    def test_delete_event(self):
        row = FSDeleteRow(path="/test/file.txt")
        event = FSDeleteEvent(rows=[row])
        assert event.event_type == EventType.DELETE
        assert event.rows[0].path == "/test/file.txt"
        
    def test_incorrect_row_type(self):
        """Test that passing generic dicts instead of typed rows usually works if pydantic coerces, 
           but passing wrong types fails."""
        # Pydantic can often coerce dict to model
        data = {
            "path": "/test/file.txt",
            "file_name": "file.txt",
            "size": 100,
            "modified_time": 100.0,
            "is_directory": False
        }
        event = FSInsertEvent(rows=[data])
        assert isinstance(event.rows[0], FSRow)
        
        # Test invalid data
        with pytest.raises(ValidationError):
            FSInsertEvent(rows=[{"path": "/test"}]) # Missing required fields

