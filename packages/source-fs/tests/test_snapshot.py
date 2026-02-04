import pytest
from pathlib import Path
from unittest.mock import MagicMock, ANY
import time

from fustor_source_fs import FSDriver
from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_core.event import UpdateEvent

@pytest.fixture(autouse=True)
def clear_driver_instances():
    """Clears the FSDriver singleton instances before each test."""
    from fustor_source_fs import FSDriver
    FSDriver._instances.clear()

@pytest.fixture
def fs_config(tmp_path: Path):
    """Provides a default FS SourceConfig pointing to a temporary directory."""
    return SourceConfig(
        driver="fs",
        uri=str(tmp_path),
        credential=PasswdCredential(user="test")
    )

@pytest.fixture
def mock_watch_manager(mocker):
    """Mocks the _WatchManager to observe schedule calls."""
    manager = MagicMock()
    manager.watches = {}
    manager.lru_cache = MagicMock()
    manager.lru_cache.get_oldest.return_value = (None, 0)
    # Patch where it is imported in fustor_source_fs/driver.py
    mocker.patch('fustor_source_fs.driver._WatchManager', return_value=manager)
    return manager

def test_snapshot_finds_files_and_generates_events(fs_config, tmp_path: Path, mock_watch_manager):
        """Test that get_snapshot_iterator finds files and directories and yields UpdateEvent correctly."""
        # Arrange
        dir1_path = tmp_path / "dir1"
        dir1_path.mkdir()
        test1_file = tmp_path / "test1.txt"
        test1_file.write_text("content1")
        test2_file = dir1_path / "test2.txt"
        test2_file.write_text("content2")
        test3_file = tmp_path / "test3.log"
        test3_file.write_text("log_content")

        driver = FSDriver('test-fs-id', fs_config)
        # Mock the required_fields_tracker for this test
        mock_tracker = MagicMock()
        mock_tracker.get_fields.return_value = {
            "test-fs.files.path",
            "test-fs.files.size",
            "test-fs.files.is_directory" # Add is_dir to expected fields
        }

        # Act
        driver.config.driver_params["hot_data_cooloff_seconds"] = 0
        iterator = driver.get_snapshot_iterator(batch_size=2, required_fields_tracker=mock_tracker)
        events = list(iterator)

        # Assert
        # Total items: tmp_path (dir), dir1 (dir), test1.txt (file), test2.txt (file), test3.log (file) = 5 items
        # With batch_size=2, this means ceil(5/2) = 3 batches.
        assert len(events) == 3 
        
        all_processed_paths = set()
        all_file_paths = set()
        all_dir_paths = set()

        for event in events:
            assert isinstance(event, UpdateEvent)
            for row in event.rows:
                file_path = row['path']
                is_directory = row.get('is_directory', False) # Default to False if not present

                all_processed_paths.add(file_path)
                if is_directory:
                    all_dir_paths.add(file_path)
                else:
                    all_file_paths.add(file_path)
                
                # Basic checks for metadata
                assert 'size' in row
                assert 'modified_time' in row
                assert 'created_time' in row
                assert 'is_directory' in row # Ensure the flag is always present

        # Assert all expected items (files and directories) were processed
        expected_paths = {
            str(tmp_path),
            str(dir1_path),
            str(test1_file),
            str(test2_file),
            str(test3_file)
        }
        assert all_processed_paths == expected_paths

        # Assert correct types
        assert str(tmp_path) in all_dir_paths
        assert str(dir1_path) in all_dir_paths
        assert str(test1_file) in all_file_paths
        assert str(test2_file) in all_file_paths
        assert str(test3_file) in all_file_paths
        assert len(all_dir_paths) == 2
        assert len(all_file_paths) == 3

        # Verify watch_manager.touch calls
        # Expected calls: tmp_path, dir1_path
        touched_paths = {call_arg.args[0] for call_arg in mock_watch_manager.touch.call_args_list}
        assert str(tmp_path) in touched_paths
        assert str(dir1_path) in touched_paths
        assert len(touched_paths) == 2 # Only tmp_path and dir1_path are directories
def test_snapshot_message_only_mode(fs_config):
    """Test that get_snapshot_iterator returns immediately if startup_mode is message-only."""
    # Arrange
    fs_config.driver_params["startup_mode"] = "message-only"
    driver = FSDriver('test-fs-id', fs_config)

    # Act
    iterator = driver.get_snapshot_iterator()
    events = list(iterator)

    # Assert
    assert len(events) == 0
