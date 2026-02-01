import os
import time
from pathlib import Path
from unittest.mock import MagicMock, call


import shutil

from watchdog.events import FileSystemEvent
from fustor_core.event import DeleteEvent, UpdateEvent
from fustor_source_fs.event_handler import OptimizedWatchEventHandler
from fustor_source_fs.components import _WatchManager


# Mock _get_file_metadata to return consistent data for testing
def _mock_get_file_metadata(path: str):
    if os.path.exists(path) and os.path.isfile(path):
        return {"file_path": path, "size": 100, "modified_time": time.time(), "created_time": time.time()}
    return None


def test_on_moved_directory_generates_correct_events_and_touches(mocker, tmp_path: Path):
    """
    Test that on_moved for a directory generates correct recursive DeleteEvents and UpdateEvents,
    and calls watch_manager methods appropriately.
    """
    # Arrange
    src_root = tmp_path / "src_root"
    src_root.mkdir()
    src_dir = src_root / "src_dir"
    src_dir.mkdir()
    sub_dir_src = src_dir / "sub_dir"
    sub_dir_src.mkdir()
    file_in_sub_src = sub_dir_src / "file_in_sub.txt"
    file_in_sub_src.write_text("content_sub")
    file_in_src_root = src_dir / "file_in_src.txt"
    file_in_src_root.write_text("content_root")

    dest_root = tmp_path / "dest_root"
    dest_root.mkdir()
    dest_dir = dest_root / "dest_dir"

    # Simulate the move on disk for os.walk and _get_file_metadata to work
    shutil.move(str(src_dir), str(dest_dir))

    # Mock dependencies
    mock_event_queue = MagicMock()
    mock_watch_manager = MagicMock(spec=_WatchManager)
    mock_watch_manager.root_path = str(tmp_path) # Needed for touch's while loop
    mock_watch_manager.watches = {}
    mock_watch_manager.lru_cache = MagicMock()
    mock_watch_manager.lru_cache.__contains__.return_value = True # Assume items are in cache for touch
    mock_watch_manager.lru_cache.remove.return_value = None
    mock_watch_manager.schedule.return_value = None # schedule is called by touch

    mocker.patch('fustor_source_fs.event_handler.get_file_metadata', side_effect=_mock_get_file_metadata)

    handler = OptimizedWatchEventHandler(mock_event_queue, mock_watch_manager)

    # Create a mock FileSystemEvent for a directory move
    mock_event = MagicMock(spec=FileSystemEvent)
    mock_event.event_type = 'moved'
    mock_event.src_path = str(src_dir) # Original path
    mock_event.dest_path = str(dest_dir) # New path
    mock_event.is_directory = True

    # Act
    handler.on_moved(mock_event)

    # Assertions for event_queue.put calls
    put_calls = mock_event_queue.put.call_args_list
    captured_events = [call_arg.args[0] for call_arg in put_calls]

    # Expected DeleteEvents (4: src_dir, sub_dir_src, file_in_sub_src, file_in_src_root)
    delete_events = [e for e in captured_events if isinstance(e, DeleteEvent)]
    assert len(delete_events) == 4
    delete_paths = {e.rows[0]['file_path'] for e in delete_events}
    assert str(src_dir) in delete_paths
    assert str(sub_dir_src) in delete_paths
    assert str(file_in_sub_src) in delete_paths
    assert str(file_in_src_root) in delete_paths

    # Expected UpdateEvents (2: file_in_sub_dest, file_in_dest_root)
    update_events = [e for e in captured_events if isinstance(e, UpdateEvent)]
    assert len(update_events) == 2
    update_paths = {e.rows[0]['file_path'] for e in update_events}
    assert str(dest_dir / "sub_dir" / "file_in_sub.txt") in update_paths
    assert str(dest_dir / "file_in_src.txt") in update_paths

    # Assertions for watch_manager calls
    # touch calls
    touch_calls = mock_watch_manager.touch.call_args_list
    touched_paths = {call_arg.args[0] for call_arg in touch_calls}
    
    # Parents of src_dir and dest_dir
    assert str(src_root) in touched_paths
    assert str(dest_root) in touched_paths
    
    # Dest dir and its children (from _touch_recursive_bottom_up)
    assert str(dest_dir) in touched_paths
    assert str(dest_dir / "sub_dir") in touched_paths

    # unschedule_recursive call
    mock_watch_manager.unschedule_recursive.assert_called_once_with(str(src_dir))


def test_on_moved_file_generates_correct_events_and_touches(mocker, tmp_path: Path):
    """
    Test that on_moved for a file generates correct DeleteEvent and UpdateEvent,
    and calls watch_manager methods appropriately.
    """
    # Arrange
    src_root = tmp_path / "src_root"
    src_root.mkdir()
    src_file = src_root / "file.txt"
    src_file.write_text("content")

    dest_root = tmp_path / "dest_root"
    dest_root.mkdir()
    dest_file = dest_root / "new_file.txt"

    # Simulate the move on disk for get_file_metadata to work
    shutil.move(str(src_file), str(dest_file))

    # Mock dependencies
    mock_event_queue = MagicMock()
    mock_watch_manager = MagicMock(spec=_WatchManager)
    mock_watch_manager.root_path = str(tmp_path)
    mocker.patch('fustor_source_fs.event_handler.get_file_metadata', side_effect=_mock_get_file_metadata)

    handler = OptimizedWatchEventHandler(mock_event_queue, mock_watch_manager)

    # Create a mock FileSystemEvent for a file move
    mock_event = MagicMock(spec=FileSystemEvent)
    mock_event.event_type = 'moved'
    mock_event.src_path = str(src_file)
    mock_event.dest_path = str(dest_file)
    mock_event.is_directory = False

    # Act
    handler.on_moved(mock_event)

    # Assertions for event_queue.put calls
    put_calls = mock_event_queue.put.call_args_list
    captured_events = [call_arg.args[0] for call_arg in put_calls]

    # Expected DeleteEvent
    delete_events = [e for e in captured_events if isinstance(e, DeleteEvent)]
    assert len(delete_events) == 1
    assert delete_events[0].rows[0]['file_path'] == str(src_file)

    # Expected UpdateEvent
    update_events = [e for e in captured_events if isinstance(e, UpdateEvent)]
    assert len(update_events) == 1
    assert update_events[0].rows[0]['file_path'] == str(dest_file)

    # Assertions for watch_manager calls
    touch_calls = mock_watch_manager.touch.call_args_list
    touched_paths = {call_arg.args[0] for call_arg in touch_calls}
    
    assert str(src_root) in touched_paths
    assert str(dest_root) in touched_paths
    assert str(dest_file) in touched_paths

    mock_watch_manager.unschedule_recursive.assert_not_called()
