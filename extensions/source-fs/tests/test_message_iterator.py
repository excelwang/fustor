
import threading
import time
from pathlib import Path
from typing import List

import pytest

from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_core.event import DeleteEvent, UpdateEvent
from fustor_source_fs import FSDriver

@pytest.fixture(autouse=True)
def clear_driver_instances():
    """Clears the FSDriver singleton instances before each test."""
    FSDriver._instances.clear()

@pytest.fixture
def fs_config(tmp_path: Path) -> SourceConfig:
    """Provides a SourceConfig pointing to a temporary directory."""
    return SourceConfig(driver="fs", uri=str(tmp_path), credential=PasswdCredential(user="test"))


@pytest.fixture
def message_iterator_runner(fs_config: SourceConfig, tmp_path: Path):
    """A fixture to run the message iterator in a background thread."""
    stop_event = threading.Event()
    events: List = []
    driver = FSDriver('test-fs-id', fs_config)
    thread = None # Hold the thread to join it in teardown

    # Schedule a watch on the root directory for all tests using this fixture
    driver.watch_manager.schedule(str(tmp_path), time.time())

    def _runner(start_pos_offset: float = -1.0):
        nonlocal thread
        start_position = time.time() + start_pos_offset
        
        def run_in_thread():
            # Use the new interface that returns only the iterator
            iterator = driver.get_message_iterator(start_position=int(start_position*1000), stop_event=stop_event)
            for event in iterator:
                events.append(event)
        
        thread = threading.Thread(target=run_in_thread)
        thread.start()
        # Allow watchdog to initialize and start processing
        time.sleep(0.1)
        return thread

    yield _runner, events, driver

    # Teardown
    stop_event.set()
    if thread and thread.is_alive():
        thread.join(timeout=2)
    driver.watch_manager.stop()


def test_realtime_file_creation(tmp_path: Path, message_iterator_runner):
    """Tests that a new file created and closed generates an UpdateEvent."""
    runner, events, _ = message_iterator_runner
    runner()

    # Act
    file_path = tmp_path / "new_file.txt"
    # Use write_text which opens, writes, and closes the file.
    file_path.write_text("hello")
    time.sleep(0.5) # Wait for event processing

    # Assert
    assert len(events) >= 1
    # Check all events (might be multiple due to on_modified)
    for event in events:
        assert isinstance(event, UpdateEvent)
        assert event.rows[0]['path'] == str(file_path)
    
    # Final event should have correct size
    assert events[-1].rows[0]['size'] == 5



def test_realtime_file_deletion(tmp_path: Path, message_iterator_runner):
    """Tests that deleting a file generates a DeleteEvent."""
    file_path = tmp_path / "delete_me.txt"
    file_path.write_text("delete")
    runner, events, _ = message_iterator_runner
    runner()
    events.clear()

    # Act
    file_path.unlink()
    time.sleep(0.5)
    assert len(events) == 1
    event = events[0]
    assert isinstance(event, DeleteEvent)


def test_realtime_file_move(tmp_path: Path, message_iterator_runner):
    """Tests that moving a file generates a DeleteEvent and an UpdateEvent."""
    src_path = tmp_path / "source.txt"
    dest_path = tmp_path / "destination.txt"
    src_path.write_text("move it")
    time.sleep(0.5)
    runner, events, _ = message_iterator_runner
    runner()
    events.clear()
    # Act
    src_path.rename(dest_path)
    time.sleep(0.5)
    # Assert
    assert len(events) == 2
    delete_events = [e for e in events if isinstance(e, DeleteEvent)]
    update_events = [e for e in events if isinstance(e, UpdateEvent)]

    assert len(delete_events) == 1
    assert len(update_events) == 1
    assert delete_events[0].rows[0]['path'] == str(src_path)
    assert update_events[0].rows[0]['path'] == str(dest_path)


def test_iterator_ignores_old_events(tmp_path: Path, message_iterator_runner):
    """Tests that the iterator correctly filters out events that occurred before
    the start_position timestamp.
    """
    runner, events, _ = message_iterator_runner

    # Create a file far in the past from the iterator's perspective
    old_file = tmp_path / "old.txt"
    old_file.write_text("ancient")
    time.sleep(0.1)

    # Clear the event from the old file creation
    events.clear()

    # Run the iterator, starting from NOW.
    runner(start_pos_offset=0.0)
    time.sleep(0.1) # Extra buffer to ensure start_position is in the past

    # Create a new file, which should be after the start_position
    new_file = tmp_path / "new.txt"
    new_file.write_text("fresh")
    time.sleep(0.5)

    # Assert
    assert len(events) >= 1
    # The last event should be the creation of the new file
    assert isinstance(events[-1], UpdateEvent)
    assert events[-1].rows[0]['path'] == str(new_file)

