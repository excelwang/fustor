import threading
import time
from pathlib import Path
from typing import List

import pytest

from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_event_model.models import DeleteEvent, UpdateEvent
from fustor_source_fs import FSDriver


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


def test_no_event_during_incomplete_write(tmp_path: Path, message_iterator_runner):
    """Tests that a file being written to continuously doesn't generate premature events."""
    runner, events, _ = message_iterator_runner
    runner()
    events.clear()  # Clear any initial events

    # Act: Open a file for writing but keep it open (not closed yet)
    file_path = tmp_path / "incomplete_write.txt"
    file_handle = open(file_path, 'w')
    file_handle.write("initial content")
    
    # Wait for potential event processing (should not generate an event)
    time.sleep(0.2)

    # Assert: No events should have been generated yet since file is not closed
    assert len(events) == 0, f"Expected 0 events during incomplete write, but got {len(events)}: {events}"

    # Act: Continue writing to the file
    file_handle.write("\nadditional content")
    time.sleep(0.2)

    # Assert: Still no events should have been generated
    assert len(events) == 0, f"Expected 0 events during continued writing, but got {len(events)}: {events}"

    # Act: Close the file, which should trigger the event
    file_handle.close()
    time.sleep(0.2)

    # Assert: Now we should have exactly one UpdateEvent
    assert len(events) == 1, f"Expected 1 event after file close, but got {len(events)}: {events}"
    event = events[0]
    assert isinstance(event, UpdateEvent), f"Expected UpdateEvent, got {type(event)}"
    assert event.rows[0]['file_path'] == str(file_path)


def test_multiple_writes_generate_single_event(tmp_path: Path, message_iterator_runner):
    """Tests that multiple writes to a file generate only one event after the file is closed."""
    runner, events, _ = message_iterator_runner
    runner()
    events.clear()  # Clear any initial events

    # Act: Open a file and write multiple times before closing
    file_path = tmp_path / "multi_write.txt"
    with open(file_path, 'w') as f:
        f.write("first write")
        time.sleep(0.1)
        f.write("\nsecond write")
        time.sleep(0.1)
        f.write("\nthird write")
        time.sleep(0.1)
        # File is automatically closed after exiting 'with' block

    # Wait for event processing
    time.sleep(0.2)

    # Assert: Only one UpdateEvent should be generated despite multiple writes
    assert len(events) == 1, f"Expected 1 event after multiple writes, but got {len(events)}: {events}"
    event = events[0]
    assert isinstance(event, UpdateEvent), f"Expected UpdateEvent, got {type(event)}"
    assert event.rows[0]['file_path'] == str(file_path)
    assert event.rows[0]['size'] > 0, "File size should be greater than 0"


def test_append_mode_writes_generate_single_event(tmp_path: Path, message_iterator_runner):
    """Tests that appending to a file generates a single event after file is closed."""
    runner, events, _ = message_iterator_runner
    runner()
    events.clear()  # Clear any initial events

    # Create the file first
    file_path = tmp_path / "append_test.txt"
    with open(file_path, 'w') as f:
        f.write("initial content")
    
    time.sleep(0.2)
    events.clear()  # Clear any initial events
    
    # Act: Open the file in append mode and add content
    with open(file_path, 'a') as f:
        f.write("\nappended content")
        time.sleep(0.1)
        f.write("\nmore appended content")

    # Wait for event processing
    time.sleep(0.2)

    # Assert: Only one UpdateEvent should be generated despite multiple appends
    assert len(events) == 1, f"Expected 1 event after multiple appends, but got {len(events)}: {events}"
    event = events[0]
    assert isinstance(event, UpdateEvent), f"Expected UpdateEvent, got {type(event)}"
    assert event.rows[0]['file_path'] == str(file_path)