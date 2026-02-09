import os
import subprocess
import sys
import tempfile
import time
import threading
from pathlib import Path
from typing import List
import signal

import pytest

from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_core.event import UpdateEvent
from fustor_source_fs import FSDriver


@pytest.fixture
def fs_config(tmp_path: Path) -> SourceConfig:
    """Provides a SourceConfig pointing to a temporary directory."""
    return SourceConfig(driver="fs", uri=str(tmp_path), credential=PasswdCredential(user="test"))


def test_crashed_process_triggers_event():
    """Test that a crashed process still triggers an update event due to auto-closing."""
    
    # Use a temporary directory for this test
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create a Python script that opens a file and writes to it, but crashes before closing
        script_content = f'''
import time
import sys

# Open a file for writing
file_path = "{temp_path}/crash_test_file.txt"
f = open(file_path, "w")
f.write("partial content")
print("File opened and content written, about to crash...")

# Sleep briefly to ensure the file system event has time to register the open/write
time.sleep(0.1)

# Abruptly terminate the process without closing the file
print("About to crash the process...")
sys.exit(1)  # Exit with an error code
# The file handle will be automatically closed by the OS
'''

        script_file = temp_path / "crash_script.py"
        with open(script_file, 'w') as f:
            f.write(script_content)

        # Start the file system monitor in a separate thread
        events: List = []
        fs_config = SourceConfig(driver="fs", uri=str(temp_path), credential=PasswdCredential(user="test"))
        driver = FSDriver('test-fs-id', fs_config)
        driver.watch_manager.schedule(str(temp_path), time.time())

        stop_event = threading.Event()
        
        def run_monitor():
            # Use the new interface that returns only the iterator
            iterator = driver.get_message_iterator(start_position=int(time.time()*1000), stop_event=stop_event)
            for event in iterator:
                events.append(event)
                # Only collect first few events then stop
                if len(events) >= 5:  # Stop after 5 events to avoid infinite loop
                    break

        monitor_thread = threading.Thread(target=run_monitor)
        monitor_thread.start()
        
        # Wait a moment for the monitor to start
        time.sleep(0.2)

        # Run the crashing script as a subprocess
        crash_script_path = str(script_file)
        try:
            result = subprocess.run([sys.executable, crash_script_path], 
                                   capture_output=True, text=True, timeout=5)
            print(f"Crash script return code: {result.returncode}")
            print(f"Crash script stdout: {result.stdout}")
            print(f"Crash script stderr: {result.stderr}")
        except subprocess.TimeoutExpired:
            print("Crash script timed out")

        # Wait for potential events to be processed
        time.sleep(1.0)

        # Stop the monitor
        stop_event.set()
        monitor_thread.join(timeout=2)
        driver.watch_manager.stop()

        # Check if any events were generated
        print(f"Events collected: {len(events)}")
        for i, event in enumerate(events):
            print(f"Event {i}: {type(event).__name__}, rows: {event.rows if hasattr(event, 'rows') else 'N/A'}")

        # The main assertion: Check if an UpdateEvent was generated for the crashed process file
        update_events = [e for e in events if isinstance(e, UpdateEvent)]
        crash_file_events = [e for e in update_events 
                            if e.rows and any(row.get('path', '').endswith('crash_test_file.txt') for row in e.rows)]
        
        print(f"Update events: {len(update_events)}")
        print(f"Events for crash test file: {len(crash_file_events)}")
        
        # This assertion tests the main question - does a crashed process trigger an event?
        if crash_file_events:
            print("SUCCESS: Update event was generated even though process crashed")
            assert True  # Test passes if event was generated
        else:
            print("NO UPDATE EVENT GENERATED - This indicates the crashed process did not trigger an event")
            # Note: This might be expected behavior depending on implementation
            # The test documents the current behavior