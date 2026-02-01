"""
Test cases for FTP scenarios where a client experiences communication failure
during a large file transfer, verifying that our file system monitoring 
still captures the event appropriately.
"""
import os
import tempfile
import time
import threading
from pathlib import Path
from typing import List
import subprocess
import sys

import pytest

from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_core.event import UpdateEvent
from fustor_source_fs import FSDriver


def simulate_ftp_transfer_interruption():
    """
    Simulates what happens when an FTP client experiences a communication 
    failure during a large file transfer.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create a Python script simulating an FTP server behavior
        # where a file is opened for writing, receives partial data, 
        # then connection is aborted
        script_content = f'''
import time
import os
import sys

# Simulate a large file transfer with interruption
file_path = "{temp_path}/ftp_transfer_interrupted.bin"

# Open file for writing (like FTP server would do)
with open(file_path, "wb") as f:
    print("FTP server: Started writing file for client")
    
    # Write partial content (simulate large file transfer)
    chunk_size = 1024  # 1KB chunks
    total_chunks = 100  # 100KB total if completed
    completed_chunks = 0
    
    try:
        for i in range(total_chunks):
            # Simulate receiving data from FTP client
            data = b"x" * chunk_size  # Simulated data chunk
            f.write(data)
            f.flush()  # Simulate periodic flush
            
            completed_chunks += 1
            print(f"FTP server: Wrote chunk {{i+1}}/{{total_chunks}}")
            
            # Simulate network delay
            time.sleep(0.01)
            
            # Simulate network interruption after 30 chunks
            if i == 29:  # About 30KB written
                print("FTP server: Network interruption detected, connection aborted!")
                # Note: We're not explicitly closing the file here to simulate
                # the connection being abruptly closed
                # But in real FTP server, the connection drop would trigger file close
                break
                
    except Exception as e:
        print(f"FTP server: Error during transfer: {{e}}")
    
    # File will be closed automatically when exiting 'with' block
    print(f"FTP server: Transfer ended after {{completed_chunks}} chunks")

# Exit abruptly to simulate client disconnection
print("FTP server process ending")
'''
        
        script_file = temp_path / "ftp_sim_script.py"
        with open(script_file, 'w') as f:
            f.write(script_content)

        # Setup the file system monitor
        events: List = []
        fs_config = SourceConfig(driver="fs", uri=str(temp_path), credential=PasswdCredential(user="test"))
        driver = FSDriver('ftp-test-id', fs_config)
        driver.watch_manager.schedule(str(temp_path), time.time())

        stop_event = threading.Event()
        
        def run_monitor():
            start_time = int(time.time() * 1000)
            # Use the new interface that returns only the iterator
            iterator = driver.get_message_iterator(start_position=start_time, stop_event=stop_event)
            for event in iterator:
                events.append(event)
                print(f"Monitor: Captured event - {{type(event).__name__}} for {{event.rows[0]['file_path'] if event.rows else 'N/A'}}")
                # Collect events for a reasonable period
                if time.time() * 1000 - start_time > 10000:  # 10 seconds max
                    break

        monitor_thread = threading.Thread(target=run_monitor)
        monitor_thread.start()
        
        # Wait a moment for the monitor to start
        time.sleep(0.2)

        # Run the FTP simulation script
        try:
            result = subprocess.run([sys.executable, str(script_file)], 
                                   capture_output=True, text=True, timeout=10)
            print(f"FTP sim return code: {result.returncode}")
            print(f"FTP sim stdout: {result.stdout}")
            print(f"FTP sim stderr: {result.stderr}")
        except subprocess.TimeoutExpired:
            print("FTP sim timed out")

        # Wait for potential events to be processed
        time.sleep(1.5)

        # Stop the monitor
        stop_event.set()
        monitor_thread.join(timeout=3)
        driver.watch_manager.stop()

        # Analyze the results
        print(f"\\n=== FTP Transfer Interruption Test Results ===")
        print(f"Events collected: {len(events)}")
        
        for i, event in enumerate(events):
            print(f"Event {i}: {type(event).__name__}")
            if hasattr(event, 'rows') and event.rows:
                row = event.rows[0]
                print(f"  - File: {row.get('file_path', 'N/A')}")
                print(f"  - Size: {row.get('size', 'N/A')} bytes")
                print(f"  - Modified: {time.ctime(row.get('modified_time', 0)) if 'modified_time' in row else 'N/A'}")
        
        # Check if we got an update event for the interrupted transfer
        update_events = [e for e in events if isinstance(e, UpdateEvent)]
        ftp_file_events = [e for e in update_events 
                          if e.rows and any('ftp_transfer_interrupted' in row.get('file_path', '') for row in e.rows)]
        
        print(f"\\nUpdate events total: {len(update_events)}")
        print(f"Events for FTP interrupted file: {len(ftp_file_events)}")
        
        if ftp_file_events:
            file_event = ftp_file_events[0]
            file_size = file_event.rows[0]['size'] if file_event.rows else 0
            print(f"SUCCESS: Update event captured for interrupted FTP transfer!")
            print(f"File size at interruption: {file_size} bytes")
            print(f"This represents partial file content as expected after interruption")
            return True
        else:
            print("No update event captured for interrupted FTP transfer")
            return False


def test_ftp_partial_transfer_capture():
    """Test that our system captures events even for FTP transfers that are interrupted."""
    success = simulate_ftp_transfer_interruption()
    assert success, "FTP transfer interruption should generate an update event"


if __name__ == "__main__":
    simulate_ftp_transfer_interruption()