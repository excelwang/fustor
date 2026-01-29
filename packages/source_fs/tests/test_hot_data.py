import os
import time
import threading
import queue
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_source_fs import FSDriver
from fustor_event_model.models import UpdateEvent, MessageSource

@pytest.fixture
def fs_config(tmp_path: Path) -> SourceConfig:
    return SourceConfig(driver="fs", uri=str(tmp_path), credential=PasswdCredential(user="test"))

def test_snapshot_postpones_hot_files(tmp_path: Path, fs_config: SourceConfig, mocker):
    # Create an old file
    old_file = tmp_path / "old.txt"
    old_file.write_text("old")
    os.utime(old_file, (time.time() - 3600, time.time() - 3600))

    # Create a hot file
    hot_file = tmp_path / "hot.txt"
    hot_file.write_text("hot")
    
    driver_config = fs_config
    driver_config.driver_params["hot_data_cooloff_seconds"] = 10.0
    driver = FSDriver('test-fs', driver_config)

    hot_mtime = os.path.getmtime(hot_file)
    mocker.patch.object(driver._logical_clock, 'get_watermark', return_value=hot_mtime + 5.0)
    
    iterator = driver.get_snapshot_iterator()
    events = list(iterator)

    all_rows = []
    for event in events:
        if isinstance(event, UpdateEvent):
            all_rows.extend(event.rows)
    
    paths = {r['file_path'] for r in all_rows}
    assert str(old_file) in paths
    assert str(hot_file) in paths

def test_snapshot_lookback_discards_active_files(tmp_path: Path, fs_config: SourceConfig, mocker):
    file_path = tmp_path / "active.txt"
    file_path.write_text("initial")
    
    driver_config = fs_config
    driver_config.driver_params["hot_data_cooloff_seconds"] = 100.0 # Very hot
    driver = FSDriver('test-fs-active', driver_config)
    
    initial_mtime = os.path.getmtime(file_path)
    # Discovery: hot
    mocker.patch.object(driver._logical_clock, 'get_watermark', return_value=initial_mtime + 1.0)
    
    # We want to mock TWO calls to os.stat for the SAME path.
    # 1. Inside the worker thread (discovery)
    # 2. Inside look-back (main thread)
    
    # We can use a side_effect that checks threading.current_thread()
    # and returns different values for the specific file_path.
    
    class MockStat:
        def __init__(self, mtime):
            self.st_mtime = mtime
            self.st_size = 7
            self.st_mode = 0o100644
            self.st_ctime = mtime
    
    def mocked_stat(path, *args, **kwargs):
        p_str = str(path)
        if p_str == str(file_path):
            if threading.current_thread() is threading.main_thread():
                # Look-back phase
                return MockStat(initial_mtime + 50.0)
            else:
                # Discovery phase (worker thread)
                return MockStat(initial_mtime)
        return os.__original_stat__(path, *args, **kwargs)

    # Save original os.stat
    if not hasattr(os, '__original_stat__'):
        os.__original_stat__ = os.stat
        
    with patch('os.stat', side_effect=mocked_stat):
        iterator = driver.get_snapshot_iterator()
        events = list(iterator)

    all_rows = []
    for event in events:
        if isinstance(event, UpdateEvent):
            all_rows.extend(event.rows)
    
    paths = {r['file_path'] for r in all_rows}
    assert str(file_path) not in paths

def test_audit_silent_directory_skips_files(tmp_path: Path, fs_config: SourceConfig, mocker):
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    f = subdir / "file.txt"
    f.write_text("data")
    
    subdir_mtime = os.path.getmtime(subdir)
    driver = FSDriver('test-fs-silent', fs_config)
    mtime_cache = {str(subdir): subdir_mtime}
    
    iterator = driver.get_audit_iterator(mtime_cache=mtime_cache)
    events_tuples = list(iterator)
    
    skipped_paths = []
    for event, _ in events_tuples:
        if event and isinstance(event, UpdateEvent):
            for row in event.rows:
                if row.get('audit_skipped'):
                    skipped_paths.append(row['file_path'])
    
    assert str(subdir) in skipped_paths
