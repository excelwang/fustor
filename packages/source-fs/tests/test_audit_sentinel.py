import pytest
import os
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from fustor_source_fs import FSDriver
from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_core.event import UpdateEvent

@pytest.fixture
def fsdriver(tmp_path):
    config = SourceConfig(
        driver="fs",
        uri=str(tmp_path),
        credential=PasswdCredential(user="test")
    )
    return FSDriver("test-fs", config)

def test_audit_iterator_detects_changes(fsdriver, tmp_path):
    """Test that audit iterator detects a modified file."""
    # Create a file
    file1 = tmp_path / "file1.txt"
    file1.write_text("initial")
    
    # Wait a bit to ensure mtime difference
    time.sleep(0.1)
    
    # Record mtime for "True Silence" cache
    mtime_cache = {str(file1): os.path.getmtime(file1)}
    
    # Modify file
    file1.write_text("modified")
    
    # Run audit
    fsdriver.config.driver_params["hot_data_cooloff_seconds"] = 0
    
    iterator = fsdriver.get_audit_iterator(mtime_cache=mtime_cache)
    results = list(iterator)
    
    found = False
    for event_tuple in results:
        event, mtime_map = event_tuple
        if event and hasattr(event, "rows"):
            for row in event.rows:
                if row['file_path'] == str(file1):
                    found = True
    assert found, "Modified file should be detected by audit"

def test_audit_iterator_skips_unchanged(fsdriver, tmp_path):
    """Test that audit iterator skips a file that hasn't changed."""
    file1 = tmp_path / "file1.txt"
    file1.write_text("stable")
    
    mtime_cache = {
        str(file1): os.path.getmtime(file1),
        str(tmp_path): os.path.getmtime(tmp_path)
    }
    
    iterator = fsdriver.get_audit_iterator(mtime_cache=mtime_cache)
    results = list(iterator)
    
    for event_tuple in results:
        event, mtime_map = event_tuple
        if event and hasattr(event, "rows"):
            for row in event.rows:
                assert row['file_path'] != str(file1)

def test_perform_sentinel_check_verify_files(fsdriver, tmp_path):
    """Test that sentinel check correctly verifies file existence and gets mtime."""
    file1 = tmp_path / "verify_me.txt"
    file1.write_text("content")
    st = os.stat(file1)
    
    # 1. Test existing file
    batch = {
        "type": "suspect_check",
        "paths": [str(file1)]
    }
    
    result_batch = fsdriver.perform_sentinel_check(batch)
    updates = result_batch["updates"]
    
    # Find results for file1
    res = next(u for u in updates if u["path"] == str(file1))
    assert res["status"] == "exists"
    assert res["mtime"] == st.st_mtime
    
    # 2. Test missing file
    missing_file = str(tmp_path / "nonexistent.txt")
    batch = {
        "type": "suspect_check",
        "paths": [missing_file]
    }
    result_batch = fsdriver.perform_sentinel_check(batch)
    updates = result_batch["updates"]
    res = next(u for u in updates if u["path"] == missing_file)
    assert res["status"] == "missing"
