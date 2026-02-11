import time
from unittest.mock import MagicMock, patch
from pathlib import Path
import pytest

from fustor_core.models.config import PasswdCredential, SourceConfig
from fustor_source_fs import FSDriver

@pytest.fixture
def fs_config(tmp_path: Path) -> SourceConfig:
    return SourceConfig(driver="fs", uri=str(tmp_path), credential=PasswdCredential(user="test"))

def test_drift_calculation_filters_future_outliers(fs_config, tmp_path):
    """
    Test that the drift calculation correctly identifies the 'stable' mtime
    by filtering out future timestamp outliers even with a small number of directories.
    """
    # Create several directories with 'normal' times (now) and one with a 'future' time.
    now = time.time()
    future = now + 1000000  # 1 million seconds in the future
    
    dir_mtime_map = {}
    for i in range(10):
        d = tmp_path / f"dir{i}"
        d.mkdir()
        dir_mtime_map[str(d)] = now
    
    # Add one 'future' directory
    future_dir = tmp_path / "future_dir"
    future_dir.mkdir()
    dir_mtime_map[str(future_dir)] = future
    
    # Initialize driver
    driver = FSDriver('test-drift', fs_config)
    
    # Mock FSScanner to return our controlled data
    with patch('fustor_source_fs.driver.FSScanner') as MockScanner:
        mock_scanner_instance = MockScanner.return_value
        # dir_mtime_map, dir_children_map, total_entries, error_count
        mock_scanner_instance.perform_pre_scan.return_value = (dir_mtime_map, {}, 11, 0)
        
        # Act
        driver._perform_pre_scan_and_schedule()
        
    # Assert
    # The stable mtime should be 'now', not 'future'
    # N=11. idx = max(0, int(11 * 0.99) - 1) = max(0, 10 - 1) = 9.
    # mtimes = [now, now, ..., now, future] (10 nows, 1 future)
    # index 9 is 'now'.
    
    # Allow small precision difference due to internal processing
    assert abs(driver.drift_from_nfs - (now - time.time())) < 5.0
    # Crucially, it should NOT be near future - time.time()
    assert driver.drift_from_nfs < 100.0 # Should be near 0

def test_drift_calculation_handles_very_small_set(fs_config, tmp_path):
    """Test that N=1 works correctly."""
    now = time.time()
    dir_mtime_map = {str(tmp_path): now}
    
    driver = FSDriver('test-drift-small', fs_config)
    with patch('fustor_source_fs.driver.FSScanner') as MockScanner:
        mock_scanner_instance = MockScanner.return_value
        mock_scanner_instance.perform_pre_scan.return_value = (dir_mtime_map, {}, 1, 0)
        driver._perform_pre_scan_and_schedule()
        
    assert abs(driver.drift_from_nfs - (now - time.time())) < 5.0
