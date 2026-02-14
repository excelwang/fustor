import os
import time
import pytest
from unittest.mock import MagicMock, patch
from fustor_source_fs.driver import FSDriver
from fustor_core.models.config import SourceConfig
from fustor_core.event.types import MessageSource
from fustor_core.event.base import UpdateEvent

@pytest.fixture
def source_uri(tmp_path):
    d = tmp_path / "source"
    d.mkdir()
    (d / "file1.txt").write_text("content")
    (d / "subdir").mkdir()
    (d / "subdir/file2.txt").write_text("content2")
    return str(d)

@pytest.fixture
def mock_driver(source_uri):
    config = SourceConfig(driver="fs", uri=source_uri)
    driver = FSDriver(id="test-fs", config=config)
    return driver

def test_scan_path_single_file(mock_driver, source_uri):
    """测试对单个文件进行按需扫描"""
    file_path = os.path.join(source_uri, "file1.txt")
    events = list(mock_driver.scan_path(file_path))
    
    assert len(events) == 1
    assert events[0].message_source == MessageSource.ON_DEMAND_JOB
    assert events[0].rows[0]["path"] == "/file1.txt"

def test_scan_path_directory_recursive(mock_driver, source_uri):
    """测试递归扫描目录"""
    events = list(mock_driver.scan_path(source_uri, recursive=True))
    
    # 应该包含 file1.txt, subdir, subdir/file2.txt
    paths = [row["path"] for e in events for row in e.rows]
    assert "/file1.txt" in paths
    assert "/subdir" in paths
    assert "/subdir/file2.txt" in paths

def test_scan_path_directory_non_recursive(mock_driver, source_uri):
    """测试非递归扫描目录（仅返回目录本身元数据）"""
    events = list(mock_driver.scan_path(source_uri, recursive=False))
    
    assert len(events) == 1
    assert events[0].rows[0]["path"] == "/"
    assert events[0].rows[0]["is_directory"] is True

def test_scan_path_outside_uri(mock_driver):
    """测试扫描 URI 之外的路径"""
    events = list(mock_driver.scan_path("/etc/passwd"))
    assert len(events) == 0

def test_scan_path_nonexistent(mock_driver, source_uri):
    """测试扫描不存在的路径"""
    events = list(mock_driver.scan_path(os.path.join(source_uri, "nonexistent")))
    assert len(events) == 0

def test_perform_sentinel_check(mock_driver, source_uri):
    """测试哨兵检查流程"""
    task = {
        "type": "suspect_check",
        "paths": ["/file1.txt", "/nonexistent.txt"]
    }
    
    result = mock_driver.perform_sentinel_check(task)
    assert result["type"] == "suspect_update"
    updates = result["updates"]
    
    # file1.txt 应该存在，nonexistent 应该返回 status='missing'
    paths_status = {u["path"]: u["status"] for u in updates}
    assert paths_status["/file1.txt"] == "exists"
    assert paths_status["/nonexistent.txt"] == "missing"

def test_perform_sentinel_check_unknown_type(mock_driver):
    """测试位置类型的哨兵检查"""
    assert mock_driver.perform_sentinel_check({"type": "unknown"}) == {}
