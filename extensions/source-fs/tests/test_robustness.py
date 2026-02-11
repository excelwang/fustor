import os
import pytest
import shutil
import threading
import time
from unittest.mock import MagicMock, patch
from fustor_source_fs.driver import FSDriver
from fustor_source_fs.components import _WatchManager
from fustor_source_fs.scanner import FSScanner
from fustor_core.models.config import SourceConfig
from fustor_core.exceptions import DriverError

@pytest.fixture
def temp_test_dir(tmp_path):
    path = tmp_path / "robustness_test"
    path.mkdir()
    return str(path)

@pytest.fixture
def mock_config(temp_test_dir):
    return SourceConfig(
        driver="fs",
        uri=temp_test_dir,
        driver_params={"max_scan_workers": 2}
    )

@pytest.mark.asyncio
async def test_missing_root_at_startup(tmp_path):
    """测试根目录在初始化时不存在，Agent 是否存活"""
    missing_path = str(tmp_path / "non_existent_root")
    config = SourceConfig(driver="fs", uri=missing_path)
    
    # 不应该抛出 FileNotFoundError
    driver = FSDriver("test-id-startup", config)
    # 重置实例状态以便测试（因为 FSDriver 有单例缓存）
    driver._initialized = False
    driver.__init__("test-id-startup", config)
    
    assert driver.watch_manager.inotify is None
    
    # 模拟启动
    driver.watch_manager.start()
    assert driver.watch_manager.inotify_thread.is_alive()
    
    # 路径出现后应该能恢复
    os.makedirs(missing_path)
    # 给 event_loop 运行时间
    time.sleep(1.1) 
    
    # 此时 inotify 应该已经被初始化了
    assert driver.watch_manager.inotify is not None
    await driver.close()

def test_permission_denied_during_scan(temp_test_dir, mock_config):
    """测试扫描过程中遇到权限不足的子目录"""
    # 模拟权限错误
    with patch("os.scandir", side_effect=PermissionError("Mock Permission Denied")):
        scanner = FSScanner(temp_test_dir)
        # 应该优雅地捕捉错误并计数
        mtime_map, children_map, count, errors = scanner.perform_pre_scan()
        assert errors > 0

@pytest.mark.asyncio
async def test_inotify_limit_reached(temp_test_dir, mock_config):
    """测试系统 inotify 达到上限 (ENOSPC)"""
    driver = FSDriver("test-id-limit", mock_config)
    
    # 模拟 Inotify.add_watch 抛出 ENOSPC (errno 28)
    with patch("watchdog.observers.inotify_c.Inotify.add_watch") as mock_add:
        mock_add.side_effect = OSError(28, "No space left on device")
        
        # 默认 30 天窗口，0 天文件必然触发 DriverError 保护
        with pytest.raises(DriverError):
            driver.watch_manager.schedule(temp_test_dir)
        
    await driver.close()

def test_file_disappears_during_snapshot(temp_test_dir):
    """测试快照扫描时，文件突然消失（竞态条件）"""
    scanner = FSScanner(temp_test_dir)
    
    # 模拟在 os.stat 时文件消失
    with patch("os.stat", side_effect=FileNotFoundError()):
        # 应该优雅跳过
        events = list(scanner.scan_snapshot("fs"))
        assert len(events) == 0

def test_invalid_encoding_path(temp_test_dir):
    """测试包含非法编码/无法解码的路径名"""
    with patch("os.scandir") as mock_scan:
        mock_entry = MagicMock()
        mock_entry.path = os.path.join(temp_test_dir, "bad_path")
        mock_entry.name = "bad_path"
        mock_entry.is_dir.side_effect = UnicodeEncodeError('utf-8', u'', 0, 1, 'mock')
        
        # 只要不 crash
        scanner = FSScanner(temp_test_dir)
        list(scanner.perform_pre_scan())

@pytest.mark.asyncio
async def test_file_replaces_directory_runtime(temp_test_dir, mock_config):
    """测试原本是目录的路径在运行时被替换成了文件 (ENOTDIR)"""
    driver = FSDriver("test-id-replace", mock_config)
    
    with patch("watchdog.observers.inotify_c.Inotify.add_watch") as mock_add:
        mock_add.side_effect = OSError(20, "Not a directory")
        
        # 不应该抛出异常，驱动应跳过
        driver.watch_manager.schedule(temp_test_dir)
    
    await driver.close()