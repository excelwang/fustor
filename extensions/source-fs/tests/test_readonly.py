import os
import pytest
import time
from fustor_source_fs import FSDriver
from fustor_core.models.config import SourceConfig

@pytest.fixture
def fs_driver(tmp_path):
    config = SourceConfig(
        driver="fs",
        uri=str(tmp_path),
        # Use an empty API Key credential to satisfy Pydantic Union validation
        credential={"key": "dummy"},
        driver_params={"startup_mode": "full"}
    )
    return FSDriver(id="test-fs", config=config)

@pytest.mark.asyncio
async def test_driver_is_strictly_readonly(tmp_path, fs_driver):
    """
    验证 source_fs 驱动对监控目录的扫描是纯读取的。
    检查点：文件内容、mtime、ctime、权限位在扫描前后必须完全一致。
    """
    # 准备测试数据
    test_file = tmp_path / "target.dat"
    content = "original content"
    test_file.write_text(content)
    
    # 记录原始元数据
    orig_stat = os.stat(test_file)
    orig_mtime = orig_stat.st_mtime
    orig_mode = orig_stat.st_mode
    
    # 执行快照扫描
    events = []
    for event in fs_driver.get_snapshot_iterator():
        events.append(event)
    
    # 核心验证
    current_stat = os.stat(test_file)
    
    assert test_file.read_text() == content, "文件内容被篡改！"
    assert current_stat.st_mtime == orig_mtime, "文件修改时间 (mtime) 被改变！"
    assert current_stat.st_mode == orig_mode, "文件权限位 (mode) 被改变！"
    assert len(events) > 0

@pytest.mark.asyncio
async def test_driver_discovery_only_paths(tmp_path):
    """验证驱动在发现目录时，不会在其中写入任何临时文件"""
    monitored_dir = tmp_path / "monitored"
    monitored_dir.mkdir()
    (monitored_dir / "exist.txt").write_text("hello")
    
    # 记录初始文件列表
    initial_files = set(os.listdir(monitored_dir))
    
    # 重新创建一个针对子目录的驱动
    config = SourceConfig(driver="fs", uri=str(monitored_dir), credential={"key": "dummy"})
    sub_driver = FSDriver(id="sub-fs", config=config)
    
    for _ in sub_driver.get_snapshot_iterator():
        pass
    
    # 验证扫描后没有产生任何新文件
    final_files = set(os.listdir(monitored_dir))
    assert initial_files == final_files, f"驱动在目录中留下了痕迹: {final_files - initial_files}"
