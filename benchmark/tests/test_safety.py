import os
import pytest
import shutil
import click
from unittest.mock import patch, MagicMock
from fustor_benchmark.generator import DataGenerator
from fustor_benchmark.services import ServiceManager

def test_generator_safety_check(tmp_path):
    """验证 DataGenerator 拒绝在非法路径下执行清理/生成"""
    # 创建一个不符合后缀要求的目录
    unsafe_dir = tmp_path / "my-important-data"
    unsafe_dir.mkdir()
    (unsafe_dir / "secret.txt").write_text("don't delete me")
    
    # 尝试初始化并生成
    gen = DataGenerator(str(unsafe_dir / "data"))
    
    # 模拟 click.echo 以捕获输出
    with patch("click.echo") as mock_echo:
        gen.generate()
        
        # 验证输出了 FATAL 错误信息
        args, _ = mock_echo.call_args
        assert "FATAL: Operation denied" in args[0]
        
        # 验证文件依然存在（未被删除）
        assert (unsafe_dir / "secret.txt").exists()

def test_service_manager_safety_check(tmp_path):
    """验证 ServiceManager 拒绝在非法路径下部署环境"""
    unsafe_run_dir = str(tmp_path / "production-app")
    os.makedirs(unsafe_run_dir, exist_ok=True)
    
    svc = ServiceManager(unsafe_run_dir)
    
    # 期望 sys.exit(1)
    with patch("click.echo") as mock_echo:
        with pytest.raises(SystemExit) as excinfo:
            svc.setup_env()
        
        assert excinfo.value.code == 1
        assert "FATAL: Environment setup denied" in mock_echo.call_args[0][0]

def test_successful_operation_on_valid_path(tmp_path):
    """验证符合后缀要求的路径可以正常操作"""
    safe_run_dir = tmp_path / "test-fustor-benchmark-run"
    safe_run_dir.mkdir()
    
    # 1. 测试 Generator
    gen = DataGenerator(str(safe_run_dir / "data"))
    gen.generate(num_uuids=1, num_subdirs=1, files_per_subdir=1)
    assert (safe_run_dir / "data").exists()
    
    # 2. 测试 ServiceManager (部分 Mock 掉启动逻辑)
    svc = ServiceManager(str(safe_run_dir))
    svc.setup_env()
    assert (safe_run_dir / ".fustor").exists()
