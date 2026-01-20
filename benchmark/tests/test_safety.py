import os
import pytest
import shutil
import click
from unittest.mock import patch, MagicMock
from fustor_benchmark.generator import DataGenerator
from fustor_benchmark.services import ServiceManager

def test_generator_safety_check(tmp_path):
    """验证 DataGenerator 拒绝在非法路径下执行清理/生成"""
    unsafe_dir = tmp_path / "my-important-data"
    unsafe_dir.mkdir()
    (unsafe_dir / "secret.txt").write_text("don't delete me")
    
    gen = DataGenerator(str(unsafe_dir / "data"))
    
    with patch("click.echo") as mock_echo:
        gen.generate()
        args, _ = mock_echo.call_args
        assert "FATAL: Operation denied" in args[0]
        assert (unsafe_dir / "secret.txt").exists()

def test_service_manager_safety_check(tmp_path):
    """验证 ServiceManager 拒绝在非法路径下部署环境"""
    unsafe_run_dir = str(tmp_path / "production-app")
    os.makedirs(unsafe_run_dir, exist_ok=True)
    
    svc = ServiceManager(unsafe_run_dir)
    
    with patch("click.echo") as mock_echo:
        with pytest.raises(SystemExit) as excinfo:
            svc.setup_env()
        assert excinfo.value.code == 1
        assert "FATAL: Environment setup denied" in mock_echo.call_args[0][0]

def test_successful_operation_on_valid_path(tmp_path):
    """验证符合后缀要求的路径可以正常操作"""
    safe_run_dir = tmp_path / "test-fustor-benchmark-run"
    safe_run_dir.mkdir()
    
    gen = DataGenerator(str(safe_run_dir / "data"))
    gen.generate(num_uuids=1, num_subdirs=1, files_per_subdir=1)
    assert (safe_run_dir / "data").exists()
    
    svc = ServiceManager(str(safe_run_dir))
    svc.setup_env()
    assert (safe_run_dir / ".fustor").exists()

def test_runner_gen_mode_logic(tmp_path):
    """验证 Runner 中的 gen_mode 逻辑分支（仅测试分支逻辑，跳过实际服务启动）"""
    safe_run_dir = tmp_path / "logic-fustor-benchmark-run"
    safe_run_dir.mkdir()
    os.makedirs(safe_run_dir / "data", exist_ok=True)
    (safe_run_dir / "data" / "dummy.txt").write_text("data")
    
    from fustor_benchmark.runner import BenchmarkRunner
    runner = BenchmarkRunner(str(safe_run_dir))
    
    # 彻底 Mock 掉 runner.run 内部 try 块中的逻辑，我们只关心 try 之前的 gen_mode 判定
    # 我们使用 patch 来拦截 runner.services.setup_env 及其之后的一切逻辑
    with patch.object(runner.services, "setup_env", side_effect=RuntimeError("STOP_HERE")):
        with patch.object(runner.generator, "generate") as mock_gen:
            
            # 1. 测试 skip 模式：应当不调用 generate
            try:
                runner.run(gen_mode='skip')
            except RuntimeError as e:
                if str(e) != "STOP_HERE": raise
            mock_gen.assert_not_called()
            
            # 2. 测试 force 模式：应当调用 generate
            try:
                runner.run(gen_mode='force')
            except RuntimeError as e:
                if str(e) != "STOP_HERE": raise
            mock_gen.assert_called_once()
            
            # 3. 测试 auto 模式（数据已存在）：应当不调用 generate
            mock_gen.reset_mock()
            try:
                runner.run(gen_mode='auto')
            except RuntimeError as e:
                if str(e) != "STOP_HERE": raise
            mock_gen.assert_not_called()