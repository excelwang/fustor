import os
import pytest
import shutil
import click
from unittest.mock import patch, MagicMock
from fustor_benchmark.generator import DataGenerator
from fustor_benchmark.services import ServiceManager

def test_generator_safety_check(tmp_path):
    """验证 DataGenerator 拒绝在非法路径下执行清理/生成"""
    unsafe_dir = str(tmp_path / "my-important-data")
    os.makedirs(unsafe_dir, exist_ok=True)
    with open(os.path.join(unsafe_dir, "secret.txt"), "w") as f:
        f.write("don't delete me")
    
    gen = DataGenerator(os.path.join(unsafe_dir, "data"))
    
    with patch("click.echo") as mock_echo:
        gen.generate()
        args, _ = mock_echo.call_args
        assert "FATAL: Operation denied" in args[0]
        assert os.path.exists(os.path.join(unsafe_dir, "secret.txt"))

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
    """验证符合后缀要求的路径可以正常操作且具备非空保护"""
    safe_root = tmp_path / "test-fustor-benchmark-run"
    safe_root.mkdir()
    data_dir = safe_root / "data"
    
    # 1. 首次生成 (目录不存在) -> 成功
    gen = DataGenerator(str(data_dir))
    gen.generate(num_uuids=1, num_subdirs=1, files_per_subdir=1)
    assert data_dir.exists()
    assert len(os.listdir(data_dir)) > 0
    
    # 2. 再次生成 (目录不为空) -> 被拦截
    with patch("click.echo") as mock_echo:
        gen.generate()
        # 检查是否包含 FATAL: Target directory ... is NOT empty
        found_error = any("is NOT empty" in call[0][0] for call in mock_echo.call_args_list)
        assert found_error

def test_runner_gen_mode_logic(tmp_path, monkeypatch):
    """验证 Runner 中的逻辑"""
    safe_root = tmp_path / "work-dir"
    safe_root.mkdir()
    monkeypatch.chdir(safe_root)
    
    safe_run_dir = safe_root / "fustor-benchmark-run"
    safe_run_dir.mkdir()
    os.makedirs(safe_run_dir / "data", exist_ok=True)
    (safe_run_dir / "data" / "dummy.txt").write_text("data")
    
    from fustor_benchmark.runner import BenchmarkRunner
    runner = BenchmarkRunner(str(safe_run_dir), target_dir=str(safe_run_dir / "data"))
    
    with patch.object(runner.services, "setup_env", side_effect=RuntimeError("STOP_HERE")):
        try:
            runner.run()
        except RuntimeError as e:
            if str(e) != "STOP_HERE": raise