import asyncio
import pytest
import os
import signal
import yaml
import shutil
from unittest.mock import MagicMock, AsyncMock, patch
from pathlib import Path

from fustor_agent.runtime.agent_pipe import AgentPipe
from fustor_core.pipe.pipe import PipeState

@pytest.fixture(autouse=True)
def mock_os_kill():
    with patch("os.kill") as m:
        yield m

@pytest.fixture
def mock_pipe():
    config = {
        "source": "src",
        "sender": "push",
        "batch_size": 2
    }
    source_h = MagicMock()
    sender_h = AsyncMock()
    pipe = AgentPipe("test-pipe", "agent:test-pipe", config, source_h, sender_h)
    pipe.session_id = "s123"
    pipe.state = PipeState.RUNNING
    return pipe

@pytest.mark.asyncio
async def test_handle_unknown_command(mock_pipe, caplog):
    """测试未知命令处理"""
    await mock_pipe._handle_commands([{"type": "unknown_action"}])
    assert "Unknown command type 'unknown_action'" in caplog.text

@pytest.mark.asyncio
async def test_handle_command_scan_unsupported(mock_pipe, caplog):
    """测试源处理器不支持扫描时的处理"""
    del mock_pipe.source_handler.scan_path # 确保没有该属性
    await mock_pipe._handle_command_scan({"path": "/tmp", "job_id": "j1"})
    assert "Source handler does not support 'scan_path'" in caplog.text

@pytest.mark.asyncio
async def test_run_on_demand_job_success(mock_pipe):
    """测试按需扫描作业流程"""
    mock_pipe.source_handler.scan_path.return_value = [
        {"id": "e1"}, {"id": "e2"}, {"id": "e3"}
    ]
    
    await mock_pipe._run_on_demand_job("/tmp", recursive=True, job_id="j1")
    
    # 验证发送了 2 个批次 (batch_size=2) + 1 个完成通知
    # 批次 1: [e1, e2]
    # 批次 2: [e3]
    # 批次 3: [] (job_complete)
    assert mock_pipe.sender_handler.send_batch.call_count == 3
    
    # 验证完成通知包含 job_id
    last_call = mock_pipe.sender_handler.send_batch.call_args_list[-1]
    assert last_call[0][2]["phase"] == "job_complete"
    assert last_call[0][2]["metadata"]["job_id"] == "j1"

@pytest.mark.asyncio
async def test_handle_command_reload(mock_pipe):
    """测试远程重载命令"""
    with patch("os.kill") as mock_kill, patch("os.getpid", return_value=999):
        mock_pipe._handle_command_reload()
        mock_kill.assert_called_with(999, signal.SIGHUP)

@pytest.mark.asyncio
async def test_handle_command_update_config_invalid_yaml(mock_pipe, caplog):
    """测试非法 YAML 的配置更新"""
    await mock_pipe._handle_commands([{"type": "update_config", "config_yaml": "invalid: : yaml"}])
    assert "Received invalid YAML syntax" in caplog.text

def test_handle_command_update_config_semantic_error(mock_pipe, caplog):
    """测试语义错误的配置更新（冗余 Pipe 对）"""
    bad_config = yaml.dump({
        "pipes": {
            "p1": {"source": "s1", "sender": "push"},
            "p2": {"source": "s1", "sender": "push"}
        }
    })
    mock_pipe._handle_command_update_config({"config_yaml": bad_config})
    assert "Received semantically invalid config" in caplog.text
    assert "Redundant configuration" in caplog.text

@pytest.mark.asyncio
async def test_handle_command_update_config_success(mock_pipe, tmp_path, caplog):
    """测试配置更新成功路径（包含备份）"""
    mock_home = tmp_path / "fustor"
    config_dir = mock_home / "agent-config"
    config_dir.mkdir(parents=True)
    
    target_file = config_dir / "test.yaml"
    target_file.write_text("old config")
    
    valid_config = yaml.dump({
        "agent_id": "test-agent",
        "sources": {"s1": {"driver": "fs", "uri": "file:///tmp"}},
        "senders": {"p1": {"driver": "http", "uri": "http://fusion"}},
        "pipes": {"p1": {"source": "s1", "sender": "p1"}}
    })
    
    with patch("fustor_core.common.get_fustor_home_dir", return_value=mock_home), \
         patch("os.kill") as mock_kill:
        
        mock_pipe._handle_command_update_config({
            "config_yaml": valid_config,
            "filename": "test.yaml"
        })
        
        # 验证备份存在
        assert (config_dir / "test.yaml.bak").read_text() == "old config"
        # 验证新配置已写入
        assert target_file.read_text() == valid_config
        # 验证触发了重载
        mock_kill.assert_called_once()
        assert "Config updated and reload triggered" in caplog.text

@pytest.mark.asyncio
async def test_handle_command_upgrade_failure(mock_pipe, caplog):
    """测试升级失败（pip 返回非 0）"""
    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"", b"pip error"))
    mock_process.returncode = 1
    
    with patch("asyncio.create_subprocess_exec", return_value=mock_process):
        await mock_pipe._handle_command_upgrade({"version": "1.0.0"})
        assert "Upgrade failed" in caplog.text

@pytest.mark.asyncio
async def test_handle_command_upgrade_success(mock_pipe):
    """测试升级成功并执行 execv"""
    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(b"done", b""))
    mock_process.returncode = 0
    
    with patch("asyncio.create_subprocess_exec", return_value=mock_process), \
         patch("os.execv") as mock_execv, \
         patch("sys.executable", "python"), \
         patch("sys.argv", ["agent.py"]):
        
        await mock_pipe._handle_command_upgrade({"version": "2.0.0"})
        
        # 验证尝试关闭 Session
        mock_pipe.sender_handler.close_session.assert_called_once()
        # 验证执行了重启
        mock_execv.assert_called_once_with("python", ["python", "agent.py"])

@pytest.mark.asyncio
async def test_handle_command_report_config_success(mock_pipe, tmp_path):
    """测试配置报告命令"""
    mock_home = tmp_path / "fustor"
    config_dir = mock_home / "agent-config"
    config_dir.mkdir(parents=True)
    (config_dir / "default.yaml").write_text("my config")
    
    with patch("fustor_core.common.get_fustor_home_dir", return_value=mock_home):
        mock_pipe._handle_command_report_config({"filename": "default.yaml"})
        
        # 验证发送了包含配置内容的批次
        await asyncio.sleep(0.01) # 等待 create_task
        mock_pipe.sender_handler.send_batch.assert_called_once()
        args = mock_pipe.sender_handler.send_batch.call_args[0]
        assert args[2]["phase"] == "config_report"
        assert args[2]["metadata"]["config_yaml"] == "my config"

@pytest.mark.asyncio
async def test_handle_command_stop_pipe_matches(mock_pipe):
    """测试停止当前 Pipe 命令"""
    with patch.object(mock_pipe, "stop", new_callable=AsyncMock) as mock_stop:
        await mock_pipe._handle_command_stop_pipe({"pipe_id": "test-pipe"})
        await asyncio.sleep(0.01)
        mock_stop.assert_called_once()

@pytest.mark.asyncio
async def test_handle_command_stop_pipe_no_match(mock_pipe, caplog):
    """测试停止其他 Pipe 命令（应忽略）"""
    with patch.object(mock_pipe, "stop", new_callable=AsyncMock) as mock_stop:
        await mock_pipe._handle_command_stop_pipe({"pipe_id": "other-pipe"})
        assert "not me, ignoring" in caplog.text
        mock_stop.assert_not_called()
