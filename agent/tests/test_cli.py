import pytest
from click.testing import CliRunner
from fustor_agent.cli import cli
from unittest.mock import patch, MagicMock

def test_cli_validate_success():
    runner = CliRunner()
    with patch("fustor_agent.config.validator.ConfigValidator.validate", return_value=(True, [])):
        result = runner.invoke(cli, ["validate"])
        assert result.exit_code == 0
        assert "Configuration is valid" in result.output

def test_cli_validate_failure():
    runner = CliRunner()
    with patch("fustor_agent.config.validator.ConfigValidator.validate", return_value=(False, ["Error 1"])):
        result = runner.invoke(cli, ["validate"])
        assert result.exit_code == 1
        assert "Configuration has 1 errors" in result.output
        assert "Error 1" in result.output

def test_cli_list_pipes():
    runner = CliRunner()
    mock_pipes = {"pipe1": MagicMock(source="src1")}
    with patch("fustor_agent.config.unified.agent_config.get_all_pipes", return_value=mock_pipes):
        with patch("fustor_agent.config.unified.agent_config.get_source", return_value=MagicMock(disabled=False)):
            result = runner.invoke(cli, ["list"])
            assert result.exit_code == 0
            assert "pipe1" in result.output
            assert "ENABLED" in result.output

def test_cli_status_stopped():
    runner = CliRunner()
    with patch("fustor_agent.cli._is_running", return_value=False):
        result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "STOPPED" in result.output

def test_cli_status_running():
    runner = CliRunner()
    with patch("fustor_agent.cli._is_running", return_value=1234):
        result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "RUNNING (PID: 1234)" in result.output
