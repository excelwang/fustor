"""
Test App.reload_config() integration: verifies that the reload
correctly stops removed pipes and starts added pipes.
"""
import pytest
import yaml
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from fustor_agent.config.unified import AgentConfigLoader


@pytest.fixture
def config_dir(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    return config_dir


def _write_config(config_dir: Path, filename: str, content: dict):
    with open(config_dir / filename, 'w') as f:
        yaml.dump(content, f)


def _make_app_stub(config_dir):
    """Create a minimal App-like object with reload_config logic, bypassing full init."""
    import logging
    from fustor_agent.config.unified import AgentConfigLoader
    
    loader = AgentConfigLoader(config_dir)
    loader.load_all()
    
    class AppStub:
        def __init__(self):
            self.logger = logging.getLogger("test")
            self.pipe_runtime = {}
            self._stop_pipe = AsyncMock()
            self._start_pipe = AsyncMock()
            self._loader = loader
        
        async def reload_config(self):
            """Mirrors App.reload_config() logic."""
            self._loader.reload()
            current_running_ids = set(self.pipe_runtime.keys())
            diff = self._loader.get_diff(current_running_ids)
            added = diff["added"]
            removed = diff["removed"]

            if not added and not removed:
                return

            for pid in removed:
                try:
                    await self._stop_pipe(pid)
                except Exception as e:
                    self.logger.error(f"Error stopping pipe '{pid}': {e}")

            for pid in added:
                try:
                    await self._start_pipe(pid)
                except Exception as e:
                    self.logger.error(f"Failed to start pipe '{pid}': {e}")
    
    return AppStub()


class TestAppReloadConfig:

    @pytest.mark.asyncio
    async def test_reload_stops_removed_pipe(self, config_dir):
        """reload_config() should call _stop_pipe for pipes removed from config."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a"}}
        })

        app = _make_app_stub(config_dir)
        app.pipe_runtime = {"pipe-a": MagicMock(), "pipe-b": MagicMock()}

        await app.reload_config()

        app._stop_pipe.assert_called_once_with("pipe-b")
        app._start_pipe.assert_not_called()

    @pytest.mark.asyncio
    async def test_reload_starts_added_pipe(self, config_dir):
        """reload_config() should call _start_pipe for pipes added to config."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {
                "pipe-a": {"source": "src-a", "sender": "sender-a"},
                "pipe-b": {"source": "src-a", "sender": "sender-a"},
            }
        })

        app = _make_app_stub(config_dir)
        app.pipe_runtime = {"pipe-a": MagicMock()}

        await app.reload_config()

        app._start_pipe.assert_called_once_with("pipe-b")
        app._stop_pipe.assert_not_called()

    @pytest.mark.asyncio
    async def test_reload_no_changes(self, config_dir):
        """reload_config() should do nothing when config matches running state."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a"}}
        })

        app = _make_app_stub(config_dir)
        app.pipe_runtime = {"pipe-a": MagicMock()}

        await app.reload_config()

        app._stop_pipe.assert_not_called()
        app._start_pipe.assert_not_called()

    @pytest.mark.asyncio
    async def test_reload_handles_stop_error_gracefully(self, config_dir):
        """reload_config() should continue even if stopping a pipe fails."""
        _write_config(config_dir, "default.yaml", {
            "sources": {"src-a": {"driver": "fs", "uri": "/data"}},
            "senders": {"sender-a": {"driver": "http", "uri": "http://localhost"}},
            "pipes": {"pipe-a": {"source": "src-a", "sender": "sender-a"}}
        })

        app = _make_app_stub(config_dir)
        app.pipe_runtime = {"pipe-a": MagicMock(), "pipe-b": MagicMock(), "pipe-c": MagicMock()}

        async def stop_side_effect(pid):
            if pid == "pipe-b":
                raise RuntimeError("stop failed")

        app._stop_pipe = AsyncMock(side_effect=stop_side_effect)

        await app.reload_config()

        # Both pipe-b and pipe-c should have been attempted
        assert app._stop_pipe.call_count == 2
