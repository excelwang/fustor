"""
Tests for FusionPipe.process_events — snapshot_end and audit_end signal handling.
"""
import pytest
import pytest_asyncio
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any, Dict, List, Optional

from fustor_core.pipe import PipeState
from fustor_core.pipe.handler import ViewHandler
from fustor_fusion.runtime import FusionPipe


class StubViewHandler(ViewHandler):
    """Minimal handler for process_events signal testing."""
    schema_name = "stub"

    def __init__(self, handler_id="stub-view"):
        super().__init__(handler_id, {})
        self.events = []
        self.audit_end_called = False

    async def initialize(self):
        pass

    async def close(self):
        pass

    async def process_event(self, event):
        self.events.append(event)

    async def on_session_start(self):
        pass

    async def on_session_close(self):
        pass

    async def handle_audit_end(self):
        self.audit_end_called = True

    def get_data_view(self, **kwargs):
        return {}

    def get_stats(self):
        return {}


def _make_event_dict(path="/file.txt", event_type="INSERT"):
    return {
        "event_type": event_type,
        "rows": [{"path": path, "size": 10, "modified_time": 100.0}],
        "message_source": "SNAPSHOT",
        "index": 100.0,
    }


@pytest.fixture
def handler():
    return StubViewHandler()


@pytest_asyncio.fixture
async def pipe(handler):
    p = FusionPipe(
        pipe_id="test-pipe",
        config={"view_ids": ["test-view"], "allow_concurrent_push": True},
        view_handlers=[handler]
    )
    await p.start()
    yield p
    await p.stop()


@pytest.mark.asyncio
async def test_snapshot_end_by_leader(pipe, handler):
    """snapshot_end signal from leader marks view snapshot complete."""
    mock_vsm = MagicMock()
    mock_vsm.is_leader = AsyncMock(return_value=True)
    mock_vsm.set_snapshot_complete = AsyncMock()

    with patch("fustor_fusion.view_state_manager.view_state_manager", mock_vsm):
        result = await pipe.process_events(
            [_make_event_dict()],
            session_id="leader-sess",
            source_type="snapshot",
            is_end=True
        )
        assert result["success"] is True
        mock_vsm.set_snapshot_complete.assert_called_with("test-view", "leader-sess")


@pytest.mark.asyncio
async def test_snapshot_end_by_follower_ignored(pipe, handler):
    """snapshot_end signal from non-leader is ignored."""
    mock_vsm = MagicMock()
    mock_vsm.is_leader = AsyncMock(return_value=False)
    mock_vsm.set_snapshot_complete = AsyncMock()

    with patch("fustor_fusion.view_state_manager.view_state_manager", mock_vsm):
        result = await pipe.process_events(
            [_make_event_dict()],
            session_id="follower-sess",
            source_type="snapshot",
            is_end=True
        )
        assert result["success"] is True
        mock_vsm.set_snapshot_complete.assert_not_called()


@pytest.mark.asyncio
async def test_audit_end_triggers_handler(pipe, handler):
    """audit_end signal calls handle_audit_end on handler."""
    result = await pipe.process_events(
        [_make_event_dict()],
        session_id="sess-1",
        source_type="audit",
        is_end=True
    )
    assert result["success"] is True
    # Wait briefly for drain
    await asyncio.sleep(0.1)
    assert handler.audit_end_called is True


@pytest.mark.asyncio
async def test_process_events_not_running():
    """process_events returns error when pipe not running."""
    h = StubViewHandler()
    p = FusionPipe(
        pipe_id="test-pipe",
        config={"view_ids": ["test-view"]},
        view_handlers=[h]
    )
    result = await p.process_events(
        [_make_event_dict()],
        session_id="s1",
        source_type="snapshot"
    )
    assert result["success"] is False


@pytest.mark.asyncio
async def test_malformed_event_skipped(pipe, handler):
    """Malformed events are skipped with a count."""
    result = await pipe.process_events(
        [{"bad_field": "no event_type"}],
        session_id="s1",
        source_type="snapshot"
    )
    assert result["success"] is True
    assert result["skipped"] == 1
