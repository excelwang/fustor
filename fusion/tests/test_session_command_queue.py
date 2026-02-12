"""
Tests for SessionManager command queue and keep_session_alive.
"""
import pytest
import time
from fustor_fusion.core.session_manager import SessionManager


@pytest.fixture
def sm():
    return SessionManager(default_session_timeout=30)


@pytest.mark.asyncio
async def test_queue_and_retrieve_commands(sm):
    """Commands queued via queue_command are retrievable via keep_session_alive."""
    await sm.create_session_entry("v1", "s1")
    
    await sm.queue_command("v1", "s1", {"type": "scan", "path": "/a"})
    await sm.queue_command("v1", "s1", {"type": "scan", "path": "/b"})
    
    success, commands = await sm.keep_session_alive("v1", "s1")
    assert success is True
    assert len(commands) == 2
    assert commands[0]["path"] == "/a"
    assert commands[1]["path"] == "/b"


@pytest.mark.asyncio
async def test_commands_cleared_after_retrieval(sm):
    """Once retrieved, commands are no longer pending."""
    await sm.create_session_entry("v1", "s1")
    await sm.queue_command("v1", "s1", {"type": "scan", "path": "/a"})
    
    await sm.keep_session_alive("v1", "s1")
    
    # Second call should get no commands
    success, commands = await sm.keep_session_alive("v1", "s1")
    assert success is True
    assert commands == []


@pytest.mark.asyncio
async def test_keep_alive_nonexistent_session(sm):
    """keep_session_alive for unknown session returns (False, [])."""
    success, commands = await sm.keep_session_alive("v1", "unknown")
    assert success is False
    assert commands == []


@pytest.mark.asyncio
async def test_queue_command_nonexistent_session(sm):
    """Queueing on a nonexistent session returns False."""
    result = await sm.queue_command("v1", "unknown", {"type": "scan", "path": "/a"})
    assert result is False


@pytest.mark.asyncio
async def test_keep_alive_updates_activity(sm):
    """keep_session_alive updates last_activity timestamp."""
    await sm.create_session_entry("v1", "s1")
    si = await sm.get_session_info("v1", "s1")
    old_activity = si.last_activity
    
    # Tiny sleep to ensure monotonic clock advances
    import asyncio
    await asyncio.sleep(0.01)
    
    await sm.keep_session_alive("v1", "s1")
    si = await sm.get_session_info("v1", "s1")
    assert si.last_activity > old_activity
