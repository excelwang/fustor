"""
Tests for SessionManager agent job lifecycle.
"""
import pytest
import time
from fustor_fusion.core.session_manager import SessionManager


@pytest.fixture
def sm():
    return SessionManager(default_session_timeout=30)


@pytest.mark.asyncio
async def test_create_agent_job(sm):
    """Creating an agent job returns a job_id and tracks it."""
    job_id = await sm.create_agent_job("v1", "/data", ["s1", "s2"])
    assert job_id is not None
    jobs = sm.get_agent_jobs()
    assert len(jobs) == 1
    assert jobs[0]["status"] == "RUNNING"
    assert jobs[0]["progress"]["total_pipes"] == 2
    assert jobs[0]["progress"]["percentage"] == 0.0


@pytest.mark.asyncio
async def test_complete_agent_job_partial(sm):
    """Completing one of two sessions sets partial progress."""
    await sm.create_session_entry("v1", "s1")
    await sm.create_session_entry("v1", "s2")
    job_id = await sm.create_agent_job("v1", "/data", ["s1", "s2"])

    result = await sm.complete_agent_job("v1", "s1", "/data", job_id)
    assert result is True
    jobs = sm.get_agent_jobs()
    assert jobs[0]["progress"]["completed_pipes"] == 1
    assert jobs[0]["status"] == "RUNNING"


@pytest.mark.asyncio
async def test_complete_agent_job_full(sm):
    """Completing all sessions marks the job COMPLETED."""
    await sm.create_session_entry("v1", "s1")
    await sm.create_session_entry("v1", "s2")
    job_id = await sm.create_agent_job("v1", "/data", ["s1", "s2"])

    await sm.complete_agent_job("v1", "s1", "/data", job_id)
    await sm.complete_agent_job("v1", "s2", "/data", job_id)

    jobs = sm.get_agent_jobs()
    assert jobs[0]["status"] == "COMPLETED"
    assert jobs[0]["progress"]["percentage"] == 100.0
    assert jobs[0]["completed_at"] is not None


@pytest.mark.asyncio
async def test_complete_nonexistent_job(sm):
    """Completing a nonexistent job returns False."""
    await sm.create_session_entry("v1", "s1")
    result = await sm.complete_agent_job("v1", "s1", "/unknown", "bad-id")
    assert result is False


@pytest.mark.asyncio
async def test_has_pending_job(sm):
    """has_pending_job detects pending scan for a path."""
    await sm.create_session_entry("v1", "s1")
    job_id = await sm.create_agent_job("v1", "/scan-target", ["s1"])

    # Queue a scan command
    await sm.queue_command("v1", "s1", {"type": "scan", "path": "/scan-target", "job_id": job_id})
    assert await sm.has_pending_job("v1", "/scan-target") is True

    # After completing, pending should be cleared
    await sm.complete_agent_job("v1", "s1", "/scan-target", job_id)
    assert await sm.has_pending_job("v1", "/scan-target") is False
