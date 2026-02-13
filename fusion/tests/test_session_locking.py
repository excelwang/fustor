import pytest
import asyncio
from fustor_fusion.view_state_manager import ViewStateManager

@pytest.mark.asyncio
async def test_atomic_locking():
    vsm = ViewStateManager()
    view_id = "view-race-test"
    session_a = "session-A"
    session_b = "session-B"

    # 1. A acquires lock
    success = await vsm.acquire_lock_if_free_or_owned(view_id, session_a)
    assert success is True
    assert await vsm.is_locked_by_session(view_id, session_a)

    # 2. B tries to acquire lock -> Should Fail
    success = await vsm.acquire_lock_if_free_or_owned(view_id, session_b)
    assert success is False
    assert await vsm.is_locked_by_session(view_id, session_a)

    # 3. A re-acquires lock (renew) -> Should Succeed
    success = await vsm.acquire_lock_if_free_or_owned(view_id, session_a)
    assert success is True
    assert await vsm.is_locked_by_session(view_id, session_a)

    # 4. A unlocks
    await vsm.unlock_for_session(view_id, session_a)
    assert not await vsm.is_locked(view_id)

    # 5. B acquires lock -> Should Succeed
    success = await vsm.acquire_lock_if_free_or_owned(view_id, session_b)
    assert success is True
    assert await vsm.is_locked_by_session(view_id, session_b)
