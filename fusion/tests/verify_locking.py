import asyncio
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_locking")

async def test_atomic_locking():
    # Import here to ensure code is accessible
    try:
        from fustor_fusion.view_state_manager import ViewStateManager
    except ImportError:
        # Fix path
        sys.path.append("/home/huajin/fustor-1/fusion/src")
        from fustor_fusion.view_state_manager import ViewStateManager

    vsm = ViewStateManager()
    view_id = "view-race-test"
    session_a = "session-A"
    session_b = "session-B"

    print("Step 1: A acquires lock")
    success = await vsm.acquire_lock_if_free_or_owned(view_id, session_a)
    assert success is True, "Session A failed to acquire free lock"
    assert await vsm.is_locked_by_session(view_id, session_a), "Lock owner mismatch for A"

    print("Step 2: B tries to acquire lock -> Should Fail")
    success = await vsm.acquire_lock_if_free_or_owned(view_id, session_b)
    assert success is False, "Session B acquired lock owned by A!"
    assert await vsm.is_locked_by_session(view_id, session_a), "Lock owner changed unexpectedly"

    print("Step 3: A re-acquires lock (renew) -> Should Succeed")
    success = await vsm.acquire_lock_if_free_or_owned(view_id, session_a)
    assert success is True, "Session A failed to renew its own lock"
    assert await vsm.is_locked_by_session(view_id, session_a)

    print("Step 4: A unlocks")
    await vsm.unlock_for_session(view_id, session_a)
    assert not await vsm.is_locked(view_id), "View still locked after unlock"

    print("Step 5: B acquires lock -> Should Succeed")
    success = await vsm.acquire_lock_if_free_or_owned(view_id, session_b)
    assert success is True, "Session B failed to acquire free lock"
    assert await vsm.is_locked_by_session(view_id, session_b)
    
    print("SUCCESS: Atomic locking logic verified.")

if __name__ == "__main__":
    asyncio.run(test_atomic_locking())
