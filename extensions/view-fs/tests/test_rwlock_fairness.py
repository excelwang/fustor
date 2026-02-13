import asyncio
import pytest
from fustor_view_fs.rwlock import AsyncRWLock

@pytest.mark.asyncio
async def test_writer_starvation_prevention():
    """
    Test that a waiting writer prevents new readers from entering,
    ensuring the writer eventually gets the lock even if there's a
    continuous stream of reader requests.
    """
    lock = AsyncRWLock()
    results = []

    # 1. First reader holds the lock for a while
    async def slow_reader():
        async with lock.read_lock():
            results.append("reader1_start")
            await asyncio.sleep(0.2)
            results.append("reader1_end")

    # 2. Writer tries to acquire while reader1 is holding
    async def writer():
        await asyncio.sleep(0.05)  # Ensure reader1 is in
        results.append("writer_trying")
        async with lock.write_lock():
            results.append("writer_start")
            await asyncio.sleep(0.1)
            results.append("writer_end")

    # 3. Second reader tries to acquire AFTER writer has started waiting
    async def late_reader():
        await asyncio.sleep(0.1)  # Ensure writer is waiting
        results.append("reader2_trying")
        async with lock.read_lock():
            results.append("reader2_start")
            results.append("reader2_end")

    # Run all tasks
    await asyncio.gather(
        slow_reader(),
        writer(),
        late_reader()
    )

    # Expected order:
    # reader1_start
    # writer_trying
    # reader2_trying
    # reader1_end
    # writer_start
    # writer_end
    # reader2_start
    # reader2_end
    
    # Crucially, writer_start must come BEFORE reader2_start
    writer_start_idx = results.index("writer_start")
    reader2_start_idx = results.index("reader2_start")
    
    assert writer_start_idx < reader2_start_idx, f"Writer was starved by reader2! Results: {results}"
    print(f"Fairness test passed. Order: {results}")

@pytest.mark.asyncio
async def test_multiple_writers_fairness():
    """Test that writers are exclusive and all finish."""
    lock = AsyncRWLock()
    results = []
    active_writers = 0

    async def writer(name, delay):
        nonlocal active_writers
        await asyncio.sleep(delay)
        async with lock.write_lock():
            active_writers += 1
            results.append(f"writer_{name}_start")
            # This is the CRITICAL check for mutual exclusion
            current_active = active_writers
            await asyncio.sleep(0.05)
            assert current_active == 1, f"Mutual exclusion violated! active_writers={current_active}"
            results.append(f"writer_{name}_end")
            active_writers -= 1

    await asyncio.gather(
        writer("A", 0),
        writer("B", 0.01),
        writer("C", 0.02)
    )

    # We expect 6 events
    assert len(results) == 6
    # Each start must be eventually followed by its end
    starts = [r for r in results if r.endswith("_start")]
    ends = [r for r in results if r.endswith("_end")]
    assert len(starts) == 3
    assert len(ends) == 3
