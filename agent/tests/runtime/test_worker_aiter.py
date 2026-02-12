"""
Tests for aiter_sync_phase_wrapper — sync-to-async iterator bridge.
"""
import pytest
import asyncio
from fustor_agent.runtime.pipe.worker import aiter_sync_phase_wrapper


@pytest.mark.asyncio
async def test_normal_iteration():
    """Normal sync iterator is consumed fully via async."""
    items = [1, 2, 3, 4, 5]
    result = []
    async for item in aiter_sync_phase_wrapper(iter(items), "test-normal"):
        result.append(item)
    assert result == items


@pytest.mark.asyncio
async def test_empty_iterator():
    """Empty iterator yields nothing."""
    result = []
    async for item in aiter_sync_phase_wrapper(iter([]), "test-empty"):
        result.append(item)
    assert result == []


@pytest.mark.asyncio
async def test_exception_propagation():
    """Exception from sync iterator propagates to async consumer."""
    def failing_iter():
        yield 1
        yield 2
        raise ValueError("sync error")

    result = []
    with pytest.raises(ValueError, match="sync error"):
        async for item in aiter_sync_phase_wrapper(failing_iter(), "test-fail"):
            result.append(item)
    # Should have received items before the error
    assert result == [1, 2]


@pytest.mark.asyncio
async def test_consumer_cancellation():
    """Consumer cancellation stops the producer thread."""
    def infinite_iter():
        i = 0
        while True:
            yield i
            i += 1

    result = []
    async for item in aiter_sync_phase_wrapper(infinite_iter(), "test-cancel"):
        result.append(item)
        if len(result) >= 5:
            break
    assert len(result) == 5
    assert result == [0, 1, 2, 3, 4]
