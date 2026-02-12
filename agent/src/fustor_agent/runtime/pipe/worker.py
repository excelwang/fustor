import asyncio
import threading
import logging
from typing import Iterator, Any, AsyncIterator, Optional

logger = logging.getLogger("fustor_agent.pipe.worker")

async def aiter_sync_phase_wrapper(
    phase_iter: Iterator[Any], 
    id_for_thread: str,
    queue_size: int = 1000,
    yield_timeout: Optional[float] = None
) -> AsyncIterator[Any]:
    """
    Safely and efficiently wrap a synchronous iterator into an async generator.
    
    This implementation runs the synchronous iterator in a dedicated background
    thread and communicates items back via an asyncio.Queue, avoiding the overhead
    of creating a new thread/Future for every single item.
    """
    queue = asyncio.Queue(maxsize=queue_size)
    loop = asyncio.get_event_loop()
    stop_event = threading.Event()
    
    def _producer():
        try:
            for item in phase_iter:
                if stop_event.is_set():
                    break
                # Blocking put via run_coroutine_threadsafe to respect backpressure
                coro = queue.put(item)
                try:
                    future = asyncio.run_coroutine_threadsafe(coro, loop)
                    future.result() # Wait for the queue to have space
                except (asyncio.CancelledError, RuntimeError):
                    coro.close()
                    break
        except Exception as e:
            if not stop_event.is_set():
                coro = queue.put(e)
                try:
                    asyncio.run_coroutine_threadsafe(coro, loop)
                except RuntimeError:
                    coro.close()
        finally:
            # Only send sentinel if consumer is still running.
            # If stop_event is set, consumer already exited — sending would
            # create an unawaited coroutine that gets GC'd with a warning.
            if not stop_event.is_set():
                coro = queue.put(StopAsyncIteration)
                try:
                    asyncio.run_coroutine_threadsafe(coro, loop)
                except RuntimeError:
                    coro.close()

    # Start producer thread
    thread = threading.Thread(
        target=_producer, 
        name=f"PipeSource-Producer-{id_for_thread}", 
        daemon=True
    )
    thread.start()

    try:
        while True:
            item = await queue.get()
            if item is StopAsyncIteration:
                break
            if isinstance(item, Exception):
                raise item
            yield item
            queue.task_done()
    finally:
        stop_event.set()
        # Drain remaining items to unblock the producer and clean up pending coroutines.
        # Without this, any in-flight queue.put() coroutine would be GC'd unawaited.
        while not queue.empty():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        thread.join(timeout=0.5)
        if thread.is_alive():
            logger.warning(f"Producer thread {thread.name} did not terminate within timeout")
