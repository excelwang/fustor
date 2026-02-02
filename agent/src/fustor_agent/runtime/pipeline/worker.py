import asyncio
import threading
import logging
from typing import Iterator, Any, AsyncIterator

logger = logging.getLogger("fustor_agent.pipeline.worker")

async def aiter_sync_wrapper(
    sync_iter: Iterator[Any], 
    id_for_thread: str,
    queue_size: int = 1000
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
            for item in sync_iter:
                if stop_event.is_set():
                    break
                # Blocking put via run_coroutine_threadsafe to respect backpressure
                future = asyncio.run_coroutine_threadsafe(queue.put(item), loop)
                try:
                    future.result() # Wait for the queue to have space
                except (asyncio.CancelledError, RuntimeError):
                    break
        except Exception as e:
            if not stop_event.is_set():
                asyncio.run_coroutine_threadsafe(queue.put(e), loop)
        finally:
            asyncio.run_coroutine_threadsafe(queue.put(StopAsyncIteration), loop)

    # Start producer thread
    thread = threading.Thread(
        target=_producer, 
        name=f"PipelineSource-Producer-{id_for_thread}", 
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
        # Fix for P2-7: Ensure thread resource is given a chance to release
        # We join with a small timeout to avoid blocking the event loop too long
        # if the thread is truly stuck (though daemon=True helps with exit).
        thread.join(timeout=0.5)
        if thread.is_alive():
            logger.warning(f"Producer thread {thread.name} did not terminate within timeout")
