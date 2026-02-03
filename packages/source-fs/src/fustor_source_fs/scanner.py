import os
import queue
import threading
import fnmatch
import logging
from typing import Dict, List, Optional, Callable, Any, Iterator, Tuple

logger = logging.getLogger("fustor_agent.driver.fs.scanner")

class RecursiveScanner:
    """
    Unified Parallel Recursive Scanner for FileSystem Driver.
    Supports Pre-scan, Snapshot, and Audit phases.
    """
    def __init__(self, root: str, num_workers: int = 4, file_pattern: str = "*"):
        self.root = root
        self.num_workers = num_workers
        self.file_pattern = file_pattern
        self._work_queue = queue.Queue()
        self._results_queue = queue.Queue(maxsize=1000)
        self._pending_dirs = 0
        self._map_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._error_count = 0

    def scan_parallel(self, 
                      worker_func: Callable[[Any, 'queue.Queue', 'queue.Queue', 'threading.Event'], None],
                      initial_item: Any = None) -> Iterator[Any]:
        """
        Generic parallel scan executor.
        """
        self._stop_event.clear()
        self._error_count = 0
        self._pending_dirs = 1
        
        # New queue for each call to avoid carryover
        self._work_queue = queue.Queue()
        self._results_queue = queue.Queue(maxsize=2000)
        
        start_item = initial_item if initial_item is not None else self.root
        self._work_queue.put(start_item)

        workers = []
        for _ in range(self.num_workers):
            t = threading.Thread(
                target=self._wrap_worker, 
                args=(worker_func,),
                daemon=True
            )
            t.start()
            workers.append(t)

        try:
            while True:
                try:
                    result = self._results_queue.get(timeout=0.1)
                    if result is None: 
                        continue
                    yield result
                except queue.Empty:
                    with self._map_lock:
                        if self._pending_dirs == 0 and self._work_queue.empty():
                            break
                    if self._stop_event.is_set():
                        break
        finally:
            self._stop_event.set()
            for t in workers:
                t.join(timeout=2.0)

    def _wrap_worker(self, worker_func):
        while not self._stop_event.is_set():
            try:
                item = self._work_queue.get(timeout=0.2)
            except queue.Empty:
                continue

            try:
                worker_func(item, self._work_queue, self._results_queue, self._stop_event)
            except Exception as e:
                self._error_count += 1
                logger.exception(f"Scanner worker encountered an unhandled error on item '{item}': {e}. Skipping item.")
            finally:
                with self._map_lock:
                    self._pending_dirs -= 1
                self._work_queue.task_done()

    def increment_pending(self, count: int = 1):
        with self._map_lock:
            self._pending_dirs += count

def get_file_metadata_minimal(path: str, stat_info: os.stat_result) -> Dict[str, Any]:
    """Extracted from event_handler.py to avoid circular deps if needed, 
    but we can import it if it's safe."""
    return {
        'path': path,
        'size': stat_info.st_size,
        'modified_time': stat_info.st_mtime,
        'is_dir': stat_info.st_mode & 0o170000 == 0o040000 # S_ISDIR
    }
