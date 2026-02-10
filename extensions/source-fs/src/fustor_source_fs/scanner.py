import os
import queue
import time
import fnmatch
import logging
import threading
from typing import Dict, List, Optional, Callable, Any, Iterator, Tuple, Union

from fustor_core.event import EventBase, UpdateEvent, MessageSource
from .event_handler import get_file_metadata

logger = logging.getLogger("fustor_agent.driver.fs.scanner")

class RecursiveScanner:
    """
    Unified Parallel Recursive Scanner for FileSystem Driver.
    Supports parallel execution of filesystem traversal tasks.
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
            
    @property
    def error_count(self) -> int:
        return self._error_count


class FSScanner:
    """
    High-level scanner component for FSDriver.
    Encapsulates specific scanning logic for Pre-scan, Snapshot, and Audit.
    """
    def __init__(self, root: str, num_workers: int = 4, file_pattern: str = "*", drift_from_nfs: float = 0.0):
        self.root = root
        self.engine = RecursiveScanner(root, num_workers, file_pattern)
        self.drift_from_nfs = drift_from_nfs
        
    def perform_pre_scan(self) -> Tuple[Dict[str, float], Dict[str, List[str]], int, int]:
        """
        Executes the pre-scan logic.
        Returns: (dir_mtime_map, dir_children_map, total_entries, error_count)
        """
        dir_mtime_map: Dict[str, float] = {}
        dir_children_map: Dict[str, List[str]] = {}
        total_entries = 0
        
        # Define the worker logic
        def pre_scan_worker(root, work_q, res_q, stop_ev):
            try:
                latest_mtime = os.path.getmtime(root)
                local_subdirs = []
                local_total = 1
                
                with os.scandir(root) as it:
                    for entry in it:
                        local_total += 1
                        if stop_ev.is_set(): break
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                local_subdirs.append(entry.path)
                                self.engine.increment_pending()
                                work_q.put(entry.path)
                            else:
                                st = entry.stat(follow_symlinks=False)
                                latest_mtime = max(latest_mtime, st.st_mtime)
                        except Exception as e:
                            logger.warning(f"[fs] Error scanning entry '{entry.path}': {e}. Skipping entry.")

                # Yield results instead of updating shared state directly
                res_q.put({
                    'root': root,
                    'mtime': latest_mtime,
                    'subdirs': local_subdirs,
                    'count': local_total
                })
                
            except Exception as e:
                logger.error(f"[fs] Failed to scan directory '{root}': {e}")
                # We don't yield success here, relying on engine error count or we could yield error info

        # Execute parallel scan and aggregate
        for result in self.engine.scan_parallel(pre_scan_worker):
            root = result['root']
            dir_mtime_map[root] = result['mtime']
            dir_children_map[root] = result['subdirs']
            total_entries += result['count']
            
        return dir_mtime_map, dir_children_map, total_entries, self.engine.error_count

    def scan_snapshot(self, 
                      event_schema: str, 
                      batch_size: int = 100, 
                      callback: Callable[[str, float], None] = None) -> Iterator[EventBase]:
        """
        Executes snapshot scan.
        callback: Optional function to call for touching watched paths (path, mtime).
        """
        if not os.path.exists(self.root) or not os.access(self.root, os.R_OK):
            raise OSError(f"Snapshot failed: Root path '{self.root}' is missing or inaccessible")

        snapshot_time = int((time.time() + self.drift_from_nfs) * 1000)
        
        def snapshot_worker(root, work_q, res_q, stop_ev):
            try:
                # FIX: Check existence first to catch specific root errors
                if not os.path.exists(root):
                    raise OSError(f"Path not found: {root}")
                    
                dir_stat = os.stat(root)
                latest_mtime_in_subtree = dir_stat.st_mtime
                dir_metadata = get_file_metadata(root, root_path=self.root, stat_info=dir_stat)
                
                # Emit directory event immediately
                res_q.put(UpdateEvent(
                    event_schema=event_schema,
                    table="files",
                    rows=[dir_metadata],
                    fields=list(dir_metadata.keys()),
                    message_source=MessageSource.SNAPSHOT,
                    index=snapshot_time
                ))

                local_batch = []
                with os.scandir(root) as it:
                    for entry in it:
                        if stop_ev.is_set(): break
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                self.engine.increment_pending()
                                work_q.put(entry.path)
                            elif fnmatch.fnmatch(entry.name, self.engine.file_pattern):
                                st = entry.stat()
                                meta = get_file_metadata(entry.path, root_path=self.root, stat_info=st)
                                latest_mtime_in_subtree = max(latest_mtime_in_subtree, meta['modified_time'])
                                local_batch.append(meta)
                                if len(local_batch) >= batch_size:
                                    res_q.put(UpdateEvent(
                                        event_schema=event_schema,
                                        table="files",
                                        rows=local_batch,
                                        fields=list(local_batch[0].keys()),
                                        message_source=MessageSource.SNAPSHOT,
                                        index=snapshot_time
                                    ))
                                    local_batch = []
                        except Exception as e:
                            logger.warning(f"[fs] Error statting entry '{entry.path}' during snapshot: {e}. Skipping.")
                
                if local_batch:
                    res_q.put(UpdateEvent(
                        event_schema=event_schema,
                        table="files",
                        rows=local_batch,
                        fields=list(local_batch[0].keys()),
                        message_source=MessageSource.SNAPSHOT,
                        index=snapshot_time
                    ))
                

                if callback:
                    callback(root, latest_mtime_in_subtree - self.drift_from_nfs)

            except Exception as e:
                logger.error(f"[fs] Failed to process directory '{root}' during snapshot: {e}")
                # Re-raise to ensure error_count is incremented by RecursiveScanner
                raise

        # Global re-batching buffer loop
        row_buffer = []
        for item in self.engine.scan_parallel(snapshot_worker):
            if isinstance(item, UpdateEvent):
                # We might want to re-batch here if we want strictly batch_size chunks,
                # but UpdateEvents are already batched by directory.
                # The original code had a global re-batching buffer.
                row_buffer.extend(item.rows)
                while len(row_buffer) >= batch_size:
                    yield UpdateEvent(
                        event_schema=event_schema,
                        table="files",
                        rows=row_buffer[:batch_size],
                        fields=list(row_buffer[0].keys()),
                        message_source=MessageSource.SNAPSHOT,
                        index=snapshot_time
                    )
                    row_buffer = row_buffer[batch_size:]
            
        if row_buffer:
            yield UpdateEvent(
                event_schema=event_schema,
                table="files",
                rows=row_buffer,
                fields=list(row_buffer[0].keys()),
                message_source=MessageSource.SNAPSHOT,
                index=snapshot_time
            )

    def scan_audit(self, 
                   event_schema: str, 
                   mtime_cache: Dict[str, float], 
                   batch_size: int = 100) -> Iterator[Tuple[Optional[UpdateEvent], Dict[str, float]]]:
        """
        Executes audit scan.
        """
        audit_time = int((time.time() + self.drift_from_nfs) * 1000)
        
        def audit_worker(item, work_q, res_q, stop_ev):
            root, _parent_path = item
            try:
                dir_stat = os.stat(root)
                current_dir_mtime = dir_stat.st_mtime
                cached_mtime = mtime_cache.get(root)
                is_silent = (cached_mtime is not None and cached_mtime == current_dir_mtime)
                
                # Report directory
                dir_metadata = get_file_metadata(root, root_path=self.root, stat_info=dir_stat)
                if dir_metadata:
                    if is_silent:
                        dir_metadata['audit_skipped'] = True
                    
                    res_q.put((UpdateEvent(
                        event_schema=event_schema,
                        table="files",
                        rows=[dir_metadata],
                        fields=list(dir_metadata.keys()),
                        message_source=MessageSource.AUDIT,
                        index=audit_time
                    ), {}))

                local_batch = []
                local_subdirs = []
                with os.scandir(root) as it:
                    for entry in it:
                        if stop_ev.is_set(): break
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                local_subdirs.append(entry.path)
                            elif not is_silent and fnmatch.fnmatch(entry.name, self.engine.file_pattern):
                                st = entry.stat()
                                meta = get_file_metadata(entry.path, root_path=self.root, stat_info=st)
                                if meta:
                                    meta['parent_path'] = root
                                    meta['parent_mtime'] = current_dir_mtime
                                    local_batch.append(meta)
                                    if len(local_batch) >= batch_size:
                                        res_q.put((UpdateEvent(
                                            event_schema=event_schema,
                                            table="files",
                                            rows=local_batch,
                                            fields=list(local_batch[0].keys()),
                                            message_source=MessageSource.AUDIT,
                                            index=audit_time
                                        ), {root: current_dir_mtime}))
                                        local_batch = []
                        except OSError:
                            pass

                if local_batch:
                    res_q.put((UpdateEvent(
                        event_schema=event_schema,
                        table="files",
                        rows=local_batch,
                        fields=list(local_batch[0].keys()),
                        message_source=MessageSource.AUDIT,
                        index=audit_time
                    ), {root: current_dir_mtime}))
                elif not is_silent:
                    res_q.put((None, {root: current_dir_mtime}))

                for sd in local_subdirs:
                    self.engine.increment_pending()
                    work_q.put((sd, root))

            except Exception as e:
                logger.error(f"[fs] Failed to process directory '{root}' during audit: {e}")

        for result in self.engine.scan_parallel(audit_worker, initial_item=(self.root, None)):
            yield result
