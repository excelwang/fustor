"""
Fuagent source driver for the file system.

This driver implements a 'Smart Dynamic Monitoring' strategy to efficiently
monitor large directory structures without exhausting system resources.
"""
import os
import queue
import time
import datetime
import logging
import uuid
import getpass
import fnmatch
import threading
import multiprocessing
from typing import Any, Dict, Iterator, List, Tuple, Optional, Set
from concurrent.futures import ThreadPoolExecutor
from fustor_core.drivers import SourceDriver
from fustor_core.models.config import SourceConfig
from fustor_core.event import EventBase, UpdateEvent, DeleteEvent, MessageSource

from .components import _WatchManager, safe_path_handling
from .event_handler import OptimizedWatchEventHandler, get_file_metadata

logger = logging.getLogger("fustor_agent.driver.fs")
            
class FSDriver(SourceDriver):
    _instances: Dict[str, 'FSDriver'] = {}
    _lock = threading.Lock()
    
    # FS driver doesn't require discovery as fields are fixed metadata.
    require_schema_discovery = False

    @property
    def is_transient(self) -> bool:
        """
        FS driver is transient - events will be lost if not processed immediately.
        """
        return True
    
    def __new__(cls, id: str, config: SourceConfig):
        # Generate unique signature based on URI and credentials to ensure permission isolation
        signature = f"{config.uri}#{hash(str(config.credential))}"
        
        with FSDriver._lock:
            if signature not in FSDriver._instances:
                # Create new instance
                instance = super().__new__(cls)
                FSDriver._instances[signature] = instance
            return FSDriver._instances[signature]
    
    def __init__(self, id: str, config: SourceConfig):
        # Prevent re-initialization of shared instances
        if hasattr(self, '_initialized'):
            return
        
        super().__init__(id, config)
        self.uri = self.config.uri
        self.event_queue: queue.Queue[EventBase] = queue.Queue()
        self.drift_from_nfs = 0.0
        self._stop_driver_event = threading.Event()
        
        min_monitoring_window_days = self.config.driver_params.get("min_monitoring_window_days", 30.0)
        throttle_interval = self.config.driver_params.get("throttle_interval_sec", 5.0)
        self.watch_manager = _WatchManager(
            self.uri, 
            event_handler=None, 
            min_monitoring_window_days=min_monitoring_window_days, 
            stop_driver_event=self._stop_driver_event, 
            throttle_interval=throttle_interval
        )
        self.event_handler = OptimizedWatchEventHandler(
            self.event_queue, 
            self.watch_manager
        )
        self.watch_manager.event_handler = self.event_handler
        self._pre_scan_completed = False
        self._pre_scan_lock = threading.Lock()
        
        # Thread pool for parallel scanning
        # Issue 7 Fix: Use a more conservative default (min(4, cpu_count)) to prevent NFS IOPS spikes.
        default_workers = min(4, multiprocessing.cpu_count())
        max_workers = self.config.driver_params.get("max_scan_workers", default_workers)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        logger.info(f"[fs] Driver initialized with {max_workers} scan workers (default was {default_workers}).")
        
        self._initialized = True

    def _perform_pre_scan_and_schedule(self):
        """
        Performs a one-time scan of the directory to populate the watch manager
        with a capacity-aware, hierarchy-complete set of the most active directories.
        """
        with self._pre_scan_lock:
            if self._pre_scan_completed:
                return

            logger.info(f"[fs] Performing initial parallel directory scan for: {self.uri}")
            
            # 1. Parallel Scan (Top-down) to collect direct mtimes
            # dir_mtime_map[path] = max(mtime of dir itself, mtime of its direct files)
            dir_mtime_map: Dict[str, float] = {}
            # dir_children_map[path] = [direct_child_dir_paths]
            dir_children_map: Dict[str, List[str]] = {}
            map_lock = threading.Lock()
            
            error_count = 0
            total_entries = 0
            
            work_queue = queue.Queue()
            work_queue.put(self.uri)
            
            active_tasks = threading.Event() # Set when tasks are in queue or being processed
            pending_count = 1 # Number of directories currently in queue or processing

            def scan_worker():
                nonlocal pending_count, error_count, total_entries
                while True:
                    try:
                        root = work_queue.get(timeout=0.1)
                    except queue.Empty:
                        if pending_count == 0:
                            break
                        continue

                    try:
                        latest_mtime = os.path.getmtime(root)
                        local_subdirs = []
                        local_total = 1
                        
                        with os.scandir(root) as it:
                            for entry in it:
                                local_total += 1
                                try:
                                    if entry.is_dir(follow_symlinks=False):
                                        local_subdirs.append(entry.path)
                                    else:
                                        st = entry.stat(follow_symlinks=False)
                                        latest_mtime = max(latest_mtime, st.st_mtime)
                                except OSError:
                                    error_count += 1

                        with map_lock:
                            dir_mtime_map[root] = latest_mtime
                            dir_children_map[root] = local_subdirs
                            total_entries += local_total
                            
                            # Add subdirs to queue
                            for sd in local_subdirs:
                                pending_count += 1
                                work_queue.put(sd)
                        
                        # Progress logging
                        if total_entries > 0 and total_entries % 10000 < local_total:
                            logger.info(f"[fs] Pre-scan progress: processed {total_entries} entries, pending dirs: {pending_count}")

                    except OSError:
                        error_count += 1
                    finally:
                        with map_lock:
                            pending_count -= 1
                        work_queue.task_done()

            # Start workers
            workers = []
            for _ in range(self._executor._max_workers):
                t = threading.Thread(target=scan_worker, daemon=True)
                t.start()
                workers.append(t)
            
            # Wait for completion
            work_queue.join()
            for t in workers:
                t.join()

            # 2. Bottom-up pass to calculate recursive subtree mtimes
            logger.info(f"[fs] Parallel scan finished. Calculating recursive mtimes for {len(dir_mtime_map)} directories...")
            
            # Sort directories by depth (deepest first) to bubble up mtimes
            all_dirs = sorted(dir_mtime_map.keys(), key=lambda x: x.count(os.sep), reverse=True)
            
            for path in all_dirs:
                # current_mtime is max(self, direct_files)
                # We need to add max(recursive_mtimes_of_subdirs)
                latest = dir_mtime_map[path]
                for subdir in dir_children_map.get(path, []):
                    latest = max(latest, dir_mtime_map.get(subdir, 0))
                dir_mtime_map[path] = latest
            
            # Establish Shadow Reference Frame for Watch Scheduling
            drift_from_nfs = 0.0
            if dir_mtime_map:
                # 1. Identify a 'Stable Horizon': Use the 99th percentile of mtimes to ignore outliers
                mtimes = sorted(dir_mtime_map.values())
                # Use the 99.9th percentile or high index to capture 'now' while ignoring 0.1% outliers
                p99_idx = max(0, len(mtimes) - 1 - (len(mtimes) // 1000))
                latest_mtime_stable = mtimes[p99_idx]
                
                # 2. Calculate the base drift between NFS (logical) and Agent (physical)
                self.drift_from_nfs = latest_mtime_stable - time.time()
                drift_from_nfs = self.drift_from_nfs
                
                # 3. Calculate logging stats
                root_recursive_mtime = dir_mtime_map.get(self.uri, 0.0)
                newest_relative_age = latest_mtime_stable - root_recursive_mtime
                
                logger.info(
                    f"[fs] Pre-scan completed: processed {total_entries} entries, "
                    f"errors: {error_count}, newest_relative_age: {newest_relative_age/86400:.2f} days, drift: {drift_from_nfs:.2f}s"
                )

            logger.info(f"[fs] Found {len(dir_mtime_map)} total directories. Building capacity-aware, hierarchy-complete watch set...")
            sorted_dirs = sorted(dir_mtime_map.items(), key=lambda item: item[1], reverse=True)[:self.watch_manager.watch_limit]
            old_limit = self.watch_manager.watch_limit
            for path, server_mtime in sorted_dirs:
                # Normalize server_mtime into Agent's physical domain for stable TTL logic
                lru_timestamp = server_mtime - drift_from_nfs
                self.watch_manager.schedule(path, lru_timestamp)
                if self.watch_manager.watch_limit < old_limit:
                    break  # Stop if we hit the limit during scheduling
            logger.info(f"[fs] Final watch set constructed. Total paths to watch: {len(self.watch_manager.lru_cache)}.")
            self._pre_scan_completed = True


    def get_snapshot_iterator(self, **kwargs) -> Iterator[EventBase]:
        """
        Parallel Snapshot Scan Phase.
        """
        stream_id = f"snapshot-fs-{uuid.uuid4().hex[:6]}"
        logger.info(f"[{stream_id}] Starting Parallel Snapshot Scan: {self.uri}")

        driver_params = self.config.driver_params
        if driver_params.get("startup_mode") == "message-only":
            return
            
        file_pattern = driver_params.get("file_pattern", "*")
        batch_size = kwargs.get("batch_size", 100)
        snapshot_time = int(time.time() * 1000)

        results_queue = queue.Queue(maxsize=batch_size * 2)
        work_queue = queue.Queue()
        work_queue.put(self.uri)
        
        # Snapshot work baseline.

        pending_dirs = 1
        map_lock = threading.Lock()
        stop_workers = threading.Event()

        def scan_worker():
            nonlocal pending_dirs
            while not stop_workers.is_set():
                try:
                    root = work_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                try:
                    dir_stat = os.stat(root)
                    latest_mtime_in_subtree = dir_stat.st_mtime
                    dir_metadata = get_file_metadata(root, stat_info=dir_stat)
                    results_queue.put(UpdateEvent(
                        event_schema=self.uri,
                        table="files",
                        rows=[dir_metadata],
                        fields=list(dir_metadata.keys()),
                        message_source=MessageSource.SNAPSHOT,
                        index=snapshot_time
                    ))

                    local_batch = []
                    with os.scandir(root) as it:
                        for entry in it:
                            try:
                                if entry.is_dir(follow_symlinks=False):
                                    with map_lock:
                                        pending_dirs += 1
                                        work_queue.put(entry.path)
                                elif fnmatch.fnmatch(entry.name, file_pattern):
                                    st = entry.stat()
                                    meta = get_file_metadata(entry.path, stat_info=st)
                                    latest_mtime_in_subtree = max(latest_mtime_in_subtree, meta['modified_time'])
                                    local_batch.append(meta)
                                    if len(local_batch) >= batch_size:
                                        results_queue.put(UpdateEvent(
                                            event_schema=self.uri,
                                            table="files",
                                            rows=local_batch,
                                            fields=list(local_batch[0].keys()),
                                            message_source=MessageSource.SNAPSHOT,
                                            index=snapshot_time
                                        ))
                                        local_batch = []
                            except OSError:
                                pass
                    
                    if local_batch:
                        results_queue.put(UpdateEvent(
                            event_schema=self.uri,
                            table="files",
                            rows=local_batch,
                            fields=list(local_batch[0].keys()),
                            message_source=MessageSource.SNAPSHOT,
                            index=snapshot_time
                        ))
                    
                    self.watch_manager.touch(root, latest_mtime_in_subtree - self.drift_from_nfs, is_recursive_upward=False)

                except OSError:
                    pass
                finally:
                    with map_lock:
                        pending_dirs -= 1
                    work_queue.task_done()

        # Start workers
        workers = []
        for _ in range(self._executor._max_workers):
            t = threading.Thread(target=scan_worker, daemon=True)
            t.start()
            workers.append(t)

        # Consumer loop with global re-batching
        row_buffer = []
        while True:
            try:
                event = results_queue.get(timeout=0.2)
                row_buffer.extend(event.rows)
                while len(row_buffer) >= batch_size:
                    yield UpdateEvent(
                        event_schema=self.uri,
                        table="files",
                        rows=row_buffer[:batch_size],
                        fields=list(row_buffer[0].keys()),
                        message_source=MessageSource.SNAPSHOT,
                        index=snapshot_time
                    )
                    row_buffer = row_buffer[batch_size:]
            except queue.Empty:
                if pending_dirs == 0 and work_queue.empty():
                    break
        
        if row_buffer:
            yield UpdateEvent(
                event_schema=self.uri,
                table="files",
                rows=row_buffer,
                fields=list(row_buffer[0].keys()),
                message_source=MessageSource.SNAPSHOT,
                index=snapshot_time
            )
        
        stop_workers.set()
        for t in workers:
            t.join()
        logger.info(f"[{stream_id}] Snapshot scan completed.")

    def get_message_iterator(self, start_position: int=-1, **kwargs) -> Iterator[EventBase]:
        
        # Perform pre-scan to populate watches before starting the observer.
        # This is essential for the message-first architecture and must block
        # until completion to prevent race conditions downstream.
        
        self._perform_pre_scan_and_schedule()

        def _iterator_func() -> Iterator[EventBase]:
            # After pre-scan is complete, any new events should be considered "starting from now"
            # If start_position is provided, use it; otherwise, start from current time
            
            stream_id = f"message-fs-{uuid.uuid4().hex[:6]}"
            
            stop_event = kwargs.get("stop_event")
            self.watch_manager.start()
            logger.info(f"[{stream_id}] WatchManager started.")

            try:
                # Process events normally, but use the effective start position
                while not (stop_event and stop_event.is_set()):
                    try:
                        max_sync_delay_seconds = self.config.driver_params.get("max_sync_delay_seconds", 1.0)
                        event = self.event_queue.get(timeout=max_sync_delay_seconds)
                        
                        if start_position!=-1 and event.index < start_position:
                            logger.debug(f"[{stream_id}] Skipping old event: {event.event_type} index={event.index} < start_position={start_position}")
                            continue
                        
                        yield event

                    except queue.Empty:
                        continue
            finally:
                self.watch_manager.stop()
                logger.info(f"[{stream_id}] Stopped real-time monitoring for: {self.uri}")

        return _iterator_func()

    def get_audit_iterator(self, mtime_cache: Dict[str, float] = None, **kwargs) -> Iterator[Tuple[Optional[EventBase], Dict[str, float]]]:
        """
        Parallel Audit Sync: Uses a task queue to scan subtrees in parallel.
        Correctly implements mtime-based 'True Silence'.
        """
        stream_id = f"audit-fs-{uuid.uuid4().hex[:6]}"
        logger.info(f"[{stream_id}] Starting Parallel Audit Scan: {self.uri}")
        
        if mtime_cache is None:
            mtime_cache = {}
            
        batch_size = kwargs.get("batch_size", 100)
        file_pattern = self.config.driver_params.get("file_pattern", "*")
        audit_time = int(time.time() * 1000) # Use milliseconds for wire-format index
        
        results_queue = queue.Queue(maxsize=batch_size * 2)
        work_queue = queue.Queue()
        work_queue.put((self.uri, None))
        
        # Audit work baseline established by physical time.
        pending_dirs = 1
        map_lock = threading.Lock()
        stop_workers = threading.Event()

        def audit_worker():
            nonlocal pending_dirs
            while not stop_workers.is_set():
                try:
                    root, parent_path = work_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                try:
                    dir_stat = os.stat(root)
                    current_dir_mtime = dir_stat.st_mtime
                    cached_mtime = mtime_cache.get(root)
                    
                    is_silent = cached_mtime is not None and cached_mtime == current_dir_mtime
                    
                    if not is_silent:
                        dir_metadata = get_file_metadata(root, stat_info=dir_stat)
                        results_queue.put((UpdateEvent(
                            event_schema=self.uri,
                            table="files",
                            rows=[dir_metadata],
                            fields=list(dir_metadata.keys()),
                            message_source=MessageSource.AUDIT,
                            index=audit_time
                        ), {}))
                    else:
                        # Optimization & Protection: Directory is silent (mtime unchanged). 
                        # Report it as skipped to ensure children are protected from deletion in Fusion.
                        dir_metadata = get_file_metadata(root, stat_info=dir_stat)
                        if dir_metadata:
                            dir_metadata['audit_skipped'] = True
                            results_queue.put((UpdateEvent(
                                event_schema=self.uri,
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
                            try:
                                if entry.is_dir(follow_symlinks=False):
                                    # Always collect subdirs to ensure deep detection since dir mtime 
                                    # only reflects direct entry changes (create/delete/rename).
                                    local_subdirs.append(entry.path)
                                
                                # OPTIMIZATION: ONLY scan/stat files if the directory is NOT silent.
                                elif not is_silent and fnmatch.fnmatch(entry.name, file_pattern):
                                    st = entry.stat()
                                    meta = get_file_metadata(entry.path, stat_info=st)
                                    # Add parent info for Rule 3 arbitration
                                    meta['parent_path'] = root
                                    meta['parent_mtime'] = current_dir_mtime
                                    local_batch.append(meta)
                                    if len(local_batch) >= batch_size:
                                        results_queue.put((UpdateEvent(
                                            event_schema=self.uri,
                                            table="files",
                                            rows=local_batch,
                                            fields=list(local_batch[0].keys()),
                                            message_source=MessageSource.AUDIT,
                                            index=audit_time
                                        ), {}))
                                        local_batch = []
                            except OSError:
                                pass

                    if local_batch:
                        results_queue.put((UpdateEvent(
                            event_schema=self.uri,
                            table="files",
                            rows=local_batch,
                            fields=list(local_batch[0].keys()),
                            message_source=MessageSource.AUDIT,
                            index=audit_time
                        ), {}))
                    elif not is_silent:
                        # Only report empty result if directory was actually scanned
                        results_queue.put((None, {root: current_dir_mtime}))
                    else:
                        # For silent directory, we've already sent the audit_skipped event.
                        # Do not send None event which would update mtime_cache incorrectly without full scan.
                        pass

                    with map_lock:
                        for sd in local_subdirs:
                            pending_dirs += 1
                            work_queue.put((sd, root))

                except OSError:
                    pass
                finally:
                    with map_lock:
                        pending_dirs -= 1
                    work_queue.task_done()

        workers = []
        for _ in range(self._executor._max_workers):
            t = threading.Thread(target=audit_worker, daemon=True)
            t.start()
            workers.append(t)

        while True:
            try:
                event_tuple = results_queue.get(timeout=0.1)
                yield event_tuple
            except queue.Empty:
                if pending_dirs == 0 and work_queue.empty():
                    break
        
        stop_workers.set()
        for t in workers:
            t.join()
        logger.info(f"[{stream_id}] Audit scan completed.")



    def perform_sentinel_check(self, task_batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Implements generic sentinel check.
        Supported types: 'suspect_check'
        """
        task_type = task_batch.get('type')
        if task_type == 'suspect_check':
             paths = task_batch.get('paths', [])
             results = self.verify_files(paths)
             return {'type': 'suspect_update', 'updates': results}
        return {}

    def verify_files(self, paths: List[str]) -> List[Dict[str, Any]]:
        """
        Verifies the existence and mtime of the given file paths.
        Used for Sentinel Sweep.
        """
        def verify_single(path):
            try:
                stat_info = os.stat(path)
                return {
                    "path": path,
                    "mtime": stat_info.st_mtime,
                    "status": "exists"
                }
            except FileNotFoundError:
                return {
                    "path": path,
                    "mtime": 0.0,
                    "status": "missing"
                }
            except Exception as e:
                logger.warning(f"[fs] Error verifying file {path}: {e}")
                return None

        # Process in parallel using the driver's executor
        results = list(self._executor.map(verify_single, paths))
        # Filter out failed ones
        return [r for r in results if r is not None]

    @classmethod
    async def get_available_fields(cls, **kwargs) -> Dict[str, Any]:
        return {"properties": {
            "file_path": {"type": "string", "description": "The full, absolute path to the file.", "column_index": 0},
            "size": {"type": "integer", "description": "The size of the file in bytes.", "column_index": 1},
            "modified_time": {"type": "number", "description": "The last modification time as a Unix timestamp (float).", "column_index": 2},
            "created_time": {"type": "number", "description": "The creation time as a Unix timestamp (float).", "column_index": 3},
        }}

    @classmethod
    async def test_connection(cls, **kwargs) -> Tuple[bool, str]:
        path = kwargs.get("uri")
        if not path or not isinstance(path, str):
            return (False, "路径未提供或格式不正确。")
        if not os.path.exists(path):
            return (False, f"路径不存在: {path}")
        if not os.path.isdir(path):
            return (False, f"路径不是一个目录: {path}")
        if not os.access(path, os.R_OK):
            return (False, f"没有读取权限: {path}")
        return (True, "连接成功，路径有效且可读。")

    @classmethod
    async def check_privileges(cls, **kwargs) -> Tuple[bool, str]:
        path = kwargs.get("uri")
        if not path:
            return (False, "Path not provided in arguments.")

        try:
            user = getpass.getuser()
        except Exception:
            user = "unknown"

        logger.info(f"[fs] Checking permissions for user '{user}' on path: {safe_path_handling(path)}")
        
        if not os.path.exists(path):
            return (False, f"路径不存在: {path}")
        if not os.path.isdir(path):
            return (False, f"路径不是一个目录: {path}")

        can_read = os.access(path, os.R_OK)
        can_execute = os.access(path, os.X_OK)

        if can_read and can_execute:
            return (True, f"权限充足：当前用户 '{user}' 可以监控该目录。")
        
        missing_perms = []
        if not can_read:
            missing_perms.append("读取")
        if not can_execute:
            missing_perms.append("执行(进入)")
        
        return (False, f"权限不足：当前用户 '{user}' 缺少 {' 和 '.join(missing_perms)} 权限。")

    async def close(self):
        """
        Close the file system watcher and stop monitoring.
        """
        logger.info(f"[fs] Closing file system watcher for {self.uri}")
        
        # Stop the watch manager if it's running
        if hasattr(self, 'watch_manager') and self.watch_manager:
            self.watch_manager.stop()
        
        # Set the stop event to ensure any active monitoring stops
        if hasattr(self, '_stop_driver_event') and self._stop_driver_event:
            self._stop_driver_event.set()
        
        logger.info(f"[fs] Closed file system watcher for {self.uri}")

    @classmethod
    async def get_wizard_steps(cls) -> Dict[str, Any]:
        return {
            "steps": [
                {
                    "step_id": "path_setup",
                    "title": "目录与权限",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "uri": {
                                "type": "string",
                                "title": "监控目录路径",
                                "description": "请输入要监控的文件夹的绝对路径。"
                            },
                            "driver_params": {
                                "type": "object",
                                "title": "驱动参数",
                                "properties": {
                                    "aged_interval": {
                                        "type": "number",
                                        "title": "被忽略监控的陈旧文件夹的年龄 (days)",
                                        "default": 0.5
                                    },
                                    "max_sync_delay_seconds": {
                                        "type": "number",
                                        "title": "最大同步延迟 (秒)",
                                        "description": "实时推送的最大延迟时间。如果超过此时间没有事件，将强制推送一次。",
                                        "default": 1.0
                                    },
                                    "min_monitoring_window_days": {
                                        "type": "number",
                                        "title": "最小监控窗口 (天)",
                                        "description": "当需要淘汰监控目录时，确保被淘汰的目录比整个监控范围内最新的文件至少旧N天。这可以防止淘汰近期仍在活跃范围内的目录。例如，设置为30，则表示只有比最新文件早30天以上的目录才允许被淘汰。",
                                        "default": 30.0
                                    }
                                }
                            }
                        },
                        "required": ["uri"],
                    },
                    "validations": ["test_connection", "check_privileges"]
                }
            ]
        }