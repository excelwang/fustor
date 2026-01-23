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
from typing import Any, Dict, Iterator, List, Tuple
from fustor_core.drivers import SourceDriver
from fustor_core.models.config import SourceConfig
from fustor_event_model.models import EventBase, UpdateEvent, DeleteEvent, MessageSource

from .components import _WatchManager, safe_path_handling
from .event_handler import OptimizedWatchEventHandler, get_file_metadata

logger = logging.getLogger("fustor_agent.driver.fs")
            
import threading

class FSDriver(SourceDriver):
    _instances: Dict[str, 'FSDriver'] = {}
    _lock = threading.Lock()
    
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
        self.clock_offset = 0.0  # Placeholder for potential future use
        self._stop_driver_event = threading.Event() # NEW
        min_monitoring_window_days = self.config.driver_params.get("min_monitoring_window_days", 30.0)
        self.watch_manager = _WatchManager(self.uri, event_handler=None, min_monitoring_window_days=min_monitoring_window_days, stop_driver_event=self._stop_driver_event)
        self.event_handler = OptimizedWatchEventHandler(self.event_queue, self.watch_manager)
        self.watch_manager.event_handler = self.event_handler
        self._pre_scan_completed = False
        self._pre_scan_lock = threading.Lock()
        self._stop_driver_event = threading.Event() # NEW
        
        self._initialized = True

    def _perform_pre_scan_and_schedule(self):
        """
        Performs a one-time scan of the directory to populate the watch manager
        with a capacity-aware, hierarchy-complete set of the most active directories.
        It uses a delta to normalize server mtimes to the client's time domain.
        """
        with self._pre_scan_lock:
            if self._pre_scan_completed:
                return

            logger.info(f"[fs] Performing initial directory scan to build hot-directory map for: {self.uri}")
            
            mtime_map: Dict[str, float] = {}
            
            # Track statistics
            error_count = 0
            total_entries = 0  # Total number of entries (directories and files) processed
            
            def handle_walk_error(e: OSError):
                nonlocal error_count
                error_count += 1
                logger.debug(f"[fs] Error during pre-scan walk, skipping path: {e.filename} - {e.strerror}")

            # Step 1: Walk the entire tree to build the mtime_map with server times
            for root, dirs, files in os.walk(self.uri, topdown=False, onerror=handle_walk_error):
                try:
                    latest_mtime = os.path.getmtime(root)
                except OSError:
                    continue

                for filename in files:
                    file_path = os.path.join(root, filename)
                    try:
                        stat_info = os.stat(file_path)
                        latest_mtime = max(latest_mtime, stat_info.st_mtime)
                    except (FileNotFoundError, PermissionError, OSError) as e:
                        error_count += 1
                        logger.debug(f"[fs] Error during pre-scan walk, skipping path: {e.filename} - {e.strerror}")

                    # Count each file as an entry
                    total_entries += 1

                for dirname in dirs:
                    dirpath = os.path.join(root, dirname)
                    latest_mtime = max(latest_mtime, mtime_map.get(dirpath, 0))
                    # Count each dir as an entry
                    total_entries += 1
                
                # Count the current directory
                mtime_map[root] = latest_mtime
                total_entries += 1  # Increment for each directory processed
                
                # Log statistics every 1000 entries (using a reasonable batch size)
                if total_entries % 10000 == 0:
                    # Find the newest and oldest directories so far
                    if mtime_map:
                        newest_dir = max(mtime_map.items(), key=lambda x: x[1])
                        oldest_dir = min(mtime_map.items(), key=lambda x: x[1])
                        newest_age = time.time() - newest_dir[1]  # Difference in seconds
                        oldest_age = time.time() - oldest_dir[1]  # Difference in seconds
                        logger.info(
                            f"[fs] Pre-scan progress: processed {total_entries} entries, "
                            f"errors: {error_count}, newest_dir: {newest_dir[0]} (age: {newest_age/86400:.2f} days), "
                            f"oldest_dir: {oldest_dir[0]} (age: {oldest_age/86400:.2f} days)"
                        )
            
            # Step 2: Calculate baseline delta using the true recursive mtime of the root.
            try:
                root_recursive_mtime = mtime_map.get(self.uri, os.path.getmtime(self.uri))
                self.clock_offset = time.time() - root_recursive_mtime
                logger.info(f"[fs] Calculated client-server time delta: {self.clock_offset:.2f} seconds.")
            except OSError as e:
                logger.warning(f"[fs] Could not stat root directory to calculate time delta: {e}. Proceeding without normalization.")

            # Log final statistics before sorting
            if mtime_map:
                newest_dir = max(mtime_map.items(), key=lambda x: x[1])
                oldest_dir = min(mtime_map.items(), key=lambda x: x[1])
                newest_age = time.time() - newest_dir[1]  # Difference in seconds
                oldest_age = time.time() - oldest_dir[1]  # Difference in seconds
                logger.info(
                    f"[fs] Pre-scan completed: processed {total_entries} entries, "
                    f"errors: {error_count}, newest_dir: {safe_path_handling(newest_dir[0])} (age: {newest_age/86400:.2f} days), "
                    f"oldest_dir: {safe_path_handling(oldest_dir[0])} (age: {oldest_age/86400:.2f} days)"
                )

            logger.info(f"[fs] Found {len(mtime_map)} total directories. Building capacity-aware, hierarchy-complete watch set...")
            sorted_dirs = sorted(mtime_map.items(), key=lambda item: item[1], reverse=True)[:self.watch_manager.watch_limit]
            old_limit = self.watch_manager.watch_limit
            for path, _ in sorted_dirs:
                server_mtime = mtime_map.get(path)
                if server_mtime:
                    # Normalize to client time domain while preserving relative differences
                    lru_timestamp = server_mtime + self.clock_offset
                else:
                    # Fallback for parents that might not have been in mtime_map (though they should be)
                    lru_timestamp = time.time()
                self.watch_manager.schedule(path, lru_timestamp)
                if self.watch_manager.watch_limit < old_limit:
                    break  # Stop if we hit the limit during scheduling
            logger.info(f"[fs] Final watch set constructed. Total paths to watch: {len(self.watch_manager.lru_cache)}.")
            self._pre_scan_completed = True


    def get_snapshot_iterator(self, **kwargs) -> Iterator[EventBase]:
        stream_id = f"snapshot-fs-{uuid.uuid4().hex[:6]}"
        logger.info(f"[{stream_id}] Starting Snapshot Scan Phase: for path: {self.uri}")

        driver_params = self.config.driver_params
        if driver_params.get("startup_mode") == "message-only":
            logger.info(f"[{stream_id}] Skipping snapshot due to 'message-only' mode.")
            return

        file_pattern = driver_params.get("file_pattern", "*")
        batch_size = kwargs.get("batch_size", 100)
        
        logger.info(f"[{stream_id}] Scan parameters: file_pattern='{file_pattern}'")

        try:
            batch: List[Dict[str, Any]] = []
            files_processed_count = 0
            error_count = 0
            snapshot_time = int(time.time() * 1000)

            def handle_walk_error(e: OSError):
                nonlocal error_count
                error_count += 1
                logger.debug(f"[{stream_id}] Error during snapshot walk, skipping path: {safe_path_handling(e.filename)} - {e.strerror}")

            temp_mtime_map: Dict[str, float] = {}

            for root, dirs, files in os.walk(self.uri, topdown=False, onerror=handle_walk_error):
                try:
                    dir_stat_info = os.stat(root)
                    latest_mtime_in_subtree = dir_stat_info.st_mtime
                except OSError:
                    dir_stat_info = None
                    latest_mtime_in_subtree = 0.0

                for filename in files:
                    file_path = os.path.join(root, filename)
                    try:
                        stat_info = os.stat(file_path)
                        latest_mtime_in_subtree = max(latest_mtime_in_subtree, stat_info.st_mtime)
                        if fnmatch.fnmatch(filename, file_pattern):
                            # TODO: Exclude files that are still being written to.
                            # Consider checking if mtime is very recent or using lsof-like check.
                            metadata = get_file_metadata(file_path, stat_info=stat_info)
                            if metadata:
                                batch.append(metadata)
                                files_processed_count += 1
                                if len(batch) >= batch_size:
                                    # Extract fields from the first row if batch is not empty
                                    fields = list(batch[0].keys()) if batch else []
                                    yield UpdateEvent(event_schema=self.uri, table="files", rows=batch, index=snapshot_time, fields=fields, message_source=MessageSource.SNAPSHOT)
                                    batch = []
                    except (FileNotFoundError, PermissionError, OSError) as e:
                        error_count += 1
                        logger.debug(f"[fs] Error processing file during snapshot: {safe_path_handling(file_path)} - {str(e)}")

                for dirname in dirs:
                    dirpath = os.path.join(root, dirname)
                    latest_mtime_in_subtree = max(latest_mtime_in_subtree, temp_mtime_map.get(dirpath, 0.0))
                
                temp_mtime_map[root] = latest_mtime_in_subtree
                aligned_lru_timestamp = latest_mtime_in_subtree + self.clock_offset
                self.watch_manager.touch(root, aligned_lru_timestamp, is_recursive_upward=False)

                if dir_stat_info:
                    dir_metadata = get_file_metadata(root, stat_info=dir_stat_info)
                    if dir_metadata:
                        batch.append(dir_metadata)
                        files_processed_count += 1
                
                if len(batch) >= batch_size:
                    # Extract fields from the first row if batch is not empty
                    fields = list(batch[0].keys()) if batch else []
                    yield UpdateEvent(event_schema=self.uri, table="files", rows=batch, index=snapshot_time, fields=fields, message_source=MessageSource.SNAPSHOT)
                    batch = []
            
            if batch:
                fields = list(batch[0].keys()) if batch else []
                yield UpdateEvent(event_schema=self.uri, table="files", rows=batch, index=snapshot_time, fields=fields, message_source=MessageSource.SNAPSHOT)

            if error_count > 0:
                logger.warning(f"[{stream_id}] Skipped {error_count} paths in total due to permission or other errors.")

            logger.info(f"[{stream_id}] Full scan complete. Processed {files_processed_count} files and directories.")

        except Exception as e:
            logger.error(f"[{stream_id}] Snapshot phase for fs failed: {e}", exc_info=True)

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

    def get_audit_iterator(self, mtime_cache: Dict[str, float] = None, **kwargs) -> Iterator[EventBase]:
        """
        Audit Sync: Fast directory scanning using mtime optimization.
        
        Uses directory mtime to skip unchanged subtrees:
        - If directory mtime hasn't changed since last audit, skip scanning its direct children
        - Still recurse into subdirectories to check their mtimes
        
        Args:
            mtime_cache: Previous audit's mtime cache {dir_path: mtime}
            
        Yields:
            UpdateEvent with message_source=AUDIT and parent_mtime in each row
        """
        stream_id = f"audit-fs-{uuid.uuid4().hex[:6]}"
        logger.info(f"[{stream_id}] Starting Audit Scan for path: {self.uri}")
        
        if mtime_cache is None:
            mtime_cache = {}
        
        new_mtime_cache: Dict[str, float] = {}
        batch_size = kwargs.get("batch_size", 100)
        file_pattern = self.config.driver_params.get("file_pattern", "*")
        audit_time = int(time.time() * 1000)
        
        files_scanned = 0
        dirs_scanned = 0
        dirs_skipped = 0
        error_count = 0
        
        batch: List[Dict[str, Any]] = []
        
        def handle_walk_error(e: OSError):
            nonlocal error_count
            error_count += 1
            logger.debug(f"[{stream_id}] Error during audit walk: {safe_path_handling(e.filename)} - {e.strerror}")

        for root, dirs, files in os.walk(self.uri, topdown=True, onerror=handle_walk_error):
            try:
                dir_stat = os.stat(root)
                current_dir_mtime = dir_stat.st_mtime
            except OSError:
                continue
            
            new_mtime_cache[root] = current_dir_mtime
            dirs_scanned += 1
            
            # Check if directory mtime changed since last audit
            cached_mtime = mtime_cache.get(root)
            if cached_mtime is not None and cached_mtime == current_dir_mtime:
                # Directory unchanged - skip scanning its direct children files
                # But we still need to recurse into subdirectories
                dirs_skipped += 1
                continue
            
            # Directory changed or new - scan all files
            for filename in files:
                if not fnmatch.fnmatch(filename, file_pattern):
                    continue
                    
                file_path = os.path.join(root, filename)
                try:
                    stat_info = os.stat(file_path)
                    metadata = get_file_metadata(file_path, stat_info=stat_info)
                    if metadata:
                        # Add parent_mtime for Fusion's arbitration
                        metadata["parent_mtime"] = current_dir_mtime
                        batch.append(metadata)
                        files_scanned += 1
                        
                        if len(batch) >= batch_size:
                            fields = list(batch[0].keys()) if batch else []
                            yield UpdateEvent(
                                event_schema=self.uri,
                                table="files",
                                rows=batch,
                                index=audit_time,
                                fields=fields,
                                message_source=MessageSource.AUDIT
                            )
                            batch = []
                except (FileNotFoundError, PermissionError, OSError) as e:
                    error_count += 1
                    logger.debug(f"[{stream_id}] Error processing file: {safe_path_handling(file_path)} - {str(e)}")
            
            # Also scan directories in this dir (for metadata updates)
            for dirname in dirs:
                dir_path = os.path.join(root, dirname)
                try:
                    sub_stat = os.stat(dir_path)
                    dir_metadata = get_file_metadata(dir_path, stat_info=sub_stat)
                    if dir_metadata:
                        dir_metadata["parent_mtime"] = current_dir_mtime
                        batch.append(dir_metadata)
                        
                        if len(batch) >= batch_size:
                            fields = list(batch[0].keys()) if batch else []
                            yield UpdateEvent(
                                event_schema=self.uri,
                                table="files",
                                rows=batch,
                                index=audit_time,
                                fields=fields,
                                message_source=MessageSource.AUDIT
                            )
                            batch = []
                except (FileNotFoundError, PermissionError, OSError):
                    pass
        
        # Yield remaining batch
        if batch:
            fields = list(batch[0].keys()) if batch else []
            yield UpdateEvent(
                event_schema=self.uri,
                table="files",
                rows=batch,
                index=audit_time,
                fields=fields,
                message_source=MessageSource.AUDIT
            )
        
        logger.info(
            f"[{stream_id}] Audit complete. "
            f"Files scanned: {files_scanned}, Dirs scanned: {dirs_scanned}, "
            f"Dirs skipped (unchanged): {dirs_skipped}, Errors: {error_count}"
        )
        
        # Return the new cache for next audit cycle
        # Caller should store this: kwargs['mtime_cache_out'] = new_mtime_cache
        if 'mtime_cache_out' in kwargs:
            kwargs['mtime_cache_out'].update(new_mtime_cache)



    def perform_consistency_check(self, task_batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Implements generic consistency check.
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
        results = []
        for path in paths:
            try:
                stat_info = os.stat(path)
                results.append({
                    "path": path,
                    "mtime": stat_info.st_mtime,
                    "status": "exists"
                })
            except FileNotFoundError:
                results.append({
                    "path": path,
                    "mtime": 0.0,
                    "status": "missing"
                })
            except Exception as e:
                logger.warning(f"[fs] Error verifying file {path}: {e}")
        return results

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