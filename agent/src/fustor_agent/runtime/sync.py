import asyncio
import logging
import queue
import threading
import time
from typing import Optional, Any, TYPE_CHECKING, Dict
from datetime import datetime, timezone

from fustor_core.models.config import SyncConfig, SenderConfig, SourceConfig
from fustor_core.models.states import SyncState, SyncInstanceDTO
from fustor_core.exceptions import DriverError
from fustor_agent.runtime.bus import EventBusFailedError

if TYPE_CHECKING:
    from fustor_agent.services.instances.bus import EventBusInstanceRuntime, EventBusService
    from fustor_agent.services.drivers.sender_driver import SenderDriverService
    from fustor_agent.services.drivers.source_driver import SourceDriverService

logger = logging.getLogger("fustor_agent")

class SyncInstance:
    def __init__(
        self,
        id: str,
        agent_id: str,
        config: SyncConfig,
        source_config: SourceConfig,
        pusher_config: SenderConfig,  # Type updated, param name kept for compatibility
        bus_service: "EventBusService",
        pusher_driver_service: "SenderDriverService",  # Type updated
        source_driver_service: "SourceDriverService",
        pusher_schema: Dict[str, Any],
        initial_statistics: Optional[Dict[str, Any]] = None
    ):
        self.id = id
        self.task_id = f"{agent_id}:{id}"
        self.config = config
        self.source_config = source_config
        self.pusher_config = pusher_config
        self.bus_service = bus_service
        self.pusher_schema = pusher_schema
        self.session_id: Optional[str] = None
        
        source_driver_class = source_driver_service._get_driver_by_type(self.source_config.driver)
        self.source_driver_instance = source_driver_class(config.source, self.source_config)

        pusher_driver_class = pusher_driver_service._get_driver_by_type(self.pusher_config.driver)
        self.pusher_driver_instance = pusher_driver_class(config.sender, self.pusher_config)

        self.bus: Optional["EventBusInstanceRuntime"] = None
        self.state: SyncState = SyncState.STOPPED
        self.info: str = "实例已创建, 等待启动。"
        self._main_task: Optional[asyncio.Task] = None
        self._statistics: Dict[str, Any] = initial_statistics if initial_statistics is not None else {"events_pushed": 0, "last_pushed_event_id": None}
        self.metrics: Dict[str, Any] = {"events_pushed_total": 0, "sync_push_latency_seconds": []}

        self._snapshot_task: Optional[asyncio.Task] = None
        self._audit_task: Optional[asyncio.Task] = None
        self._sentinel_task: Optional[asyncio.Task] = None
        self._message_sync_task: Optional[asyncio.Task] = None
        self._monitor_heartbeat_task: Optional[asyncio.Task] = None
        self.audit_mtime_cache: Dict[str, float] = {}
        self.current_role: Optional[str] = None  # Track current role (leader/follower)
        
        # Heartbeat-related attributes
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stop_heartbeat_event = asyncio.Event()
        self._heartbeat_error_event = asyncio.Event()  # Event to signal heartbeat error
        self._last_active_time = datetime.now(timezone.utc)
        self._last_active_time = datetime.now(timezone.utc)
        # Heartbeat interval is now dynamically set by the session response from Fusion
        # Default to 10s until session is established
        self.heartbeat_interval: int = 10 
        # Use config values for consistency intervals (with fallback defaults)
        self.audit_interval: int = getattr(config, 'audit_interval_sec', 600)
        self.sentinel_interval: int = getattr(config, 'sentinel_interval_sec', 120)

        self._fast_mapper_fn = self._compile_mapper_function()

    def __str__(self):
        return f"Sync Instance {self.id}"
    
    def _set_state(self, new_state: SyncState, info: Optional[str] = None):
        # This method is for setting primary, exclusive states.
        # For adding/removing flags, directly manipulate self.state using bitwise operators.
        if self.state != new_state:
            logger.info(f"同步任务 '{self.id}' 状态变更: {self.state.name} -> {new_state.name}")
            self.state = new_state
        if info is not None:
            self.info = info

    async def start(self):
        if self.state != SyncState.STOPPED:
            logger.warning(f"任务 '{self.id}' 已在运行或处于非停止状态 ({self.state.name})，无法启动。")
            return

        if self._main_task and not self._main_task.done():
            self._main_task.cancel()
        
        self._main_task = asyncio.create_task(self._run_control_loop())
        
        # Start heartbeat task
        self._heartbeat_task = asyncio.create_task(self._run_heartbeat_loop())

    async def _run_heartbeat_loop(self):
        """后台心跳循环，定期更新锁状态"""
        while not self._stop_heartbeat_event.is_set():
            # Wait for session ID to be available before starting heartbeats
            while not self.session_id and not self._stop_heartbeat_event.is_set():
                await asyncio.sleep(0.5)  # Brief wait before checking again
            
            # If we're stopping, exit the loop
            if self._stop_heartbeat_event.is_set():
                break
                
            try:
                await self._send_heartbeat()
                
                await asyncio.wait_for(
                    self._stop_heartbeat_event.wait(), 
                    timeout=self.heartbeat_interval
                )
            except asyncio.TimeoutError:
                continue # Continue to next heartbeat
            except DriverError as e:
                logger.error(f"Heartbeat failed for sync '{self.id}' after multiple retries: {e}", exc_info=False)
                self._heartbeat_error_event.set()
                break # Exit loop on critical failure
            except Exception as e:
                logger.error(f"An unexpected error occurred in the heartbeat loop for sync '{self.id}': {e}", exc_info=True)
                self._heartbeat_error_event.set()
                break # Exit loop on critical failure

    async def _send_heartbeat(self):
        """发送心跳以维持会话状态"""
        result = await self.pusher_driver_instance.heartbeat(
            session_id=self.session_id
        )
        logger.debug(f"Heartbeat sent for sync '{self.id}', result: {result}")
        
        # Handle dynamic role updates (e.g. failover promotion)
        server_role = result.get('role')
        if server_role:
             await self._handle_role_change(server_role)
             
        return result

    async def _handle_role_change(self, new_role: str):
        if new_role == self.current_role:
             return
             
        logger.info(f"Role change detected for {self.id}: {self.current_role} -> {new_role}")
        self.current_role = new_role
        
        if new_role == 'leader':
             # Promoted to leader
             logger.info(f"Promoted to LEADER. Starting maintenance tasks and triggering supplemental snapshot to claim authority.")
             
             # IMPORTANT: When becoming leader (especially after failover), we must ensure Fusion 
             # recognizes us as the authoritative source for the full state.
             # Fusion resets 'snapshot_complete' when authoritative session changes.
             # So we must perform a snapshot to re-establish readiness.
             # Sequence: Snapshot -> (if success) -> Audit & Sentinel
             if not self._snapshot_task or self._snapshot_task.done():
                 self._snapshot_task = asyncio.create_task(self._run_leader_sequence())
                 
        elif new_role == 'follower':
             # Demoted to follower
             logger.info(f"Demoted to FOLLOWER. Stopping maintenance tasks.")
             if self._audit_task and not self._audit_task.done():
                 self._audit_task.cancel()
                 try:
                     await self._audit_task
                 except asyncio.CancelledError:
                     pass
             self._audit_task = None
             
             if self._sentinel_task and not self._sentinel_task.done():
                 self._sentinel_task.cancel()
                 try:
                     await self._sentinel_task
                 except asyncio.CancelledError:
                     pass
             self._sentinel_task = None
    async def stop(self):
        if self.state == SyncState.STOPPED or SyncState.STOPPING in self.state:
            return
        
        # Only change state if we're not already in an error state
        if self.state != SyncState.ERROR:
            self._set_state(SyncState.STOPPING, "正在停止任务...")
        else:
            # If in error state, keep error state but perform cleanup
            logger.info(f"任务 '{self.id}' 处于错误状态，正在清理资源。")
        
        # 停止心跳任务
        if self._heartbeat_task:
            self._stop_heartbeat_event.set()
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            except DriverError:
                # If heartbeat task ended with DriverError, it's already handled
                pass
        
        if self._main_task and not self._main_task.done():
            self._main_task.cancel()
        
        try:
            if self._main_task:
                await self._main_task
        except asyncio.CancelledError:
            pass

        # Close the driver's clients
        await self.pusher_driver_instance.close()
        
        # Also close the source driver to stop any background monitoring tasks
        if hasattr(self, 'source_driver_instance') and self.source_driver_instance:
            try:
                await self.source_driver_instance.close()
            except Exception as e:
                logger.warning(f"Error closing source driver instance: {e}")

        # Only set to STOPPED if not in error state
        if self.state != SyncState.ERROR:
            self._set_state(SyncState.STOPPED, "任务已停止。")
        else:
            logger.info(f"任务 '{self.id}' 保持错误状态，已停止相关任务。")

    async def _run_control_loop(self):
        try:
            # First, request a session from the Ingestor via the pusher driver
            # Retry with backoff to handle transient API key sync delays (e.g. 401 during startup)
            session_data = None
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    session_data = await self.pusher_driver_instance.create_session(self.task_id)
                    break
                except Exception as e:
                    if attempt < max_retries - 1 and ("401" in str(e) or "Unauthorized" in str(e)):
                        logger.warning(f"Failed to create session (attempt {attempt+1}/{max_retries}) due to unauthorized error. Retrying in 2s...")
                        await asyncio.sleep(2)
                    else:
                        raise
            
            if not session_data:
                raise RuntimeError("Failed to obtain session after retries")

            self.session_id = session_data.get("session_id")
            role = session_data.get("role", "follower")
            self.heartbeat_interval = session_data.get("suggested_heartbeat_interval_seconds", 10)
            self.current_role = role
            
            logger.info(f"任务 '{self.id}' 正在启动，已从Ingestor获取会话 ID: {self.session_id}, Role: {self.current_role}")
            
            self._set_state(SyncState.STARTING, "正在向接收端查询最新同步点位...")
            start_position = await self.pusher_driver_instance.get_latest_committed_index(session_id=self.session_id)

            # Set MESSAGE_SYNC state using bitwise OR
            self.state |= SyncState.MESSAGE_SYNC
            self.info = f"任务启动，进入消息同步阶段，起始点位: {start_position}"
            
            # Create tasks for monitoring heartbeat errors and message sync
            self._monitor_heartbeat_task = asyncio.create_task(self._monitor_heartbeat_errors())
            self._message_sync_task = asyncio.create_task(self._run_message_sync(start_position))

            tasks_to_wait = [self._monitor_heartbeat_task, self._message_sync_task, self._heartbeat_task]
            
            if self.current_role == "leader":
                 logger.info(f"Assigned LEADER role for {self.id}. Starting Leader duties sequence.")
                 # We use the snapshot_task variable to track the sequence task, as it starts with snapshot
                 self._snapshot_task = asyncio.create_task(self._run_leader_sequence())
            
            # Wait for any of the tasks to complete
            done, pending = await asyncio.wait(
                tasks_to_wait,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Check if any completed task had an exception
            for task in done:
                if task.exception() is not None:
                    # If the heartbeat task had an exception, it means heartbeat failed
                    if task == self._heartbeat_task:
                        try:
                            await task  # This will raise the exception from the heartbeat task
                        except DriverError as e:
                            logger.error(f"Heartbeat task failed: {e}")
                            self._set_state(SyncState.ERROR, f"心跳失败: {e}")
                            # Set the heartbeat error event to notify other tasks
                            self._heartbeat_error_event.set()
                        except Exception as e:
                            logger.error(f"Heartbeat task failed with unexpected error: {e}")
                            self._set_state(SyncState.ERROR, f"心跳任务异常: {e}")
                            self._heartbeat_error_event.set()
                    else:
                        # Let the regular exception handling handle other exceptions
                        task.result()  # This will raise the exception if there was one

            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        except asyncio.CancelledError:
            logger.info(f"同步任务 '{self.id}' 被取消。")
        except DriverError as e:
            logger.error(f"同步任务 '{self.id}' 因驱动程序错误而失败: {e}")
            self._set_state(SyncState.ERROR, f"主控制循环崩溃: {e}")
        except EventBusFailedError as e:
            error_msg = f"数据总线错误: {e}"
            logger.error(f"同步任务 '{self.id}' 因底层事件总线错误而失败: {e}", exc_info=True)
            self._set_state(SyncState.ERROR, error_msg)
        except Exception as e:
            logger.error(f"同步任务 '{self.id}' 崩溃: {e}", exc_info=True)
            self._set_state(SyncState.ERROR, f"主控制循环崩溃: {e}")
        finally:
            # Cancel all background tasks when exiting the control loop
            if self._monitor_heartbeat_task and not self._monitor_heartbeat_task.done():
                self._monitor_heartbeat_task.cancel()
                try:
                    await self._monitor_heartbeat_task
                except asyncio.CancelledError:
                    pass

            if self._message_sync_task and not self._message_sync_task.done():
                self._message_sync_task.cancel()
                try:
                    await self._message_sync_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    # Log message sync cleanup errors if any
                    logger.debug(f"Error during message sync task cancellation: {e}")

            # Cancel the heartbeat task when exiting the control loop
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    # The heartbeat task might have already ended with an exception
                    pass

            if self._snapshot_task and not self._snapshot_task.done():
                self._snapshot_task.cancel()
                try:
                    await self._snapshot_task
                except asyncio.CancelledError:
                    pass

            if self._audit_task and not self._audit_task.done():
                self._audit_task.cancel()
                try:
                    await self._audit_task
                except asyncio.CancelledError:
                    pass

            if self._sentinel_task and not self._sentinel_task.done():
                self._sentinel_task.cancel()
                try:
                    await self._sentinel_task
                except asyncio.CancelledError:
                    pass
            # Ensure MESSAGE_SYNC is removed if it was set
            self.state &= ~SyncState.MESSAGE_SYNC
            logger.info(f"同步任务 '{self.id}' 结束。")

    async def _monitor_heartbeat_errors(self):
        """Monitor for heartbeat errors and set error state when they occur."""
        try:
            # Wait for the heartbeat error event to be set
            await self._heartbeat_error_event.wait()
            
            # When the event is set, it indicates a heartbeat failure that should stop the sync
            error_msg = f"心跳失败，同步任务停止。"
            self._set_state(SyncState.ERROR, error_msg)
            logger.error(f"Sync '{self.id}' stopped due to heartbeat failure: {error_msg}")
            
        except asyncio.CancelledError:
            # If the task is cancelled, just return
            pass

    async def trigger_audit(self):
        """Manually trigger an audit cycle."""
        if self.current_role != 'leader':
            logger.warning(f"Cannot trigger audit for {self.id}: not a leader.")
            return
        if self.state & SyncState.AUDIT_SYNC:
            logger.warning(f"Audit for {self.id} already running.")
            return
        logger.info(f"Manually triggering audit for {self.id}")
        await self._run_audit_sync()

    async def trigger_sentinel(self):
        """Manually trigger a sentinel check."""
        if self.current_role != 'leader':
            logger.warning(f"Cannot trigger sentinel for {self.id}: not a leader.")
            return
        if self.state & SyncState.SENTINEL_SWEEP:
            logger.warning(f"Sentinel check for {self.id} already running.")
            return
        logger.info(f"Manually triggering sentinel for {self.id}")
        await self._run_sentinel_check()

    async def _run_audit_loop(self):
        if self.audit_interval <= 0:
            logger.info(f"审计循环已禁用 audit_loop disabled for {self.id} (interval={self.audit_interval}s)")
            return

        logger.info(f"开启审计循环 audit_loop for {self.id}, interval={self.audit_interval}s")
        while True:
            try:
                await asyncio.sleep(self.audit_interval)
                await self._run_audit_sync()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in audit_loop for {self.id}: {e}", exc_info=True)
                await asyncio.sleep(60) # Backoff

    async def _run_sentinel_loop(self):
        if self.sentinel_interval <= 0:
            logger.info(f"哨兵巡检循环已禁用 sentinel_loop disabled for {self.id} (interval={self.sentinel_interval}s)")
            return

        logger.info(f"开启哨兵巡检循环 sentinel_loop for {self.id}, interval={self.sentinel_interval}s")
        while True:
            try:
                await asyncio.sleep(self.sentinel_interval)
                await self._run_sentinel_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sentinel_loop for {self.id}: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def _run_sentinel_check(self):
        if self.state & SyncState.SENTINEL_SWEEP:
            logger.warning(f"Sentinel check for {self.id} already running, skipping.")
            return

        self.state |= SyncState.SENTINEL_SWEEP
        try:
             # 1. Ask Pusher for tasks (Generic)
             tasks = await self.pusher_driver_instance.get_sentinel_tasks(source_id=self.config.source)
             
             if not tasks:
                 return

             # 2. Ask Source to execute (Generic)
             # Run in thread as source drivers might be blocking (like FS OS operations)
             # Note: perform_sentinel_check now has a default implementation in base driver, 
             # so we can call it directly, but 'hasattr' check is safe if base class varies.
             if hasattr(self.source_driver_instance, 'perform_sentinel_check'):
                 results = await asyncio.to_thread(
                     self.source_driver_instance.perform_sentinel_check, 
                     tasks
                 )
             else:
                 logger.debug("Source driver does not support perform_sentinel_check.")
                 results = {}

             if not results:
                 return

             # 3. Report Results (Generic)
             await self.pusher_driver_instance.submit_sentinel_results(results)
             logger.info(f"Sentinel check completed for {self.id}")

        except Exception as e:
            logger.error(f"Sentinel check failed for {self.id}: {e}")
        finally:
             self.state &= ~SyncState.SENTINEL_SWEEP

    async def _run_audit_sync(self):
        # Rule: Only the Leader performs Audit scans to prevent concurrent interference and redundant IO.
        if self.current_role != 'leader':
            return
            
        if self.state & SyncState.AUDIT_SYNC:
             return
        
        self.state |= SyncState.AUDIT_SYNC
        logger.info(f"Audit sync started for {self.id}")
        
        try:
            await self.pusher_driver_instance.signal_audit_start(source_id=self.config.source)
        except NotImplementedError:
             pass

        # Prepare for threaded execution
        snapshot_completed_successfully = False
        queue_size = 100
        event_queue = queue.Queue(maxsize=queue_size)
        stop_event = threading.Event()

        try:
            def _threaded_audit_producer():
                try:
                    iterator = self.source_driver_instance.get_audit_iterator(
                        mtime_cache=self.audit_mtime_cache,
                        batch_size=self.pusher_config.batch_size
                    )
                    for item in iterator:
                        if stop_event.is_set():
                            break
                        event_queue.put(item)
                except Exception as e:
                    logger.error(f"Audit producer thread for '{self.id}' failed: {e}", exc_info=True)
                    event_queue.put(e)
                finally:
                    event_queue.put(None)

            producer_thread = threading.Thread(target=_threaded_audit_producer, daemon=True)
            producer_thread.start()

            while True:
                item = await asyncio.to_thread(event_queue.get)

                if item is None:
                    snapshot_completed_successfully = True
                    break
                
                if isinstance(item, Exception):
                    raise item

                current_event, finalized_mtimes = item

                if current_event and current_event.rows:
                    await self.pusher_driver_instance.push(
                        events=[current_event], 
                        session_id=self.session_id, 
                        source_type='audit' # Important for Fusion to distinguish
                    )
                    self._statistics["events_pushed"] += len(current_event.rows)
                
                # Incremental Update: commit mtimes to local cache after successful push
                if finalized_mtimes:
                    self.audit_mtime_cache.update(finalized_mtimes)
            
            if snapshot_completed_successfully:
                try:
                    await self.pusher_driver_instance.signal_audit_end(source_id=self.config.source)
                except NotImplementedError:
                    pass
                    
                logger.info(f"Audit sync completed for {self.id}")

        except Exception as e:
            logger.error(f"Audit sync failed for {self.id}: {e}", exc_info=True)
        finally:
            stop_event.set()
            if 'producer_thread' in locals() and producer_thread.is_alive():
                producer_thread.join(timeout=5.0)
            self.state &= ~SyncState.AUDIT_SYNC

    async def _run_leader_sequence(self):
        """
        Orchestrates the sequence of Leader duties:
        1. Snapshot Sync (Blocking, Required for Readiness)
        2. Audit Loop (Starts only if Snapshot succeeds)
        3. Sentinel Loop (Starts with Audit)
        """
        logger.info(f"[{self.id}] Starting Leader Sequence: Snapshot -> Audit")
        
        # 1. Run Snapshot Sync and wait for it
        success = await self._run_snapshot_sync()
        
        if success and self.current_role == 'leader' and self.state != SyncState.STOPPED:
            logger.info(f"[{self.id}] Snapshot complete. Starting Audit and Sentinel loops.")
            
            # 2. Start Audit Loop
            if not self._audit_task or self._audit_task.done():
                self._audit_task = asyncio.create_task(self._run_audit_loop())
                # Trigger immediate audit for faster test convergence
                asyncio.create_task(self.trigger_audit())
                
            # 3. Start Sentinel Loop
            if not self._sentinel_task or self._sentinel_task.done():
                self._sentinel_task = asyncio.create_task(self._run_sentinel_loop())
        else:
            logger.warning(f"[{self.id}] Snapshot failed or role changed. Skipping Audit/Sentinel start.")

    async def _run_snapshot_sync(self) -> bool:
        # Add SNAPSHOT_SYNC state using bitwise OR
        self.state |= SyncState.SNAPSHOT_SYNC
        self.info = "补充性质的快照同步任务开始运行。"
        logger.info(f"Sync instance '{self.id}' starting supplemental snapshot sync.")
        snapshot_completed_successfully = False
        try:
            from fustor_agent.services.instances.bus import RequiredFieldsTracker
            
            event_queue = queue.Queue(maxsize=100)
            stop_event = threading.Event()

            def _threaded_snapshot_producer():
                try:
                    snapshot_field_tracker = RequiredFieldsTracker()
                    required_source_fields = {fm.source[0].split(':')[0] for fm in self.config.fields_mapping if fm.source}
                    snapshot_field_tracker.update_fields(required_source_fields)

                    iterator = self.source_driver_instance.get_snapshot_iterator(
                        batch_size=self.pusher_config.batch_size,
                        required_fields_tracker=snapshot_field_tracker
                    )
                    for event_batch in iterator:
                        if stop_event.is_set():
                            break
                        event_queue.put(event_batch)
                except Exception as e:
                    logger.error(f"Snapshot producer thread for '{self.id}' failed: {e}", exc_info=True)
                    event_queue.put(e)
                finally:
                    event_queue.put(None)

            producer_thread = threading.Thread(target=_threaded_snapshot_producer, daemon=True)
            producer_thread.start()

            while True:
                current_event = await asyncio.to_thread(event_queue.get)
                
                if current_event is None:
                    snapshot_completed_successfully = True # Set flag on natural exit
                    break
                
                if isinstance(current_event, Exception):
                    raise current_event

                if not current_event.rows:
                    continue

                final_rows_for_push = current_event.rows
                if self.config.fields_mapping:
                    mapped_rows = [self._process_field_mapping(row) for row in current_event.rows if self._process_field_mapping(row)]
                    if not mapped_rows:
                        continue
                    final_rows_for_push = mapped_rows
                
                current_event.rows = final_rows_for_push
                try:
                    push_start_time = time.monotonic()
                    await self.pusher_driver_instance.push(events=[current_event], session_id=self.session_id, source_type='snapshot')
                    push_duration = time.monotonic() - push_start_time
                    logger.info(f"Sync '{self.id}' snapshot push latency: {push_duration:.4f} seconds")
                except Exception as e:
                    # Check if this is the SessionObsoletedError specifically
                    if "Session is obsolete" in str(e):
                        logger.warning(f"Snapshot task for sync '{self.id}' is obsolete and was commanded to stop by the ingestor. Stopping gracefully.")
                        break
                    elif isinstance(e, DriverError) and ("419" in str(e) or "obsolete" in str(e).lower()):
                        logger.warning(f"Snapshot task for sync '{self.id}' is obsolete and was commanded to stop by the ingestor. Stopping gracefully.")
                        break
                    else:
                        raise
                self._statistics["events_pushed"] += len(current_event.rows)

            if snapshot_completed_successfully: # Only call if the loop finished naturally
                await self.pusher_driver_instance.push(events=[], task_id=self.id, session_id=self.session_id, is_snapshot_end=True, source_type='snapshot')
                logger.info(f"补充快照同步任务 '{self.id}' 完成。")
                return True

        except Exception as e:
            logger.error(f"补充快照同步任务 '{self.id}' 失败: {e}", exc_info=True)
            return False
        finally:
            stop_event.set()
            if producer_thread.is_alive():
                producer_thread.join(timeout=5.0)
            # Remove SNAPSHOT_SYNC state using bitwise AND with NOT
            self.state &= ~SyncState.SNAPSHOT_SYNC
            self.info = "快照同步任务已清理。"
            self._snapshot_task = None
        
        return False

    async def _run_message_sync(self, start_position: int):
        # Flag to track if we've sent the initial trigger event
        initial_event_sent = False
        
        try:
            self.bus, needed_position_lost = await self.bus_service.get_or_create_bus_for_subscriber(
                source_id=self.config.source,
                source_config=self.source_config,
                sync_id=self.id,
                required_position=start_position,
                fields_mapping=self.config.fields_mapping
            )
            if needed_position_lost:
                logger.warning(f"源 '{self.config.source}' 无法从请求的点位 {start_position} 开始，已从最新点位启动。")
                # 启动快照同步，因为位置丢失
                if not (self._snapshot_task and not self._snapshot_task.done()):
                    self._snapshot_task = asyncio.create_task(self._run_snapshot_sync())
                else:
                    logger.info(f"任务 '{self.id}' 已有一个快照正在运行，本次快照请求被忽略。")
        except DriverError as e:
            error_msg = f"无法启动事件总线或源驱动: {e}"
            self._set_state(SyncState.ERROR, error_msg)
            logger.error(error_msg, exc_info=True)
            return

        while SyncState.MESSAGE_SYNC in self.state:
            events_batch = await self.bus.internal_bus.get_events_for(
                self.id,
                self.pusher_config.batch_size,
                0.2
            )
            
            # If no events received and we haven't sent the initial trigger, create a fake event
            if not events_batch and not initial_event_sent:
                from fustor_core.event import UpdateEvent
                # Create a fake initial event to trigger the pusher and potentially start snapshot sync
                fake_event = UpdateEvent(
                    event_schema=self.config.source,  # Use source as event_schema
                    table="initial_trigger",    # Use a special table name for the trigger
                    rows=[],                    # Empty rows
                    index=start_position,       # Use the start position as the index
                    fields=[]                   # Empty fields for a fake event
                )
                events_batch = [fake_event]
                initial_event_sent = True
                logger.debug(f"Sent initial trigger event for task '{self.id}' to ensure pusher is called")
            
            if not events_batch: 
                continue
            
            events_to_push = []
            last_position_in_batch = 0
            for event in events_batch:
                if event.rows:
                    final_rows_for_push = event.rows
                    if self.config.fields_mapping:
                        mapped_rows = [self._process_field_mapping(row) for row in event.rows if self._process_field_mapping(row)]
                        if not mapped_rows:
                            last_position_in_batch = event.index
                            continue
                        final_rows_for_push = mapped_rows
                    
                    event.rows = final_rows_for_push
                events_to_push.append(event)
                
                last_position_in_batch = event.index

            if not events_to_push:
                await self.bus_service.commit_and_handle_split(
                    bus_id=self.bus.id, sync_id=self.id, num_events=len(events_batch),
                    last_consumed_position=last_position_in_batch, fields_mapping=self.config.fields_mapping
                )
                continue

            push_start_time = time.monotonic()
            response_dict = await self.pusher_driver_instance.push(
                events=events_to_push, 
                session_id=self.session_id,
                is_snapshot_end=False
            )
            push_duration = time.monotonic() - push_start_time
            logger.info(f"Sync '{self.id}' push latency: {push_duration:.4f} seconds")

            pushed_rows_count = sum(len(e.rows) for e in events_to_push)
            self._statistics["events_pushed"] += pushed_rows_count
            self._statistics["last_pushed_event_id"] = last_position_in_batch

            await self.bus_service.commit_and_handle_split(
                bus_id=self.bus.id, sync_id=self.id, num_events=len(events_batch),
                last_consumed_position=last_position_in_batch, fields_mapping=self.config.fields_mapping
            )

    def get_dto(self) -> SyncInstanceDTO:
        bus_info = self.bus.get_dto() if self.bus else None
        bus_id = self.bus.id if self.bus else None
        return SyncInstanceDTO(
            id=self.id,
            state=self.state,
            info=self.info,
            statistics=self._statistics,
            bus_info=bus_info,
            bus_id=bus_id
        )

    def _compile_mapper_function(self):
        if not self.config.fields_mapping:
            def passthrough_mapper(self, event_data, logger):
                return event_data
            import types
            logger.info(f"Sync '{self.id}' has no fields_mapping, using passthrough mapper.")
            return types.MethodType(passthrough_mapper, self)

        type_converter_map = {
            "string": "str",
            "integer": "int",
            "number": "float",
        }

        code_lines = [
            "def fast_mapper(self, event_data, logger):",
            "    processed_data = {}",
        ]

        endpoint_names = {m.to.split('.', 1)[0] for m in self.config.fields_mapping if '.' in m.to}
        for name in endpoint_names:
            code_lines.append(f"    processed_data['{name}'] = {{}}")

        for mapping in self.config.fields_mapping:
            if '.' not in mapping.to:
                continue

            endpoint_name, target_field_name = mapping.to.split('.', 1)
            
            target_field_schema = self.pusher_schema.get("properties", {}).get(f"{endpoint_name}.{target_field_name}")
            expected_type = target_field_schema.get("type") if target_field_schema else None

            if expected_type == "object":
                code_lines.append(f"    # Mapping for object: {mapping.to}")
                code_lines.append(f"    object_payload = {{}}")
                for source_str in mapping.source:
                    source_field_name = source_str.split(':')[0].split('.')[-1]
                    code_lines.append(f"    if '{source_field_name}' in event_data:")
                    code_lines.append(f"        object_payload['{source_field_name}'] = event_data.get('{source_field_name}')")
                code_lines.append(f"    processed_data['{endpoint_name}']['{target_field_name}'] = object_payload")
                continue

            if not mapping.source:
                continue
            
            source_field_name = mapping.source[0].split(':')[0].split('.')[-1]
            
            code_lines.append(f"    # Mapping for direct field: {mapping.to}")
            code_lines.append(f"    source_value = event_data.get('{source_field_name}')")
            code_lines.append(f"    if source_value is not None:")
            
            converter = type_converter_map.get(expected_type)
            if converter:
                code_lines.append(f"        try:")
                code_lines.append(f"            processed_data['{endpoint_name}']['{target_field_name}'] = {converter}(source_value)")
                code_lines.append(f"        except (ValueError, TypeError):")
                code_lines.append(f"            logger.warning(f'Failed to convert value {{source_value}} to type {expected_type} for field {mapping.to}')")
            else:
                code_lines.append(f"        processed_data['{endpoint_name}']['{target_field_name}'] = source_value")

        code_lines.append("    return processed_data")
        
        function_code = "\n".join(code_lines)
        
        local_namespace = {}
        try:
            exec(function_code, globals(), local_namespace)
            logger.info(f"Successfully compiled dynamic mapper function for sync '{self.id}'.")
            import types
            return types.MethodType(local_namespace['fast_mapper'], self)
        except Exception as e:
            logger.error(f"Failed to compile dynamic mapper function for sync '{self.id}': {e}", exc_info=True)
            return self._process_field_mapping_original

    def _process_field_mapping(self, event_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        return self._fast_mapper_fn(event_data, logger)

    def _process_field_mapping_original(self, event_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        logger.debug(f"[SyncID: {self.id}] --- Starting field mapping for event_data: {event_data}")
        processed_data = {}
        for mapping in self.config.fields_mapping:
            if '.' not in mapping.to: continue
            endpoint_name, target_field_name = mapping.to.split('.', 1)
            if endpoint_name not in processed_data: processed_data[endpoint_name] = {}
            target_field_schema = self.pusher_schema.get("properties", {}).get(f"{endpoint_name}.{target_field_name}")
            expected_type = target_field_schema.get("type") if target_field_schema else None
            if expected_type == "object":
                object_payload = {}
                for source_str in mapping.source:
                    parts = source_str.split(':')
                    if len(parts) != 2: continue
                    source_field_name_full = parts[0]
                    source_field_name = source_field_name_full.split('.')[-1]
                    source_value = event_data.get(source_field_name)
                    if source_value is not None:
                        object_key = source_field_name_full.split('.')[-1]
                        object_payload[object_key] = source_value
                processed_data[endpoint_name][target_field_name] = object_payload
            else:
                source_value = None
                for source_str in mapping.source:
                    source_field_name = source_str.split(':')[0].split('.')[-1]
                    if source_field_name in event_data:
                        source_value = event_data[source_field_name]
                        break
                if source_value is not None:
                    processed_data[endpoint_name][target_field_name] = source_value
        return processed_data

    async def remap_to_new_bus(self, new_bus: "EventBusInstanceRuntime", needed_position_lost: bool):
        self.bus = new_bus
        if needed_position_lost:
            logger.warning(f"Sync instance '{self.id}' was remapped to a new bus due to a split, and the new bus started from a later position. Starting snapshot sync...")
            # 启动快照同步，因为位置丢失
            if not (self._snapshot_task and not self._snapshot_task.done()):
                self._snapshot_task = asyncio.create_task(self._run_snapshot_sync())
            else:
                logger.info(f"任务 '{self.id}' 已有一个快照正在运行，本次快照请求被忽略。")
        logger.info(f"Sync instance '{self.id}' has been remapped to new bus '{new_bus.id}'.")
