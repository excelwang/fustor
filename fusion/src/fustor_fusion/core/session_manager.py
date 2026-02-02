import asyncio
import time
from typing import Dict, Optional
import logging
from fustor_fusion_sdk.interfaces import SessionManagerInterface, SessionInfo # Import the interface and SessionInfo
from ..in_memory_queue import memory_event_queue

logger = logging.getLogger(__name__)

class SessionManager(SessionManagerInterface): # Inherit from the interface
    """
    内存中的session管理器，用于跟踪所有活跃的session
    """
    def __init__(self, default_session_timeout: int = 30):
        # {view_id: {session_id: SessionInfo}}
        self._sessions: Dict[str, Dict[str, SessionInfo]] = {}
        self._lock = asyncio.Lock()
        # Use configured timeout if not provided
        self._default_session_timeout = default_session_timeout
        self._periodic_cleanup_task: Optional[asyncio.Task] = None
    
    async def create_session_entry(self, view_id: str, session_id: str, 
                                 task_id: Optional[str] = None, 
                                 client_ip: Optional[str] = None,
                                 allow_concurrent_push: Optional[bool] = None,
                                 session_timeout_seconds: Optional[int] = None) -> SessionInfo:
        """
        创建新的会话条目并启动其清理任务。
        """
        # Backward compatibility: handle both string and legacy integer
        view_id = str(view_id)
        timeout = session_timeout_seconds or self._default_session_timeout
        
        async with self._lock:
            if view_id not in self._sessions:
                self._sessions[view_id] = {}
            
            now_monotonic = time.monotonic()
            now_epoch = time.time()
            
            session_info = SessionInfo(
                session_id=session_id,
                datastore_id=view_id, # Keep field name for now but use string
                last_activity=now_monotonic,
                created_at=now_epoch,
                task_id=task_id,
                allow_concurrent_push = allow_concurrent_push,
                session_timeout_seconds = session_timeout_seconds,
                client_ip=client_ip
            )
            self._sessions[view_id][session_id] = session_info
            # Create a new cleanup task for this session
            session_info.cleanup_task = asyncio.create_task(
                self._schedule_session_cleanup(view_id, session_id, timeout)
            )
            return session_info

    async def keep_session_alive(self, view_id: str, session_id: str, 
                               client_ip: Optional[str] = None) -> Optional[SessionInfo]:
        """
        更新现有会话的活跃时间并重置其清理任务任务。
        """
        view_id = str(view_id)
        async with self._lock:
            if view_id not in self._sessions or session_id not in self._sessions[view_id]:
                return None # Session not found
            
            session_info = self._sessions[view_id][session_id]
            session_info.last_activity = time.monotonic()
            if client_ip:
                session_info.client_ip = client_ip

            # Cancel the old cleanup task
            if session_info.cleanup_task:
                session_info.cleanup_task.cancel()

            # Create a new cleanup task with the updated timeout
            timeout = session_info.session_timeout_seconds or self._default_session_timeout
            session_info.cleanup_task = asyncio.create_task(
                self._schedule_session_cleanup(datastore_id, session_id, timeout)
            )
            return session_info
    async def _check_if_datastore_live(self, view_id: str) -> bool:
        """
        Check if any view provider for the view requires full reset (Live mode).
        """
        from ..view_manager.manager import get_cached_view_manager
        try:
            manager = await get_cached_view_manager(view_id)
            if not manager or not manager.providers:
                return False
                
            for provider in manager.providers.values():
                # Use property defined in ViewDriver base class
                if getattr(provider, "requires_full_reset_on_session_close", False):
                    return True
        except Exception as e:
            logger.warning(f"Failed to check live status for view {view_id}: {e}")
        return False

    async def _schedule_session_cleanup(self, view_id: str, session_id: str, timeout_seconds: int):
        """
        Schedule cleanup for a single session after timeout.
        """
        view_id = str(view_id)
        from ..datastore_state_manager import datastore_state_manager
        
        try:
            while True:
                # Calculate how much longer we need to wait
                async with self._lock:
                    if (view_id not in self._sessions or 
                        session_id not in self._sessions[view_id]):
                        return # Session already gone
                    
                    session_info = self._sessions[view_id][session_id]
                    elapsed = time.monotonic() - session_info.last_activity
                    remaining = timeout_seconds - elapsed
                    
                if remaining <= 0:
                    break # Expired
                
                # Wait for the remaining period
                await asyncio.sleep(max(0.1, remaining))
            
            # Execute removal
            async with self._lock:
                if (datastore_id in self._sessions and 
                    session_id in self._sessions[datastore_id]):
                    
                    session_info = self._sessions[datastore_id][session_id]
                    # Double check last_activity
                    if time.monotonic() - session_info.last_activity >= timeout_seconds:
                        del self._sessions[view_id][session_id]
                        
                        # Clean up empty datastore entries
                        if not self._sessions[view_id]:
                            del self._sessions[view_id]
                            
                            # NEW: Check if this is a 'live' datastore and clear data
                            from ..view_manager.manager import reset_views
                            
                            is_live = await self._check_if_datastore_live(view_id)
                            
                            if is_live:
                                logger.info(f"View {view_id} is 'live' type. Resetting views as no sessions remain.")
                                await reset_views(view_id)
                            else:
                                from ..in_memory_queue import memory_event_queue
                                await memory_event_queue.clear_datastore_data(view_id)
                            
                            logger.info(f"Cleared all session-associated data for view {view_id}.")
                        
                        # Release any associated lock and leader role
                        from ..datastore_state_manager import datastore_state_manager
                        await datastore_state_manager.unlock_for_session(view_id, session_id)
                        await datastore_state_manager.release_leader(view_id, session_id)
                        
                        logger.info(f"Session {session_id} on view {view_id} expired and removed")
        except asyncio.CancelledError:
            # Task was cancelled, which is fine
            pass
        except Exception as e:
            logger.error(f"Error in session {session_id} cleanup: {e}", exc_info=True)
            # Ensure session gets removed on error to prevent stuck sessions
            async with self._lock:
                if (datastore_id in self._sessions and 
                    session_id in self._sessions[datastore_id]):
                    del self._sessions[datastore_id][session_id]
                    if not self._sessions[datastore_id]:
                        del self._sessions[datastore_id]
                    
                    # Release any associated lock in the datastore state manager
                    await datastore_state_manager.unlock_for_session(datastore_id, session_id)
                    # Release leader role if this session was the leader
                    await datastore_state_manager.release_leader(datastore_id, session_id)

    
    async def get_session_info(self, datastore_id: int, session_id: str) -> Optional[SessionInfo]:
        """
        获取session信息
        """
        async with self._lock:
            if datastore_id not in self._sessions:
                return None
            
            return self._sessions[datastore_id].get(session_id)
    
    async def get_datastore_sessions(self, datastore_id: int) -> Dict[str, SessionInfo]:
        """
        获取特定datastore的所有session信息
        """
        async with self._lock:
            return self._sessions.get(datastore_id, {}).copy()
    
    async def remove_session(self, datastore_id: int, session_id: str) -> bool:
        """
        移除一个session
        """
        async with self._lock:
            if datastore_id not in self._sessions:
                return False
            
            if session_id not in self._sessions[datastore_id]:
                return False
            
            # Cancel the cleanup task if it exists
            session_info = self._sessions[datastore_id][session_id]
            if session_info.cleanup_task:
                session_info.cleanup_task.cancel()
                try:
                    await session_info.cleanup_task
                except asyncio.CancelledError:
                    pass
            
            del self._sessions[datastore_id][session_id]
            
            # 如果datastore没有更多session了，删除该datastore的条目
            if not self._sessions[datastore_id]:
                del self._sessions[datastore_id]
                
                # Check for 'live' type and reset
                from ..view_manager.manager import reset_views
                
                is_live = await self._check_if_datastore_live(datastore_id)
                
                if is_live:
                    await reset_views(datastore_id)
                else:
                    await memory_event_queue.clear_datastore_data(datastore_id)
                    
                logger.info(f"Cleared all data for datastore {datastore_id} as no sessions remain after removal.")
            
            return True

    async def cleanup_expired_sessions(self):
        """
        Clean up all expired sessions across all datastores
        """
        from ..datastore_state_manager import datastore_state_manager
        
        async with self._lock:
            current_time = time.monotonic()
            expired_sessions = []
            
            for datastore_id, datastore_sessions in self._sessions.items():
                for session_id, session_info in datastore_sessions.items():
                    timeout = session_info.session_timeout_seconds or self._default_session_timeout
                    if current_time - session_info.last_activity >= timeout:
                        expired_sessions.append((datastore_id, session_id, session_info.cleanup_task))
            
            # Remove expired sessions
            for datastore_id, session_id, cleanup_task in expired_sessions:
                if (datastore_id in self._sessions and 
                    session_id in self._sessions[datastore_id]):
                    
                    # Cancel the cleanup task
                    if cleanup_task:
                        cleanup_task.cancel()
                        try:
                            await cleanup_task
                        except asyncio.CancelledError:
                            pass
                    
                    # Remove the session
                    del self._sessions[datastore_id][session_id]
                    
                    # If datastore has no more sessions, remove the datastore entry
                    if not self._sessions[datastore_id]:
                        del self._sessions[datastore_id]
                        
                        from ..view_manager.manager import reset_views
                        
                        is_live = await self._check_if_datastore_live(datastore_id)
                        
                        if is_live:
                            await reset_views(datastore_id)
                        else:
                            await memory_event_queue.clear_datastore_data(datastore_id)
                        
                        logger.info(f"Cleared all data for datastore {datastore_id} as no sessions remain after cleanup.")
                    
                    # Release any associated lock in the datastore state manager
                    await datastore_state_manager.unlock_for_session(datastore_id, session_id)
                    # Release leader role if this session was the leader
                    await datastore_state_manager.release_leader(datastore_id, session_id)
                    
                    logger.info(f"Session {session_id} on datastore {datastore_id} expired and removed by periodic cleanup")


    async def terminate_session(self, datastore_id: int, session_id: str) -> bool:
        """
        Explicitly terminate a session (useful for clean shutdowns)
        """
        from ..datastore_state_manager import datastore_state_manager
        
        # First, try to remove the session
        success = await self.remove_session(datastore_id, session_id)
        
        # Then, ensure the lock is also released from the datastore state manager
        if success:
            await datastore_state_manager.unlock_for_session(datastore_id, session_id)
            # Check if this was the last session for the datastore
            async with self._lock:
                if datastore_id in self._sessions and not self._sessions[datastore_id]:
                    del self._sessions[datastore_id] # Clean up the empty dict entry
                    await memory_event_queue.clear_datastore_data(datastore_id)
                    logger.info(f"Cleared all data for datastore {datastore_id} as no sessions remain after termination.")
        
        return success

    async def clear_all_sessions(self, datastore_id: int):
        """
        Terminate all sessions for a specific datastore.
        Used for full reset.
        """
        async with self._lock:
            if datastore_id not in self._sessions:
                return
            
            # Copy keys to avoid modification during iteration
            session_ids = list(self._sessions[datastore_id].keys())
        
        # Terminate each session (outside the lock to avoid deadlock if terminate_session locking)
        for sid in session_ids:
            try:
                await self.terminate_session(datastore_id, sid)
            except Exception as e:
                logger.error(f"Failed to terminate session {sid} during clear_all: {e}")
        
        logger.info(f"Terminated all {len(session_ids)} sessions for datastore {datastore_id} during reset.")

    async def start_periodic_cleanup(self, interval_seconds: int = 60):
        """
        Start a background task that periodically cleans up expired sessions.
        """
        if self._periodic_cleanup_task and not self._periodic_cleanup_task.done():
            self._periodic_cleanup_task.cancel()
        
        self._periodic_cleanup_task = asyncio.create_task(
            self._run_periodic_cleanup(interval_seconds)
        )
        logger.info(f"Started periodic session cleanup, interval: {interval_seconds}s")
    
    async def _run_periodic_cleanup(self, interval_seconds: int):
        """
        Background task that runs periodic cleanup of expired sessions.
        """
        try:
            while True:
                await asyncio.sleep(interval_seconds)
                await self.cleanup_expired_sessions()
        except asyncio.CancelledError:
            logger.info("Periodic session cleanup task was cancelled")
            raise  # Re-raise to properly handle cancellation
    
    async def stop_periodic_cleanup(self):
        """
        Stop the periodic cleanup task.
        """
        if self._periodic_cleanup_task and not self._periodic_cleanup_task.done():
            self._periodic_cleanup_task.cancel()
            try:
                await self._periodic_cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped periodic session cleanup")


# 全局session管理器实例
session_manager = SessionManager()