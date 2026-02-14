import asyncio
import time
import uuid
from typing import Dict, Optional, List, Any, Set, Tuple
import logging
from dataclasses import dataclass, field
from fustor_fusion_sdk.interfaces import SessionInfo 

logger = logging.getLogger(__name__)

@dataclass
class AgentJob:
    job_id: str
    view_id: str
    path: str
    status: str  # "PENDING", "COMPLETED", "FAILED"
    created_at: float
    completed_at: Optional[float] = None
    expected_sessions: Set[str] = field(default_factory=set)
    completed_sessions: Set[str] = field(default_factory=set)

class SessionManager:
    """
    Robust In-memory Session Manager.
    Uses per-view locks for fine-grained concurrency and a single background
    task for expiration cleanup.
    """
    
    def __init__(self, default_session_timeout: int = 30):
        # {view_id: {session_id: SessionInfo}}
        self._sessions: Dict[str, Dict[str, SessionInfo]] = {}
        self._view_locks: Dict[str, asyncio.Lock] = {}
        self._default_session_timeout = default_session_timeout
        self._cleanup_task: Optional[asyncio.Task] = None
        self._is_removing: Set[str] = set() # Set of session_ids currently being removed
        
        # Agent Job Tracking (Generic async command tracking)
        self._agent_jobs: Dict[str, AgentJob] = {}
        self._path_to_job_id: Dict[Tuple[str, str], str] = {} # (view_id, path) -> job_id

    def _get_view_lock(self, view_id: str) -> asyncio.Lock:
        """获取 per-view 锁（惰性创建）。"""
        return self._view_locks.setdefault(view_id, asyncio.Lock())

    async def create_agent_job(self, view_id: str, path: str, session_ids: List[str]) -> str:
        """Create a new agent job for multiple sessions and return its unique ID."""
        job_id = str(uuid.uuid4())[:8]
        job = AgentJob(
            job_id=job_id,
            view_id=view_id,
            path=path,
            status="RUNNING",  # Initialized as RUNNING since it's immediately queued
            created_at=time.time(),
            expected_sessions=set(session_ids)
        )
        self._agent_jobs[job_id] = job
        self._path_to_job_id[(view_id, path)] = job_id
        return job_id

    def get_agent_jobs(self) -> List[Dict[str, Any]]:
        """List all agent jobs with completion percentage."""
        results = []
        for j in self._agent_jobs.values():
            total = len(j.expected_sessions)
            done = len(j.completed_sessions)
            percentage = round((done / total * 100.0), 2) if total > 0 else 100.0
            
            results.append({
                "job_id": j.job_id,
                "view_id": j.view_id,
                "path": j.path,
                "status": j.status,
                "progress": {
                    "completed_pipes": done,
                    "total_pipes": total,
                    "percentage": percentage
                },
                "created_at": j.created_at,
                "completed_at": j.completed_at
            })
        return results

    async def create_session_entry(self, view_id: str, session_id: str, 
                                 task_id: Optional[str] = None, 
                                 client_ip: Optional[str] = None,
                                 allow_concurrent_push: Optional[bool] = None,
                                 session_timeout_seconds: Optional[int] = None,
                                 source_uri: Optional[str] = None) -> SessionInfo:
        view_id = str(view_id)
        timeout = session_timeout_seconds or self._default_session_timeout
        
        async with self._get_view_lock(view_id):
            if view_id not in self._sessions:
                self._sessions[view_id] = {}
            
            now_monotonic = time.monotonic()
            now_epoch = time.time()
            
            session_info = SessionInfo(
                session_id=session_id,
                view_id=view_id,
                last_activity=now_monotonic,
                created_at=now_epoch,
                task_id=task_id,
                allow_concurrent_push = allow_concurrent_push,
                session_timeout_seconds = timeout,
                client_ip=client_ip,
                source_uri=source_uri
            )
            self._sessions[view_id][session_id] = session_info
            logger.info(f"Created session {session_id} for view {view_id} (timeout: {timeout}s)")
            
            # Ensure cleanup task is running
            if not self._cleanup_task or self._cleanup_task.done():
                await self.start_periodic_cleanup(1) # Check every second for better responsiveness in tests
                
            return session_info

    async def queue_command(self, view_id: str, session_id: str, command: Dict[str, Any]) -> bool:
        """Queue a command for the agent to pick up on next heartbeat."""
        view_id = str(view_id)
        async with self._get_view_lock(view_id):
            if view_id in self._sessions and session_id in self._sessions[view_id]:
                session_info = self._sessions[view_id][session_id]
                if session_info.pending_commands is None:
                    session_info.pending_commands = []
                
                # Track pending finds (formerly scans)
                if command.get("type") == "scan" and command.get("path"):
                    path = command["path"]
                    if session_info.pending_scans is None:
                        session_info.pending_scans = set()
                    session_info.pending_scans.add(path)
                    
                    # Find job_id and add to command for Agent to return
                    job_id = command.get("job_id") or self._path_to_job_id.get((view_id, path))
                    if job_id:
                        command["job_id"] = job_id # Pass job_id to agent
                
                session_info.pending_commands.append(command)
                logger.debug(f"Queued command for session {session_id}: {command['type']}")
                return True
        return False

    async def complete_agent_job(self, view_id: str, session_id: str, path: str, job_id: Optional[str] = None) -> bool:
        """Mark an agent job as complete for a specific session."""
        view_id = str(view_id)
        logger.info(f"complete_agent_job called: view_id={view_id}, session_id={session_id}, path={path}, job_id={job_id}")
        async with self._get_view_lock(view_id):
            # 1. Resolve job_id
            if not job_id:
                job_id = self._path_to_job_id.get((view_id, path))
            
            if job_id and job_id in self._agent_jobs:
                job = self._agent_jobs[job_id]
                
                # Mark this session as completed
                job.completed_sessions.add(session_id)
                logger.info(f"Job {job_id}: Session {session_id} completed. Progress: {len(job.completed_sessions)}/{len(job.expected_sessions)}")
                
                # Check if ALL expected sessions have finished
                # We use subset check in case some sessions disconnected during find
                remaining = job.expected_sessions - job.completed_sessions
                # Filter out sessions that are no longer active
                active_view_sessions = set(self._sessions.get(view_id, {}).keys())
                remaining_active = remaining.intersection(active_view_sessions)
                
                if not remaining_active:
                    job.status = "COMPLETED"
                    job.completed_at = time.time()
                    logger.info(f"Job {job_id} fully COMPLETED (All active expected sessions finished)")
                
                # Cleanup session-specific pending state
                if view_id in self._sessions and session_id in self._sessions[view_id]:
                    si = self._sessions[view_id][session_id]
                    if si.pending_scans:
                        si.pending_scans.discard(path)
                return True
            else:
                logger.warning(f"No job found for job_id={job_id} or path={path}")
        return False

    async def has_pending_job(self, view_id: str, path: str) -> bool:
        """Check if any session has a pending job for the given path."""
        view_id = str(view_id)
        async with self._get_view_lock(view_id):
            if view_id in self._sessions:
                for session_info in self._sessions[view_id].values():
                    if session_info.pending_scans and path in session_info.pending_scans:
                        return True
        return False

    async def keep_session_alive(self, view_id: str, session_id: str, 
                               client_ip: Optional[str] = None,
                               can_realtime: bool = False,
                               agent_status: Optional[Dict[str, Any]] = None) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Updates session activity and returns any pending commands.
        Returns: (success, list_of_commands)
        """
        view_id = str(view_id)
        commands = []
        async with self._get_view_lock(view_id):
            if view_id in self._sessions and session_id in self._sessions[view_id]:
                session_info = self._sessions[view_id][session_id]
                session_info.last_activity = time.monotonic()
                session_info.can_realtime = can_realtime
                if agent_status:
                    session_info.agent_status = agent_status
                if client_ip:
                    session_info.client_ip = client_ip
                
                # Pop commands
                if session_info.pending_commands:
                    commands = session_info.pending_commands[:]
                    session_info.pending_commands.clear()
                    
                return True, commands
        return False, []

    async def get_session_info(self, view_id: str, session_id: str) -> Optional[SessionInfo]:
        view_id = str(view_id)
        # Lock-free read: dict.get is atomic in CPython
        view_sessions = self._sessions.get(view_id)
        if view_sessions:
            return view_sessions.get(session_id)
        return None

    async def get_view_sessions(self, view_id: str) -> Dict[str, SessionInfo]:
        view_id = str(view_id)
        # Lock-free read + copy for snapshot isolation
        view_sessions = self._sessions.get(view_id)
        if view_sessions:
            return view_sessions.copy()
        return {}

    async def get_all_active_sessions(self) -> Dict[str, Dict[str, SessionInfo]]:
        # Snapshot copy of top-level dict; each view dict is also copied
        return {vid: s.copy() for vid, s in self._sessions.items()}

    async def remove_session(self, view_id: str, session_id: str) -> bool:
        """Public API to remove a session."""
        return await self._terminate_session_internal(str(view_id), session_id, "manual")

    async def terminate_session(self, view_id: str, session_id: str) -> bool:
        """Alias for remove_session."""
        return await self.remove_session(view_id, session_id)

    async def clear_all_sessions(self, view_id: str) -> bool:
        view_id = str(view_id)
        # Collect session IDs under lock, then terminate each outside
        async with self._get_view_lock(view_id):
            if view_id not in self._sessions:
                return False
            sids = list(self._sessions[view_id].keys())
        
        results = []
        for sid in sids:
            results.append(await self._terminate_session_internal(view_id, sid, "clear_all"))
        return any(results)

    async def _terminate_session_internal(self, view_id: str, session_id: str, reason: str) -> bool:
        """
        Consolidated session termination logic.
        Handles state removal, role release, and promotion.
        """
        async with self._get_view_lock(view_id):
            if session_id in self._is_removing:
                return False # Already being removed
            
            if view_id not in self._sessions or session_id not in self._sessions[view_id]:
                return False
            
            # Mark as being removed to prevent races
            self._is_removing.add(session_id)
            
            # Extract info needed for external calls
            si = self._sessions[view_id][session_id]
            
            # Remove from local state immediately
            del self._sessions[view_id][session_id]
            view_empty = not self._sessions[view_id]
            if view_empty:
                del self._sessions[view_id]
        
        try:
            logger.info(f"Terminating session {session_id} on view {view_id} (Reason: {reason})")
            
            # External Managers (VSM, VM)
            from ..view_state_manager import view_state_manager
            from ..view_manager.manager import reset_views
            
            # 1. Release Lock and Role
            is_leader = await view_state_manager.is_leader(view_id, session_id)
            await view_state_manager.unlock_for_session(view_id, session_id)
            await view_state_manager.release_leader(view_id, session_id)
            
            # 2. Handle View Reset if last session
            if view_empty:
                is_live = await self._check_if_view_live(view_id)
                if is_live:
                    logger.info(f"View {view_id} became empty. Triggering reset.")
                    await reset_views(view_id)
            
            # 3. Handle Promotion if leader left
            if is_leader:
                async with self._get_view_lock(view_id):
                    remaining_sessions = list(self._sessions.get(view_id, {}).keys())
                
                for rsid in remaining_sessions:
                    if await view_state_manager.try_become_leader(view_id, rsid):
                        await view_state_manager.set_authoritative_session(view_id, rsid)
                        logger.info(f"Promoted {rsid} to Leader for view {view_id}")
                        break
            
            return True
        finally:
            self._is_removing.discard(session_id)

    async def _check_if_view_live(self, view_id: str) -> bool:
        from ..view_manager.manager import get_cached_view_manager
        try:
            manager = await get_cached_view_manager(view_id)
            if manager:
                for driver_instance in manager.driver_instances.values():
                    if getattr(driver_instance, "requires_full_reset_on_session_close", False):
                        return True
        except Exception as e:
            logger.warning(f"Failed to check if view {view_id} is live: {e}")
        return False

    async def start_periodic_cleanup(self, interval_seconds: int = 1):
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
        self._cleanup_task = asyncio.create_task(self._run_cleanup_loop(interval_seconds))

    async def _run_cleanup_loop(self, interval: int):
        logger.info(f"Session cleanup loop started (Interval: {interval}s)")
        try:
            while True:
                await asyncio.sleep(interval)
                await self.cleanup_expired_sessions()
                await self.cleanup_old_jobs()
        except asyncio.CancelledError:
            logger.info("Session cleanup loop stopped")

    async def cleanup_old_jobs(self, ttl_seconds: int = 3600):
        """Remove completed or very old jobs from history."""
        now = time.time()
        to_remove = []
        for jid, job in self._agent_jobs.items():
            # Remove completed jobs after TTL
            if job.status in ("COMPLETED", "FAILED") and job.completed_at:
                if now - job.completed_at > ttl_seconds:
                    to_remove.append(jid)
            # Failsafe for stuck jobs (e.g. 24h)
            elif now - job.created_at > 86400:
                to_remove.append(jid)
        
        for jid in to_remove:
            job = self._agent_jobs.pop(jid, None)
            if job:
                self._path_to_job_id.pop((job.view_id, job.path), None)
                logger.debug(f"Cleaned up old job {jid}")

    async def cleanup_expired_sessions(self):
        now = time.monotonic()
        expired = []
        
        # Scan all views for expired sessions (lock-free reads for checking)
        for vid, sessions in list(self._sessions.items()):
            for sid, si in list(sessions.items()):
                if sid in self._is_removing:
                    continue
                
                elapsed = now - si.last_activity
                if elapsed >= si.session_timeout_seconds:
                    expired.append((vid, sid))
        
        for vid, sid in expired:
            await self._terminate_session_internal(vid, sid, "expired")

    async def stop_periodic_cleanup(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

# Global Instance
session_manager = SessionManager()