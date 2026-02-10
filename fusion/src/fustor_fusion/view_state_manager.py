"""
内存视图状态管理器
用于管理视图的运行时状态，替代数据库中的 ViewStateModel

Performance: 使用 per-view 锁替代全局锁，只读操作无锁。
"""
import asyncio
import logging
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ViewState:
    """表示视图的内存状态"""
    view_id: str
    status: str = 'IDLE'
    locked_by_session_id: Optional[str] = None
    updated_at: datetime = field(default_factory=datetime.now)
    created_at: datetime = field(default_factory=datetime.now)
    authoritative_session_id: Optional[str] = None
    completed_snapshot_session_id: Optional[str] = None
    # Leader/Follower: First-Come-First-Serve
    leader_session_id: Optional[str] = None




class ViewStateManager:
    """管理所有视图的内存状态
    
    并发策略:
    - 写操作使用 per-view 锁（不同 view 的写操作互不阻塞）
    - 只读操作无锁（CPython GIL 保证 dict.get 原子性，asyncio 单线程无竞争）
    """
    
    def __init__(self):
        self._states: Dict[str, ViewState] = {}
        self._view_locks: Dict[str, asyncio.Lock] = {}
    
    def _get_view_lock(self, view_id: str) -> asyncio.Lock:
        """获取 per-view 锁（惰性创建）。"""
        return self._view_locks.setdefault(view_id, asyncio.Lock())

    def _ensure_state(self, view_id_str: str) -> ViewState:
        """确保 ViewState 存在（内部使用，调用者应持有 per-view 锁）。"""
        state = self._states.get(view_id_str)
        if state is None:
            state = ViewState(view_id=view_id_str)
            self._states[view_id_str] = state
        return state
        
    # --- 只读操作（无锁） ---
    
    async def get_state(self, view_id: str) -> Optional[ViewState]:
        """获取指定视图的状态"""
        return self._states.get(str(view_id))
    
    async def is_snapshot_complete(self, view_id: str) -> bool:
        """
        检查视图的快照是否已完成。
        逻辑：必须存在已完成的 Session ID，且该 ID 必须与当前权威 Session ID 一致。
        """
        view_id_str = str(view_id)
        state = self._states.get(view_id_str)
        if not state:
            return False
        
        if not state.authoritative_session_id:
            return False
            
        is_complete = (state.completed_snapshot_session_id == state.authoritative_session_id)
        if not is_complete and state.completed_snapshot_session_id:
            logger.debug(f"View {view_id_str} has a completed snapshot ({state.completed_snapshot_session_id}), "
                         f"but it is no longer authoritative (current: {state.authoritative_session_id})")
        
        return is_complete

    async def is_locked_by_session(self, view_id: str, session_id: str) -> bool:
        """检查视图是否被指定会话锁定"""
        state = self._states.get(str(view_id))
        return bool(state and state.locked_by_session_id == session_id)
    
    async def is_locked(self, view_id: str) -> bool:
        """检查视图是否被锁定"""
        state = self._states.get(str(view_id))
        return bool(state and state.locked_by_session_id)

    async def get_all_states(self) -> Dict[str, ViewState]:
        """获取所有视图状态"""
        return self._states.copy()

    async def get_locked_session_id(self, view_id: str) -> Optional[str]:
        """获取锁定指定视图的会话ID"""
        state = self._states.get(str(view_id))
        if state:
            return state.locked_by_session_id
        return None

    async def is_authoritative_session(self, view_id: str, session_id: str) -> bool:
        """Checks if a session is the authoritative one for a view."""
        state = self._states.get(str(view_id))
        if not state or not state.authoritative_session_id:
            return True
        return state.authoritative_session_id == session_id

    async def is_leader(self, view_id: str, session_id: str) -> bool:
        """Check if a session is the Leader for this view."""
        state = self._states.get(str(view_id))
        if not state:
            return False
        return state.leader_session_id == session_id
    
    async def get_leader_session_id(self, view_id: str) -> Optional[str]:
        """Get the current Leader session ID for a view."""
        state = self._states.get(str(view_id))
        if state:
            return state.leader_session_id
        return None

    async def get_leader(self, view_id: str) -> Optional[str]:
        """Alias for get_leader_session_id to match FusionPipe expectations."""
        return await self.get_leader_session_id(view_id)

    # --- 写操作（per-view 锁） ---

    async def set_snapshot_complete(self, view_id: str, session_id: str):
        """设置视图的快照完成状态（绑定到特定会话）"""
        view_id_str = str(view_id)
        async with self._get_view_lock(view_id_str):
            state = self._ensure_state(view_id_str)
            state.completed_snapshot_session_id = session_id
            state.updated_at = datetime.now()
            logger.info(f"View {view_id_str} snapshot marked complete by session {session_id}")

    async def set_state(self, view_id: str, status: str, locked_by_session_id: Optional[str] = None) -> ViewState:
        """设置指定视图的状态"""
        view_id_str = str(view_id)
        async with self._get_view_lock(view_id_str):
            state = self._ensure_state(view_id_str)
            state.status = status
            state.locked_by_session_id = locked_by_session_id
            state.updated_at = datetime.now()
            return state
    
    async def update_status(self, view_id: str, status: str) -> ViewState:
        """更新指定视图的状态"""
        view_id_str = str(view_id)
        async with self._get_view_lock(view_id_str):
            state = self._ensure_state(view_id_str)
            state.status = status
            state.updated_at = datetime.now()
            return state
            
    async def lock_for_session(self, view_id: str, session_id: str) -> bool:
        """为会话锁定视图"""
        view_id_str = str(view_id)
        async with self._get_view_lock(view_id_str):
            state = self._ensure_state(view_id_str)
            if not state.locked_by_session_id or state.locked_by_session_id == session_id:
                state.locked_by_session_id = session_id
                state.status = 'ACTIVE'
                state.updated_at = datetime.now()
                return True
            else:
                return False
    
    async def unlock_for_session(self, view_id: str, session_id: str) -> bool:
        """为会话解锁视图"""
        view_id_str = str(view_id)
        async with self._get_view_lock(view_id_str):
            state = self._states.get(view_id_str)
            if state:
                if state.locked_by_session_id == session_id:
                    state.locked_by_session_id = None
                    state.status = 'IDLE'
                    state.updated_at = datetime.now()
                    return True
                else:
                    return False
            else:
                return True
    
    async def unlock(self, view_id: str) -> bool:
        """完全解锁视图"""
        view_id_str = str(view_id)
        async with self._get_view_lock(view_id_str):
            state = self._states.get(view_id_str)
            if state:
                state.locked_by_session_id = None
                state.status = 'IDLE'
                state.updated_at = datetime.now()
                return True
            else:
                return False

    async def set_authoritative_session(self, view_id: str, session_id: str):
        """Sets the authoritative session ID for a view."""
        view_id_str = str(view_id)
        async with self._get_view_lock(view_id_str):
            state = self._ensure_state(view_id_str)
            if state.authoritative_session_id != session_id:
                state.authoritative_session_id = session_id
                logger.info(f"Set authoritative session for view {view_id_str} to {session_id}.")

    async def try_become_leader(self, view_id: str, session_id: str) -> bool:
        """
        Try to become the Leader for this view.
        First-Come-First-Serve: only succeeds if no current leader.
        """
        view_id_str = str(view_id)
        # Fast path: already leader (lock-free check)
        state = self._states.get(view_id_str)
        if state and state.leader_session_id == session_id:
            return True
        
        async with self._get_view_lock(view_id_str):
            state = self._ensure_state(view_id_str)
            
            if state.leader_session_id is None:
                state.leader_session_id = session_id
                state.updated_at = datetime.now()
                logger.info(f"Session {session_id} became Leader for view {view_id_str}")
                return True
            elif state.leader_session_id == session_id:
                return True
            else:
                logger.info(f"Session {session_id} is Follower for view {view_id_str} (Leader: {state.leader_session_id})")
                return False
    
    async def release_leader(self, view_id: str, session_id: str) -> bool:
        """Release Leader role."""
        view_id_str = str(view_id)
        async with self._get_view_lock(view_id_str):
            state = self._states.get(view_id_str)
            if not state:
                return False
            
            if state.leader_session_id == session_id:
                state.leader_session_id = None
                state.updated_at = datetime.now()
                logger.info(f"Leader {session_id} released for view {view_id_str}")
                return True
            return False

    async def clear_state(self, view_id: str):
        """
        Purge the runtime state for a given view.
        Used for full reset during tests.
        """
        view_id_str = str(view_id)
        async with self._get_view_lock(view_id_str):
            if view_id_str in self._states:
                del self._states[view_id_str]
                logger.info(f"Purged runtime state for view {view_id_str}")

# Global instance
view_state_manager = ViewStateManager()

# Legacy aliases
ViewStateManager = ViewStateManager
view_state_manager = view_state_manager