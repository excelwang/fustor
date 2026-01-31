"""
内存数据存储状态管理器
用于管理数据存储的运行时状态，替代数据库中的 DatastoreStateModel
"""
import asyncio
import logging
from typing import Dict, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class DatastoreState:
    """表示数据存储的内存状态"""
    datastore_id: int
    status: str = 'IDLE'
    locked_by_session_id: Optional[str] = None
    updated_at: datetime = field(default_factory=datetime.now)
    created_at: datetime = field(default_factory=datetime.now)
    authoritative_session_id: Optional[str] = None
    completed_snapshot_session_id: Optional[str] = None
    # Leader/Follower: First-Come-First-Serve
    leader_session_id: Optional[str] = None



class DatastoreStateManager:
    """管理所有数据存储的内存状态"""
    
    def __init__(self):
        self._states: Dict[int, DatastoreState] = {}
        self._lock = asyncio.Lock()
        
    async def get_state(self, datastore_id: int) -> Optional[DatastoreState]:
        """获取指定数据存储的状态"""
        async with self._lock:
            return self._states.get(datastore_id)
    
    async def set_snapshot_complete(self, datastore_id: int, session_id: str):
        """设置数据存储的快照完成状态（绑定到特定会话）"""
        async with self._lock:
            if datastore_id in self._states:
                state = self._states[datastore_id]
                state.completed_snapshot_session_id = session_id
                state.updated_at = datetime.now()
                logger.info(f"Datastore {datastore_id} snapshot marked complete by session {session_id}")
            else:
                # 理论上 session 存在时状态一定已初始化，此处作为兜底
                self._states[datastore_id] = DatastoreState(
                    datastore_id=datastore_id,
                    completed_snapshot_session_id=session_id
                )

    async def is_snapshot_complete(self, datastore_id: int) -> bool:
        """
        检查数据存储的快照是否已完成。
        逻辑：必须存在已完成的 Session ID，且该 ID 必须与当前权威 Session ID 一致。
        """
        async with self._lock:
            state = self._states.get(datastore_id)
            if not state:
                return False
            
            # 如果没有设置权威 Session，则认为未完成
            if not state.authoritative_session_id:
                return False
                
            # 只有当最近一次完成的快照 Session 就是当前的权威 Session 时，才认为数据可用
            is_complete = (state.completed_snapshot_session_id == state.authoritative_session_id)
            if not is_complete and state.completed_snapshot_session_id:
                logger.debug(f"Datastore {datastore_id} has a completed snapshot ({state.completed_snapshot_session_id}), "
                             f"but it is no longer authoritative (current: {state.authoritative_session_id})")
            
            return is_complete
    
    async def set_state(self, datastore_id: int, status: str, locked_by_session_id: Optional[str] = None) -> DatastoreState:
        """设置指定数据存储的状态"""
        async with self._lock:
            if datastore_id in self._states:
                state = self._states[datastore_id]
                state.status = status
                state.locked_by_session_id = locked_by_session_id
                state.updated_at = datetime.now()
            else:
                state = DatastoreState(
                    datastore_id=datastore_id,
                    status=status,
                    locked_by_session_id=locked_by_session_id
                )
                self._states[datastore_id] = state
            
            return state
    
    async def update_status(self, datastore_id: int, status: str) -> DatastoreState:
        """更新指定数据存储的状态"""
        async with self._lock:
            if datastore_id in self._states:
                state = self._states[datastore_id]
                state.status = status
                state.updated_at = datetime.now()
            else:
                state = DatastoreState(
                    datastore_id=datastore_id,
                    status=status
                )
                self._states[datastore_id] = state
            
            return state
            
    async def lock_for_session(self, datastore_id: int, session_id: str) -> bool:
        """为会话锁定数据存储"""
        async with self._lock:
            state = self._states.get(datastore_id)
            if state:
                # 如果当前未被锁定或被同一个会话锁定，允许锁定
                if not state.locked_by_session_id or state.locked_by_session_id == session_id:
                    state.locked_by_session_id = session_id
                    state.status = 'ACTIVE'
                    state.updated_at = datetime.now()
                    return True
                else:
                    # 已被其他会话锁定
                    return False
            else:
                # 创建新状态并锁定
                state = DatastoreState(
                    datastore_id=datastore_id,
                    status='ACTIVE',
                    locked_by_session_id=session_id
                )
                self._states[datastore_id] = state
                return True
    
    async def unlock_for_session(self, datastore_id: int, session_id: str) -> bool:
        """为会话解锁数据存储"""
        async with self._lock:
            state = self._states.get(datastore_id)
            if state:
                if state.locked_by_session_id == session_id:
                    state.locked_by_session_id = None
                    state.status = 'IDLE'
                    state.updated_at = datetime.now()
                    return True
                else:
                    # 不是锁定的会话，不能解锁
                    return False
            else:
                # 不存在的状态，认为已解锁
                return True
    
    async def is_locked_by_session(self, datastore_id: int, session_id: str) -> bool:
        """检查数据存储是否被指定会话锁定"""
        async with self._lock:
            state = self._states.get(datastore_id)
            return bool(state and state.locked_by_session_id == session_id)
    
    async def is_locked(self, datastore_id: int) -> bool:
        """检查数据存储是否被锁定"""
        async with self._lock:
            state = self._states.get(datastore_id)
            return bool(state and state.locked_by_session_id)
    
    async def unlock(self, datastore_id: int) -> bool:
        """完全解锁数据存储"""
        async with self._lock:
            state = self._states.get(datastore_id)
            if state:
                state.locked_by_session_id = None
                state.status = 'IDLE'
                state.updated_at = datetime.now()
                return True
            else:
                return False
    
    async def clear_state(self, datastore_id: int) -> bool:
        """清除指定数据存储的状态"""
        async with self._lock:
            if datastore_id in self._states:
                del self._states[datastore_id]
                return True
            return False
    
    async def get_all_states(self) -> Dict[int, DatastoreState]:
        """获取所有数据存储状态"""
        async with self._lock:
            return self._states.copy()

    async def get_locked_session_id(self, datastore_id: int) -> Optional[str]:
        """获取锁定指定数据存储的会话ID"""
        async with self._lock:
            state = self._states.get(datastore_id)
            if state:
                return state.locked_by_session_id
            return None

    async def set_authoritative_session(self, datastore_id: int, session_id: str):
        """Sets the authoritative session ID for a datastore."""
        async with self._lock:
            state = self._states.get(datastore_id)
            if not state:
                # If state doesn't exist, create it.
                state = DatastoreState(datastore_id=datastore_id)
                self._states[datastore_id] = state
            if state.authoritative_session_id != session_id:
                state.authoritative_session_id = session_id
                logger.info(f"Set authoritative session for datastore {datastore_id} to {session_id}.")

    async def is_authoritative_session(self, datastore_id: int, session_id: str) -> bool:
        """Checks if a session is the authoritative one for a datastore."""
        async with self._lock:
            state = self._states.get(datastore_id)
            if not state or not state.authoritative_session_id:
                # If no authoritative session is set, any session is considered authoritative for now.
                return True
            return state.authoritative_session_id == session_id

    # === Leader/Follower Election (First-Come-First-Serve) ===
    
    async def try_become_leader(self, datastore_id: int, session_id: str) -> bool:
        """
        Try to become the Leader for this datastore.
        First-Come-First-Serve: only succeeds if no current leader.
        
        Returns:
            True if this session became the Leader, False if already has a Leader
        """
        async with self._lock:
            state = self._states.get(datastore_id)
            if not state:
                state = DatastoreState(datastore_id=datastore_id)
                self._states[datastore_id] = state
            
            if state.leader_session_id is None:
                # No leader yet - this session becomes Leader
                state.leader_session_id = session_id
                state.updated_at = datetime.now()
                logger.info(f"Session {session_id} became Leader for datastore {datastore_id}")
                return True
            elif state.leader_session_id == session_id:
                # Already the leader
                return True
            else:
                # Another session is Leader - this session becomes Follower
                logger.info(f"Session {session_id} is Follower for datastore {datastore_id} (Leader: {state.leader_session_id})")
                return False
    
    async def release_leader(self, datastore_id: int, session_id: str) -> bool:
        """
        Release Leader role when session ends or times out.
        Only the current Leader can release.
        """
        async with self._lock:
            state = self._states.get(datastore_id)
            if not state:
                return False
            
            if state.leader_session_id == session_id:
                state.leader_session_id = None
                state.updated_at = datetime.now()
                logger.info(f"Leader {session_id} released for datastore {datastore_id}")
                return True
            return False
    
    async def is_leader(self, datastore_id: int, session_id: str) -> bool:
        """Check if a session is the Leader for this datastore."""
        async with self._lock:
            state = self._states.get(datastore_id)
            if not state:
                return False
            return state.leader_session_id == session_id
    
    async def get_leader_session_id(self, datastore_id: int) -> Optional[str]:
        """Get the current Leader session ID for a datastore."""
        async with self._lock:
            state = self._states.get(datastore_id)
            if state:
                return state.leader_session_id
            return None

    async def clear_state(self, datastore_id: int):
        """
        Purge the runtime state for a given datastore.
        Used for full reset during tests.
        """
        async with self._lock:
            if datastore_id in self._states:
                del self._states[datastore_id]
                logger.info(f"Purged runtime state for datastore {datastore_id}")

# 全局实例
datastore_state_manager = DatastoreStateManager()