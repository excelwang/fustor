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
    is_snapshot_complete: bool = False


class DatastoreStateManager:
    """管理所有数据存储的内存状态"""
    
    def __init__(self):
        self._states: Dict[int, DatastoreState] = {}
        self._lock = asyncio.Lock()
        
    async def get_state(self, datastore_id: int) -> Optional[DatastoreState]:
        """获取指定数据存储的状态"""
        async with self._lock:
            return self._states.get(datastore_id)
    
    async def set_snapshot_complete(self, datastore_id: int, complete: bool = True):
        """设置数据存储的快照完成状态"""
        async with self._lock:
            if datastore_id in self._states:
                self._states[datastore_id].is_snapshot_complete = complete
                self._states[datastore_id].updated_at = datetime.now()
            else:
                self._states[datastore_id] = DatastoreState(
                    datastore_id=datastore_id,
                    is_snapshot_complete=complete
                )
            logger.info(f"Datastore {datastore_id} snapshot complete status set to {complete}")

    async def is_snapshot_complete(self, datastore_id: int) -> bool:
        """检查数据存储的快照是否已完成"""
        async with self._lock:
            state = self._states.get(datastore_id)
            return bool(state and state.is_snapshot_complete)
    
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

# 全局实例
datastore_state_manager = DatastoreStateManager()