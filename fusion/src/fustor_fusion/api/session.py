from fastapi import APIRouter, Depends, status, HTTPException, Header, Query, Request
from pydantic import BaseModel
import logging
from typing import List, Dict, Any, Optional
import time
import uuid

from ..auth.dependencies import get_datastore_id_from_api_key
from ..auth.datastore_cache import datastore_config_cache, DatastoreConfig
from ..core.session_manager import session_manager
from ..datastore_state_manager import datastore_state_manager
from ..parsers.manager import reset_directory_tree, on_session_start

logger = logging.getLogger(__name__)
session_router = APIRouter(tags=["Session Management"])

# --- Pydantic Models for Session Creation ---
class CreateSessionPayload(BaseModel):
    """Payload for creating a new session"""
    task_id: str
    client_info: Optional[Dict[str, Any]] = None

# --- End Session Creation Models ---

async def _should_allow_new_session(datastore_config: DatastoreConfig, datastore_id: int, task_id: str, session_id: str) -> bool:
    """
    Determine if a new session should be allowed based on datastore configuration and current active sessions
    """
    sessions = await session_manager.get_datastore_sessions(datastore_id)
    active_session_ids = set(sessions.keys())

    logger.debug(f"Checking if new session {session_id} for task {task_id} should be allowed on datastore {datastore_id}")
    logger.debug(f"Current active sessions: {list(active_session_ids)}")
    logger.debug(f"Datastore allows concurrent push: {datastore_config.allow_concurrent_push}")

    if datastore_config.allow_concurrent_push:
        # If concurrent pushes are allowed, we only care about sessions for the same task_id
        current_task_sessions = [
            s_info for s_id, s_info in sessions.items()
            if s_info.task_id == task_id
        ]
        logger.debug(f"Current sessions for task {task_id}: {len(current_task_sessions)}")
        return len(current_task_sessions) == 0
    else:
        # If concurrent pushes are not allowed, the datastore acts as a global lock
        locked_session_id = await datastore_state_manager.get_locked_session_id(datastore_id)
        logger.debug(f"Datastore {datastore_id} is locked by session: {locked_session_id}")

        if not locked_session_id:
            # Not locked, so a new session is allowed
            logger.debug(f"Datastore {datastore_id} is not locked. Allowing new session.")
            return True

        # The datastore is locked. Check if the lock is stale.
        if locked_session_id not in active_session_ids:
            # The session holding the lock is no longer in the active session manager.
            # This indicates a stale lock (e.g., from a previous crashed instance).
            logger.warning(f"Datastore {datastore_id} is locked by a stale session {locked_session_id} that is no longer active. Unlocking automatically.")
            await datastore_state_manager.unlock_for_session(datastore_id, locked_session_id)
            return True # Allow the new session to proceed
        else:
            # The datastore is locked by a currently active session.
            logger.warning(f"Datastore {datastore_id} is locked by an active session {locked_session_id}. Denying new session {session_id}.")
            return False

@session_router.post("/", 
    summary="创建新的同步会话", 
    description="为新的同步任务创建会话ID并注册会话")
async def create_session(
    payload: CreateSessionPayload,
    request: Request,
    datastore_id: int = Depends(get_datastore_id_from_api_key),
):
    # Get datastore configuration from cache
    datastore_config = datastore_config_cache.get_datastore_config(datastore_id)
    
    if not datastore_config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Configuration for Datastore {datastore_id} not found"
        )
    
    session_id = str(uuid.uuid4())
    
    should_allow_new_session = await _should_allow_new_session(
        datastore_config, datastore_id, payload.task_id, session_id
    )
    
    if not should_allow_new_session:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="New session cannot be created due to current active sessions"
        )
    
    # Always set the new session as authoritative to enable status tracking
    await datastore_state_manager.set_authoritative_session(datastore_id, session_id)

    active_sessions = await session_manager.get_datastore_sessions(datastore_id)
    if not active_sessions and datastore_config.allow_concurrent_push:
        logger.info(f"Datastore {datastore_id} allows concurrent push and this is the first session. Resetting parser.")
        # Reset the parser state (memory only)
        try:
            await reset_directory_tree(datastore_id)
            logger.info(f"Successfully reset parser for datastore {datastore_id}.")
        except Exception as e:
            logger.error(f"Exception during parser reset for datastore {datastore_id}: {e}", exc_info=True)

    client_ip = request.client.host
    
    # Notify parsers about the new session for lifecycle management (e.g. blind-spot reset)
    try:
        await on_session_start(datastore_id, session_id)
        logger.info(f"Triggered on_session_start for {session_id} on datastore {datastore_id}")
    except Exception as e:
        logger.error(f"Failed to trigger on_session_start during session creation: {e}")

    await session_manager.create_session_entry(
        datastore_id, 
        session_id, 
        task_id=payload.task_id,
        client_ip=client_ip,
        allow_concurrent_push=datastore_config.allow_concurrent_push,
        session_timeout_seconds=datastore_config.session_timeout_seconds
    )
    
    if not datastore_config.allow_concurrent_push:
        await datastore_state_manager.lock_for_session(datastore_id, session_id)
    
    # Leader/Follower election (First-Come-First-Serve)
    is_leader = await datastore_state_manager.try_become_leader(datastore_id, session_id)
    role = "leader" if is_leader else "follower"
    
    return {
        "session_id": session_id,
        "role": role,
        "is_leader": is_leader,
        "suggested_heartbeat_interval_seconds": max(1, datastore_config.session_timeout_seconds // 2),
        "session_timeout_seconds": datastore_config.session_timeout_seconds
    }


@session_router.post("/heartbeat", tags=["Session Management"], summary="会话心跳保活")
async def heartbeat(
    request: Request,
    datastore_id: int = Depends(get_datastore_id_from_api_key),
    session_id: str = Header(..., description="会话ID"),
):
    si = await session_manager.get_session_info(datastore_id, session_id)
    
    if not si:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Session {session_id} not found"
        )
    
    is_locked_by_session = await datastore_state_manager.is_locked_by_session(datastore_id, session_id)
    if not is_locked_by_session:
        await datastore_state_manager.lock_for_session(datastore_id, session_id)
    
    await session_manager.keep_session_alive(datastore_id, session_id, client_ip=request.client.host)
    
    # Check and try to maintain/acquire leader role
    is_leader = await datastore_state_manager.try_become_leader(datastore_id, session_id)
    role = "leader" if is_leader else "follower"

    return {
        "status": "ok", 
        "message": f"Session {session_id} heartbeat updated successfully",
        "role": role,
        "is_leader": is_leader
    }

@session_router.delete("/", tags=["Session Management"], summary="结束会话")
async def end_session(
    datastore_id: int = Depends(get_datastore_id_from_api_key),
    session_id: str = Header(..., description="会话ID"),
):
    success = await session_manager.terminate_session(datastore_id, session_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Session {session_id} not found"
        )
    
    await datastore_state_manager.unlock_for_session(datastore_id, session_id)
    # Release leader role if this session was the leader
    await datastore_state_manager.release_leader(datastore_id, session_id)
    
    return {
        "status": "ok",
        "message": f"Session {session_id} terminated successfully",
    }


@session_router.get("/", tags=["Session Management"], summary="获取活动会话列表")
async def list_sessions(
    datastore_id: int = Depends(get_datastore_id_from_api_key),
):
    sessions = await session_manager.get_datastore_sessions(datastore_id)
    
    session_list = []
    for session_id, session_info in sessions.items():
        session_data = {
            "session_id": session_id,
            "task_id": session_info.task_id,
            "agent_id": session_info.task_id,  # Map task_id to agent_id for client convenience
            "client_ip": session_info.client_ip,
            "last_activity": session_info.last_activity,
            "created_at": session_info.created_at,
            "allow_concurrent_push": session_info.allow_concurrent_push,
            "session_timeout_seconds": session_info.session_timeout_seconds
        }
        
        # Check leader status - use is_leader instead of is_locked_by_session
        is_leader = await datastore_state_manager.is_leader(datastore_id, session_id)
        session_data["role"] = "leader" if is_leader else "follower"
        session_data["can_snapshot"] = is_leader
        session_data["can_audit"] = is_leader
        session_data["can_realtime"] = True
        
        session_list.append(session_data)
    
    return {
        "datastore_id": datastore_id,
        "active_sessions": session_list,
        "count": len(session_list)
    }