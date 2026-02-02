# fusion/src/fustor_fusion/api/session.py
"""
Session management API for creating, maintaining, and closing sync sessions.
"""
from fastapi import APIRouter, Depends, status, HTTPException, Header, Query, Request
from pydantic import BaseModel
import logging
from typing import List, Dict, Any, Optional
import time
import uuid

from ..auth.dependencies import get_datastore_id_from_api_key
from ..config import receivers_config
from ..core.session_manager import session_manager
from ..datastore_state_manager import datastore_state_manager
from ..view_manager.manager import reset_views, on_session_start, on_session_close


logger = logging.getLogger(__name__)
session_router = APIRouter(tags=["Session Management"])

# Default configuration values
DEFAULT_SESSION_TIMEOUT = 30
DEFAULT_ALLOW_CONCURRENT_PUSH = False


class CreateSessionPayload(BaseModel):
    """Payload for creating a new session"""
    task_id: str
    client_info: Optional[Dict[str, Any]] = None


def _get_session_config(pipeline_id: str) -> Dict[str, Any]:
    """Get session configuration from receivers config."""
    pipelines = receivers_config.get_active_pipelines()
    if pipeline_id in pipelines:
        cfg = pipelines[pipeline_id]
        return {
            "session_timeout_seconds": cfg.get("session_timeout_seconds", DEFAULT_SESSION_TIMEOUT),
            "allow_concurrent_push": cfg.get("allow_concurrent_push", DEFAULT_ALLOW_CONCURRENT_PUSH),
        }
    return {
        "session_timeout_seconds": DEFAULT_SESSION_TIMEOUT,
        "allow_concurrent_push": DEFAULT_ALLOW_CONCURRENT_PUSH,
    }


async def _should_allow_new_session(
    allow_concurrent_push: bool, 
    datastore_id: int, 
    task_id: str, 
    session_id: str
) -> bool:
    """
    Determine if a new session should be allowed based on configuration and current active sessions.
    """
    sessions = await session_manager.get_datastore_sessions(datastore_id)
    active_session_ids = set(sessions.keys())

    logger.debug(f"Checking if new session {session_id} for task {task_id} should be allowed on datastore {datastore_id}")
    logger.debug(f"Current active sessions: {list(active_session_ids)}")
    logger.debug(f"Allow concurrent push: {allow_concurrent_push}")

    if allow_concurrent_push:
        return True
    else:
        locked_session_id = await datastore_state_manager.get_locked_session_id(datastore_id)
        logger.debug(f"Datastore {datastore_id} is locked by session: {locked_session_id}")

        if not locked_session_id:
            logger.debug(f"Datastore {datastore_id} is not locked. Allowing new session.")
            return True

        if locked_session_id not in active_session_ids:
            logger.warning(f"Datastore {datastore_id} is locked by a stale session {locked_session_id} that is no longer active. Unlocking automatically.")
            await datastore_state_manager.unlock_for_session(datastore_id, locked_session_id)
            return True
        else:
            logger.warning(f"Datastore {datastore_id} is locked by an active session {locked_session_id}. Denying new session {session_id}.")
            return False


@session_router.post("/", summary="Create new sync session")
async def create_session(
    payload: CreateSessionPayload,
    request: Request,
    datastore_id: int = Depends(get_datastore_id_from_api_key),
):
    session_config = _get_session_config(str(datastore_id))
    allow_concurrent_push = session_config["allow_concurrent_push"]
    session_timeout_seconds = session_config["session_timeout_seconds"]
    
    session_id = str(uuid.uuid4())
    
    should_allow = await _should_allow_new_session(
        allow_concurrent_push, datastore_id, payload.task_id, session_id
    )
    
    if not should_allow:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="New session cannot be created due to current active sessions"
        )
    
    # Leader/Follower election (First-Come-First-Serve)
    is_leader = await datastore_state_manager.try_become_leader(datastore_id, session_id)
    
    if is_leader:
        await datastore_state_manager.set_authoritative_session(datastore_id, session_id)

    active_sessions = await session_manager.get_datastore_sessions(datastore_id)
    if not active_sessions and allow_concurrent_push:
        logger.info(f"Datastore {datastore_id} allows concurrent push and this is the first session. Resetting parser.")
        try:
            await reset_views(datastore_id)
            logger.info(f"Successfully reset parser for datastore {datastore_id}.")
        except Exception as e:
            logger.error(f"Exception during parser reset for datastore {datastore_id}: {e}", exc_info=True)

    client_ip = request.client.host
    
    try:
        await on_session_start(datastore_id)
        logger.info(f"Triggered on_session_start for {session_id} on datastore {datastore_id}")
    except Exception as e:
        logger.error(f"Failed to trigger on_session_start during session creation: {e}")

    await session_manager.create_session_entry(
        datastore_id, 
        session_id, 
        task_id=payload.task_id,
        client_ip=client_ip,
        allow_concurrent_push=allow_concurrent_push,
        session_timeout_seconds=session_timeout_seconds
    )
    
    if not allow_concurrent_push:
        await datastore_state_manager.lock_for_session(datastore_id, session_id)
    
    role = "leader" if is_leader else "follower"
    
    return {
        "session_id": session_id,
        "role": role,
        "is_leader": is_leader,
        "suggested_heartbeat_interval_seconds": session_timeout_seconds // 2,
        "session_timeout_seconds": session_timeout_seconds
    }


@session_router.post("/heartbeat", tags=["Session Management"], summary="Session heartbeat keepalive")
async def heartbeat(
    request: Request,
    datastore_id: int = Depends(get_datastore_id_from_api_key),
    session_id: str = Header(..., description="Session ID"),
):
    si = await session_manager.get_session_info(datastore_id, session_id)
    
    if not si:
        raise HTTPException(
            status_code=419,  # Session Obsoleted
            detail=f"Session {session_id} not found"
        )
    
    is_locked_by_session = await datastore_state_manager.is_locked_by_session(datastore_id, session_id)
    if not is_locked_by_session:
        await datastore_state_manager.lock_for_session(datastore_id, session_id)
    
    await session_manager.keep_session_alive(datastore_id, session_id, client_ip=request.client.host)
    
    is_leader = await datastore_state_manager.try_become_leader(datastore_id, session_id)
    
    if is_leader:
        await datastore_state_manager.set_authoritative_session(datastore_id, session_id)
        
    role = "leader" if is_leader else "follower"

    return {
        "status": "ok", 
        "message": f"Session {session_id} heartbeat updated successfully",
        "role": role,
        "is_leader": is_leader
    }


@session_router.delete("/", tags=["Session Management"], summary="End session")
async def end_session(
    datastore_id: int = Depends(get_datastore_id_from_api_key),
    session_id: str = Header(..., description="Session ID"),
):
    success = await session_manager.terminate_session(datastore_id, session_id)
    
    if not success:
        raise HTTPException(
            status_code=419,  # Session Obsoleted
            detail=f"Session {session_id} not found"
        )
    
    await datastore_state_manager.unlock_for_session(datastore_id, session_id)
    await datastore_state_manager.release_leader(datastore_id, session_id)
    
    try:
        await on_session_close(datastore_id)
    except Exception as e:
        logger.warning(f"Failed to notify view providers of session close: {e}")
    
    return {
        "status": "ok",
        "message": f"Session {session_id} terminated successfully",
    }


@session_router.get("/", tags=["Session Management"], summary="List active sessions")
async def list_sessions(
    datastore_id: int = Depends(get_datastore_id_from_api_key),
):
    sessions = await session_manager.get_datastore_sessions(datastore_id)
    
    session_list = []
    for session_id, session_info in sessions.items():
        session_data = {
            "session_id": session_id,
            "task_id": session_info.task_id,
            "agent_id": session_info.task_id,
            "client_ip": session_info.client_ip,
            "last_activity": session_info.last_activity,
            "created_at": session_info.created_at,
            "allow_concurrent_push": session_info.allow_concurrent_push,
            "session_timeout_seconds": session_info.session_timeout_seconds
        }
        
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
