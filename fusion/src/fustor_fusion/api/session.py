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

from ..auth.dependencies import get_view_id_from_api_key

# Note: get_datastore_id_from_api_key was removed. Use get_view_id_from_api_key.

from ..config import receivers_config
from ..core.session_manager import session_manager
from ..view_state_manager import view_state_manager
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
    view_id: str, 
    task_id: str, 
    session_id: str
) -> bool:
    """
    Determine if a new session should be allowed based on configuration and current active sessions.
    """
    view_id = str(view_id)
    sessions = await session_manager.get_view_sessions(view_id)
    active_session_ids = set(sessions.keys())
    
    # NEW: Even if concurrent push is allowed, we don't allow duplicate task_id
    # for different sessions. This prevents confusion in state management.
    for si in sessions.values():
        if si.task_id == task_id:
            logger.warning(f"Task {task_id} already has an active session {si.session_id} on view {view_id}")
            return False

    if allow_concurrent_push:
        return True
    else:
        locked_session_id = await view_state_manager.get_locked_session_id(view_id)
        logger.debug(f"View {view_id} is locked by session: {locked_session_id}")

        if not locked_session_id:
            logger.debug(f"View {view_id} is not locked. Allowing new session.")
            return True

        if locked_session_id not in active_session_ids:
            logger.warning(f"View {view_id} is locked by a stale session {locked_session_id} that is no longer active. Unlocking automatically.")
            await view_state_manager.unlock_for_session(view_id, locked_session_id)
            return True
        else:
            logger.warning(f"View {view_id} is locked by an active session {locked_session_id}. Denying new session {session_id}.")
            return False


@session_router.post("/", summary="Create new sync session")
async def create_session(
    payload: CreateSessionPayload,
    request: Request,
    view_id: str = Depends(get_view_id_from_api_key),
):
    view_id = str(view_id)
    session_config = _get_session_config(view_id)
    allow_concurrent_push = session_config["allow_concurrent_push"]
    session_timeout_seconds = session_config["session_timeout_seconds"]
    
    session_id = str(uuid.uuid4())
    
    should_allow = await _should_allow_new_session(
        allow_concurrent_push, view_id, payload.task_id, session_id
    )
    
    if not should_allow:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="New session cannot be created due to current active sessions"
        )
    
    # Leader/Follower election (First-Come-First-Serve)
    is_leader = await view_state_manager.try_become_leader(view_id, session_id)
    
    if is_leader:
        await view_state_manager.set_authoritative_session(view_id, session_id)

    active_sessions = await session_manager.get_view_sessions(view_id)
    if not active_sessions and allow_concurrent_push:
        logger.info(f"View {view_id} allows concurrent push and this is the first session. Resetting views.")
        try:
            await reset_views(view_id)
            logger.info(f"Successfully reset views for {view_id}.")
        except Exception as e:
            logger.error(f"Exception during views reset for {view_id}: {e}", exc_info=True)

    client_ip = request.client.host
    
    try:
        await on_session_start(view_id)
        logger.info(f"Triggered on_session_start for {session_id} on view {view_id}")
    except Exception as e:
        logger.error(f"Failed to trigger on_session_start during session creation: {e}")

    await session_manager.create_session_entry(
        view_id, 
        session_id, 
        task_id=payload.task_id,
        client_ip=client_ip,
        allow_concurrent_push=allow_concurrent_push,
        session_timeout_seconds=session_timeout_seconds
    )
    
    if not allow_concurrent_push:
        await view_state_manager.lock_for_session(view_id, session_id)
    
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
    view_id: str = Depends(get_view_id_from_api_key),
    session_id: str = Header(..., description="Session ID"),
):
    view_id = str(view_id)
    si = await session_manager.get_session_info(view_id, session_id)
    
    if not si:
        raise HTTPException(
            status_code=419,  # Session Obsoleted
            detail=f"Session {session_id} not found"
        )
    
    is_locked_by_session = await view_state_manager.is_locked_by_session(view_id, session_id)
    if not is_locked_by_session:
        await view_state_manager.lock_for_session(view_id, session_id)
    
    await session_manager.keep_session_alive(view_id, session_id, client_ip=request.client.host)
    
    is_leader = await view_state_manager.try_become_leader(view_id, session_id)
    
    if is_leader:
        await view_state_manager.set_authoritative_session(view_id, session_id)
        
    role = "leader" if is_leader else "follower"

    return {
        "status": "ok", 
        "message": f"Session {session_id} heartbeat updated successfully",
        "role": role,
        "is_leader": is_leader
    }


@session_router.delete("/", tags=["Session Management"], summary="End session")
async def end_session(
    view_id: str = Depends(get_view_id_from_api_key),
    session_id: str = Header(..., description="Session ID"),
):
    view_id = str(view_id)
    success = await session_manager.terminate_session(view_id, session_id)
    
    if not success:
        # Session labels as "not found" but the goal of ending it is achieved
        logger.info(f"Session {session_id} not found or already terminated. Treating as success.")
        return {
            "status": "ok",
            "message": f"Session {session_id} already terminated"
        }
    
    await view_state_manager.unlock_for_session(view_id, session_id)
    await view_state_manager.release_leader(view_id, session_id)
    
    try:
        await on_session_close(view_id)
    except Exception as e:
        logger.warning(f"Failed to notify view providers of session close: {e}")
    
    return {
        "status": "ok",
        "message": f"Session {session_id} terminated successfully",
    }


@session_router.get("/", tags=["Session Management"], summary="List active sessions")
async def list_sessions(
    view_id: str = Depends(get_view_id_from_api_key),
):
    view_id = str(view_id)
    sessions = await session_manager.get_view_sessions(view_id)
    
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
        
        is_leader = await view_state_manager.is_leader(view_id, session_id)
        session_data["role"] = "leader" if is_leader else "follower"
        session_data["can_snapshot"] = is_leader
        session_data["can_audit"] = is_leader
        session_data["can_realtime"] = True
        
        session_list.append(session_data)
    
    return {
        "view_id": view_id,
        "active_sessions": session_list,
        "count": len(session_list)
    }
