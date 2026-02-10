# fusion/src/fustor_fusion/api/session.py
"""
Session management API for creating, maintaining, and closing pipesessions.
"""
from fastapi import APIRouter, Depends, status, HTTPException, Header, Query, Request
from pydantic import BaseModel
import logging
from typing import List, Dict, Any, Optional
import time
import uuid

from ..auth.dependencies import get_view_id_from_api_key


from ..config.unified import fusion_config
from ..core.session_manager import session_manager
from ..view_state_manager import view_state_manager
from ..view_manager.manager import reset_views, on_session_start, on_session_close


logger = logging.getLogger(__name__)
session_router = APIRouter(tags=["Session Management"])

# Default configuration values
DEFAULT_SESSION_TIMEOUT = 30
DEFAULT_ALLOW_CONCURRENT_PUSH = False
DEFAULT_AUDIT_INTERVAL = 600.0
DEFAULT_SENTINEL_INTERVAL = 120.0


class CreateSessionPayload(BaseModel):
    """Payload for creating a new session"""
    task_id: str
    client_info: Optional[Dict[str, Any]] = None
    session_timeout_seconds: Optional[int] = None


def _get_session_config(pipe_id: str) -> Dict[str, Any]:
    """Get session configuration from fusion config (pipes configuration)."""
    # In unified config, pipe config holds session parameters
    pipe = fusion_config.get_pipe(pipe_id)
    
    if pipe:
        return {
            "session_timeout_seconds": pipe.session_timeout_seconds,
            "allow_concurrent_push": pipe.allow_concurrent_push,
            "audit_interval_sec": pipe.audit_interval_sec,
            "sentinel_interval_sec": pipe.sentinel_interval_sec,
        }
        
    return {
        "session_timeout_seconds": DEFAULT_SESSION_TIMEOUT,
        "allow_concurrent_push": DEFAULT_ALLOW_CONCURRENT_PUSH,
        "audit_interval_sec": DEFAULT_AUDIT_INTERVAL,
        "sentinel_interval_sec": DEFAULT_SENTINEL_INTERVAL,
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


@session_router.post("/", summary="Create new pipesession")
async def create_session(
    payload: CreateSessionPayload,
    request: Request,
    view_id: str = Depends(get_view_id_from_api_key),
):
    view_id = str(view_id)
    session_config = _get_session_config(view_id)
    allow_concurrent_push = session_config["allow_concurrent_push"]
    
    # Use client-requested timeout if provided, otherwise fallback to server config
    session_timeout_seconds = payload.session_timeout_seconds or session_config["session_timeout_seconds"]
    logger.debug(f"Session timeout determined: {session_timeout_seconds}")

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

    source_uri = payload.client_info.get("source_uri") if payload.client_info else None
    
    await session_manager.create_session_entry(
        view_id, 
        session_id, 
        task_id=payload.task_id,
        client_ip=client_ip,
        allow_concurrent_push=allow_concurrent_push,
        session_timeout_seconds=session_timeout_seconds,
        source_uri=source_uri
    )
    
    if not allow_concurrent_push:
        await view_state_manager.lock_for_session(view_id, session_id)
    
    role = "leader" if is_leader else "follower"
    
    return {
        "session_id": session_id,
        "role": role,
        "is_leader": is_leader,
        "suggested_heartbeat_interval_seconds": session_timeout_seconds // 2,
        "session_timeout_seconds": session_timeout_seconds,
        "audit_interval_sec": session_config.get("audit_interval_sec"),
        "sentinel_interval_sec": session_config.get("sentinel_interval_sec"),
    }


@session_router.post("/{session_id}/heartbeat", tags=["Session Management"], summary="Session heartbeat keepalive")
async def heartbeat(
    session_id: str,
    request: Request,
    view_id: str = Depends(get_view_id_from_api_key),
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
    
    payload = await request.json()
    can_realtime = payload.get("can_realtime", False)
    
    success, commands = await session_manager.keep_session_alive(view_id, session_id, client_ip=request.client.host, can_realtime=can_realtime)
    if not success:  # Should match session existence check above, but for safety
        logger.warning(f"Session {session_id} not found during heartbeat update (race condition?)")
    
    is_leader = await view_state_manager.try_become_leader(view_id, session_id)
    
    if is_leader:
        await view_state_manager.set_authoritative_session(view_id, session_id)
        
    role = "leader" if is_leader else "follower"

    return {
        "status": "ok", 
        "message": f"Session {session_id} heartbeat updated successfully",
        "role": role,
        "is_leader": is_leader,
        "commands": commands
    }


@session_router.delete("/{session_id}", tags=["Session Management"], summary="End session")
async def end_session(
    session_id: str,
    view_id: str = Depends(get_view_id_from_api_key),
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
        logger.warning(f"Failed to notify view driver instances of session close: {e}")
    
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
            "agent_id": session_info.task_id.split(":")[0] if session_info.task_id and ":" in session_info.task_id else session_info.task_id,
            
            "client_ip": session_info.client_ip,
            "source_uri": session_info.source_uri,
            "last_activity": session_info.last_activity,
            "created_at": session_info.created_at,
            "allow_concurrent_push": session_info.allow_concurrent_push,
            "session_timeout_seconds": session_info.session_timeout_seconds
        }
        
        is_leader = await view_state_manager.is_leader(view_id, session_id)
        session_data["role"] = "leader" if is_leader else "follower"
        session_data["can_snapshot"] = is_leader
        session_data["can_audit"] = is_leader
        session_data["can_realtime"] = session_info.can_realtime
        session_data["can_send"] = True
        
        session_list.append(session_data)
    
    return {
        "view_id": view_id,
        "active_sessions": session_list,
        "count": len(session_list)
    }
