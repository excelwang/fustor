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

from ..auth.dependencies import get_pipe_id_from_auth, get_view_id_from_auth


from ..config.unified import fusion_config
from ..core.session_manager import session_manager
from ..view_state_manager import view_state_manager
from ..view_manager.manager import reset_views, on_session_start, on_session_close
from .. import runtime_objects


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


async def _check_duplicate_task(
    view_id: str, 
    task_id: str
) -> bool:
    """
    Check if task_id is already active in this view.
    Returns True if duplicate exists.
    """
    sessions = await session_manager.get_view_sessions(view_id)
    for si in sessions.values():
        if si.task_id == task_id:
            logger.warning(f"Task {task_id} already has an active session {si.session_id} on view {view_id}")
            return True
    return False


@session_router.post("/", summary="Create new pipesession")
async def create_session(
    payload: CreateSessionPayload,
    request: Request,
    pipe_id: str = Depends(get_pipe_id_from_auth),
):
    """
    Creates a new Pipe Session for an Agent.
    Sessions are strictly bound to a Pipe (ingestion channel).
    """
    session_id = str(uuid.uuid4())
    client_ip = request.client.host
    
    # Use client-requested timeout if provided, otherwise fallback to server config
    session_config = _get_session_config(pipe_id)
    session_timeout_seconds = payload.session_timeout_seconds or session_config["session_timeout_seconds"]

    if runtime_objects.pipe_manager:
        # Authentication already gave us the authoritative pipe_id
        pipe = runtime_objects.pipe_manager.get_pipe(pipe_id)
        if not pipe:
            logger.error(f"Pipe '{pipe_id}' not found. Configuration might have changed after authentication.")
            raise HTTPException(
                status_code=403, 
                detail=f"Resource {pipe_id} is not an authorized pipe."
            )
        
        # Now we proceed with session creation on this pipe.

        # In the new architecture, we delegate to PipeManager to ensure correct routing
        try:
            client_info = payload.client_info or {}
            client_info["client_ip"] = client_ip
            
            session_info = await runtime_objects.pipe_manager._on_session_created(
                session_id=session_id,
                task_id=payload.task_id,
                p_id=pipe_id, 
                client_info=client_info,
                session_timeout_seconds=session_timeout_seconds
            )
            
            return {
                "session_id": session_id,
                "role": session_info.role,
                "is_leader": (session_info.role == "leader"),
                "session_timeout_seconds": session_timeout_seconds,
                "audit_interval_sec": session_info.audit_interval_sec,
                "sentinel_interval_sec": session_info.sentinel_interval_sec,
            }
        except ValueError as e:
            # Handle concurrency/locking conflicts correctly (e.g. duplicate task ID)
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
        except RuntimeError as e:
            # Task already active etc
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
        except Exception as e:
            logger.error(f"PipeManager failed to create session: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    # --- Legacy Fallback (If PipeManager not fully active) ---
    allow_concurrent_push = session_config["allow_concurrent_push"]
    
    # 1. Check for duplicate task ID (Logic moved from _should_allow_new_session)
    if await _check_duplicate_task(view_id, payload.task_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Task {payload.task_id} already active on view {view_id}"
        )

    # 2. Atomic Locking Check (Race Condition Fix)
    if not allow_concurrent_push:
        # Check for stale lock first
        locked_session_id = await view_state_manager.get_locked_session_id(view_id)
        if locked_session_id:
            sessions = await session_manager.get_view_sessions(view_id)
            if locked_session_id not in sessions:
                logger.warning(f"View {view_id} locked by stale session {locked_session_id}. Auto-unlocking.")
                await view_state_manager.unlock_for_session(view_id, locked_session_id)
        
        # Atomic acquire
        acquired = await view_state_manager.acquire_lock_if_free_or_owned(view_id, session_id)
        if not acquired:
             raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"View {view_id} is currently locked by another session"
            )
    
    # 3. Create Session Entry
    try:
        is_leader = await view_state_manager.try_become_leader(view_id, session_id)
        if is_leader:
            await view_state_manager.set_authoritative_session(view_id, session_id)

        try:
            await on_session_start(view_id)
        except Exception as e:
            logger.error(f"Failed to trigger on_session_start: {e}")

        await session_manager.create_session_entry(
            view_id, 
            session_id, 
            task_id=payload.task_id,
            client_ip=client_ip,
            allow_concurrent_push=allow_concurrent_push,
            session_timeout_seconds=session_timeout_seconds,
            source_uri=payload.client_info.get("source_uri") if payload.client_info else None
        )
    except Exception as e:
        # Rollback lock if creation fails
        if not allow_concurrent_push:
            await view_state_manager.unlock_for_session(view_id, session_id)
        raise e
    
    role = "leader" if is_leader else "follower"
    return {
        "session_id": session_id,
        "role": role,
        "is_leader": is_leader,
        "session_timeout_seconds": session_timeout_seconds,
        "audit_interval_sec": session_config.get("audit_interval_sec"),
        "sentinel_interval_sec": session_config.get("sentinel_interval_sec"),
    }


@session_router.post("/{session_id}/heartbeat", tags=["Session Management"], summary="Session heartbeat keepalive")
async def heartbeat(
    session_id: str,
    request: Request,
    pipe_id: str = Depends(get_pipe_id_from_auth),
):
    pipe_id = str(pipe_id)
    # 1. Lookup Pipe
    pipe = runtime_objects.pipe_manager.get_pipe(pipe_id) if runtime_objects.pipe_manager else None
    if not pipe:
        # Fallback to legacy behavior if pipe manager not active
        raise HTTPException(status_code=404, detail="Pipe not found")

    # 2. Update via Pipe Runtime (broadcasts to all views)
    payload = await request.json()
    can_realtime = payload.get("can_realtime", False)
    agent_status = payload.get("agent_status")
    
    success = await pipe.keep_session_alive(
        session_id=session_id,
        can_realtime=can_realtime,
        agent_status=agent_status
    )
    
    if not success:
        raise HTTPException(
            status_code=419,  # Session Obsoleted
            detail=f"Session {session_id} not found or expired"
        )
    
    # 3. Handle leader promotion/commands (Bridge level)
    bridge = runtime_objects.pipe_manager.get_bridge(pipe_id)
    res = await bridge.keep_alive(
        session_id=session_id,
        client_ip=request.client.host,
        can_realtime=can_realtime,
        agent_status=agent_status
    )
    
    return res


@session_router.delete("/{session_id}", tags=["Session Management"], summary="End session")
async def end_session(
    session_id: str,
    identity: str = Depends(get_view_id_from_auth),
):
    """
    End a session. 
    Authorized for Pipe Owners (Pipe Key) and View Admins (View Key).
    """
    # 1. Try interpreting identity as Pipe ID
    if runtime_objects.pipe_manager:
        bridge = runtime_objects.pipe_manager.get_bridge(identity)
        if bridge:
            await bridge.close_session(session_id)
            return {"status": "ok", "message": f"Session {session_id} terminated on pipe {identity}"}
        
        # 2. Case 2: Identity is a View ID (Admin Mode)
        # Find all pipes serving this view
        pipes = runtime_objects.pipe_manager.resolve_pipes_for_view(identity)
        if pipes:
            closed_count = 0
            for p_id in pipes:
                bridge = runtime_objects.pipe_manager.get_bridge(p_id)
                if bridge:
                    # Idempotent close
                    await bridge.close_session(session_id)
                    closed_count += 1
            return {"status": "ok", "message": f"Session {session_id} terminated on {closed_count} pipes via View Auth"}

    # Fallback legacy Local Session Manager (if no bridge found)
    # Treat identity as view_id since legacy SM is view-based
    await session_manager.terminate_session(identity, session_id)
    
    return {
        "status": "ok",
        "message": f"Session {session_id} terminated successfully (Legacy)",
    }
    
@session_router.get("/", tags=["Session Management"], summary="Get current authorized identity for API Key")
async def get_authorized_pipe_info(
    pipe_id: str = Depends(get_pipe_id_from_auth),
):
    """
    Returns the pipe_id and served views associated with the provided API key.
    """
    views = []
    if runtime_objects.pipe_manager:
        pipe = runtime_objects.pipe_manager.get_pipe(pipe_id)
        if pipe:
            views = pipe.view_ids
            
    return {
        "pipe_id": pipe_id,
        "views": views,
        "status": "authorized"
    }
