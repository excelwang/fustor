# fusion/src/fustor_fusion/api/management.py
"""
Management API for centralized control of Fusion and Agent services.

Provides:
- Dashboard: Real-time overview of all views, pipes, sessions, and agents
- Command dispatch: Send commands to agents via heartbeat channel
- Configuration: View current Fusion configuration
- Reload: Trigger Fusion hot-reload
"""
import os
import signal
import time
import logging
from typing import Dict, Any, Optional
from fastapi import APIRouter, HTTPException, status, Header, Depends
from pydantic import BaseModel

from ..config.unified import fusion_config
from ..core.session_manager import session_manager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Management auth dependency
# ---------------------------------------------------------------------------

async def require_management_key(
    x_management_key: Optional[str] = Header(None, alias="X-Management-Key"),
) -> None:
    """
    Validate management API key.
    
    If fusion.management_api_key is configured, all management endpoints
    require a matching X-Management-Key header.
    If not configured, management API is open (backward compatible).
    """
    configured_key = fusion_config.fusion.management_api_key
    if not configured_key:
        return  # No key configured → open access
    if not x_management_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-Management-Key header is required",
        )
    
    import hmac
    if not hmac.compare_digest(x_management_key, configured_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid management key",
        )


router = APIRouter(
    prefix="/management",
    tags=["Management"],
    dependencies=[Depends(require_management_key)],
)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class AgentCommandRequest(BaseModel):
    """Payload for dispatching a command to an agent."""
    type: str  # "reload_config", "stop_pipe", "report_status"
    pipe_id: Optional[str] = None


class AgentConfigUpdateRequest(BaseModel):
    """Payload for pushing configuration to an agent."""
    config_yaml: str  # Raw YAML content to write to agent's config file
    filename: str = "default.yaml"  # Target config file name


class AgentUpgradeRequest(BaseModel):
    """Payload for triggering an agent self-upgrade."""
    version: str
    index_url: Optional[str] = None
    upgrade_timeout_sec: int = 60


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@router.get("/dashboard")
async def dashboard():
    """
    Return a full overview of all views, pipes, sessions, and connected agents.
    
    This is the primary entry-point for the management UI.
    """
    from .. import runtime_objects

    # --- Views ---
    views_info: Dict[str, Any] = {}
    if runtime_objects.view_managers:
        for v_group_id, vm in runtime_objects.view_managers.items():
            drivers = {}
            for drv_name, drv_inst in vm.driver_instances.items():
                drivers[drv_name] = {
                    "schema_name": getattr(drv_inst, "schema_name", None),
                    "target_schema": getattr(drv_inst, "target_schema", None),
                }
            views_info[v_group_id] = {"drivers": drivers}

    # --- Pipes ---
    pipes_info: Dict[str, Any] = {}
    if runtime_objects.pipe_manager:
        for pipe_id, pipe in runtime_objects.pipe_manager.get_pipes().items():
            dto = await pipe.get_dto()
            pipes_info[pipe_id] = dto

    # --- Sessions (grouped by view) ---
    all_sessions = await session_manager.get_all_active_sessions()
    sessions_info: Dict[str, Any] = {}
    agents_info: Dict[str, Any] = {}

    for view_id, sess_map in all_sessions.items():
        view_sessions = []
        for sid, si in sess_map.items():
            agent_id = None
            # Issue 4 fix: Prioritize agent_status
            if getattr(si, "agent_status", None) and si.agent_status.get("agent_id"):
                agent_id = si.agent_status["agent_id"]
            elif si.task_id and ":" in si.task_id:
                agent_id = si.task_id.split(":")[0]
            elif si.task_id:
                agent_id = si.task_id

            sess_dto = {
                "session_id": sid,
                "agent_id": agent_id,
                "task_id": si.task_id,
                "client_ip": si.client_ip,
                "source_uri": si.source_uri,
                "can_realtime": si.can_realtime,
                "created_at": si.created_at,
                "last_activity": si.last_activity,
                "age_seconds": round(time.monotonic() - si.created_at, 1),
                "idle_seconds": round(time.monotonic() - si.last_activity, 1),
                "agent_status": si.agent_status,
            }
            view_sessions.append(sess_dto)

            # Build agent index
            if agent_id:
                if agent_id not in agents_info:
                    agents_info[agent_id] = {
                        "agent_id": agent_id,
                        "client_ip": si.client_ip,
                        "sessions": [],
                        "pipe_statuses": {}, # Issue 5 fix: Store status per pipe
                    }
                agents_info[agent_id]["sessions"].append({
                    "session_id": sid,
                    "view_id": view_id,
                    "task_id": si.task_id,
                })
                
                # Update pipe status if available
                if si.agent_status:
                    p_id = si.agent_status.get("pipe_id") or view_id
                    agents_info[agent_id]["pipe_statuses"][p_id] = si.agent_status
                    # Compatibility: keep a top-level status for version info (use latest)
                    agents_info[agent_id]["status"] = si.agent_status

        sessions_info[view_id] = view_sessions

    return {
        "timestamp": time.time(),
        "views": views_info,
        "pipes": pipes_info,
        "sessions": sessions_info,
        "agents": agents_info,
    }


# ---------------------------------------------------------------------------
# Views listing (legacy, kept for backward compat)
# ---------------------------------------------------------------------------

@router.get("/views")
async def list_views():
    """List all running views."""
    from .. import runtime_objects

    result = {}
    if runtime_objects.view_managers:
        for v_group_id, vm in runtime_objects.view_managers.items():
            result[v_group_id] = list(vm.driver_instances.keys())

    return {"running_views": result}


# ---------------------------------------------------------------------------
# Agent command dispatch
# ---------------------------------------------------------------------------

@router.post("/agents/{agent_id}/command")
async def dispatch_agent_command(agent_id: str, payload: AgentCommandRequest):
    """
    Queue a command for a specific agent.

    The command will be delivered on the agent's next heartbeat.
    The agent is identified by its agent_id (the prefix of task_id before ':').
    """
    all_sessions = await session_manager.get_all_active_sessions()

    # Find all sessions belonging to this agent
    matched = []
    for view_id, sess_map in all_sessions.items():
        for sid, si in sess_map.items():
            if si.task_id:
                parts = si.task_id.split(":")
                if parts[0] == agent_id:
                    matched.append((view_id, sid, si))

    if not matched:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active sessions found for agent '{agent_id}'",
        )

    command_dict: Dict[str, Any] = {"type": payload.type}
    if payload.pipe_id:
        command_dict["pipe_id"] = payload.pipe_id

    queued_count = 0
    for view_id, session_id, _ in matched:
        success = await session_manager.queue_command(view_id, session_id, command_dict)
        if success:
            queued_count += 1

    return {
        "status": "ok",
        "agent_id": agent_id,
        "command": payload.type,
        "sessions_queued": queued_count,
        "total_sessions": len(matched),
    }


# ---------------------------------------------------------------------------
# Agent config push
# ---------------------------------------------------------------------------

@router.post("/agents/{agent_id}/config")
async def push_agent_config(agent_id: str, payload: AgentConfigUpdateRequest):
    """
    Push a new configuration to an agent.

    The config YAML is delivered via heartbeat command channel.
    The agent writes it to its local config directory and triggers a hot-reload.
    """
    all_sessions = await session_manager.get_all_active_sessions()

    matched = []
    for view_id, sess_map in all_sessions.items():
        for sid, si in sess_map.items():
            if si.task_id:
                parts = si.task_id.split(":")
                if parts[0] == agent_id:
                    matched.append((view_id, sid, si))

    if not matched:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active sessions found for agent '{agent_id}'",
        )

    command_dict = {
        "type": "update_config",
        "config_yaml": payload.config_yaml,
        "filename": payload.filename,
    }

    queued_count = 0
    for view_id, session_id, _ in matched:
        success = await session_manager.queue_command(view_id, session_id, command_dict)
        if success:
            queued_count += 1
            break  # Only need to deliver to one session per agent

    return {
        "status": "ok",
        "agent_id": agent_id,
        "filename": payload.filename,
        "sessions_queued": queued_count,
    }


@router.get("/agents/{agent_id}/config")
async def get_agent_config(agent_id: str):
    """
    Get the configuration for a specific agent.
    
    Since agent configs are local to agents, this returns a heuristic 
    template based on current session information or an empty template.
    """
    # For now, return a template. 
    # Real implementation would require a fetch_config command.
    return {
        "agent_id": agent_id,
        "config_yaml": f"# Configuration for agent: {agent_id}\nagent_id: {agent_id}\n\nsources:\n  # Add sources here\n\nsenders:\n  # Add senders here\n\npipes:\n  # Add pipes here\n",
        "filename": "default.yaml"
    }


@router.post("/agents/{agent_id}/upgrade")
async def upgrade_agent(agent_id: str, payload: AgentUpgradeRequest):
    """
    Trigger a self-upgrade on a specific agent.

    The command is delivered via heartbeat. The agent will run pip install
    and then restart itself using os.execv.
    """
    all_sessions = await session_manager.get_all_active_sessions()

    matched = []
    for view_id, sess_map in all_sessions.items():
        for sid, si in sess_map.items():
            if si.task_id:
                parts = si.task_id.split(":")
                if parts[0] == agent_id:
                    matched.append((view_id, sid, si))

    if not matched:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active sessions found for agent '{agent_id}'",
        )

    command_dict = {
        "type": "upgrade_agent",
        "version": payload.version,
        "index_url": payload.index_url,
        "upgrade_timeout_sec": payload.upgrade_timeout_sec,
    }

    queued_count = 0
    for view_id, session_id, _ in matched:
        success = await session_manager.queue_command(view_id, session_id, command_dict)
        if success:
            queued_count += 1

    return {
        "status": "ok",
        "agent_id": agent_id,
        "target_version": payload.version,
        "sessions_queued": queued_count,
    }


# ---------------------------------------------------------------------------
# Configuration view
# ---------------------------------------------------------------------------

@router.get("/config")
async def get_config():
    """Return the current Fusion configuration (read-only view)."""
    pipes = {}
    for pid, p in fusion_config.get_all_pipes().items():
        pipes[pid] = {
            "receiver": p.receiver,
            "views": p.views,
            "disabled": p.disabled,
            "session_timeout_seconds": p.session_timeout_seconds,
            "audit_interval_sec": p.audit_interval_sec,
            "sentinel_interval_sec": p.sentinel_interval_sec,
        }

    receivers = {}
    for rid, r in fusion_config.get_all_receivers().items():
        receivers[rid] = {
            "driver": r.driver,
            "port": r.port,
            "disabled": r.disabled,
        }

    views = {}
    for vid, v in fusion_config.get_all_views().items():
        views[vid] = {
            "driver": v.driver,
            "disabled": v.disabled,
            "driver_params": v.driver_params,
        }

    return {
        "pipes": pipes,
        "receivers": receivers,
        "views": views,
    }


# ---------------------------------------------------------------------------
# Fusion self-reload
# ---------------------------------------------------------------------------

@router.post("/reload")
async def reload_fusion():
    """
    Trigger a hot-reload of Fusion configuration.

    Equivalent to sending SIGHUP to the Fusion process.
    """
    try:
        os.kill(os.getpid(), signal.SIGHUP)
        return {"status": "ok", "message": "SIGHUP sent, reload in progress"}
    except Exception as e:
        logger.error(f"Failed to send SIGHUP: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger reload: {e}",
        )
