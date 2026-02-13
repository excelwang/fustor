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
import hmac
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
    type: str  # "reload_config", "stop_pipe", "scan", "report_status", "upgrade"
    pipe_id: Optional[str] = None
    path: Optional[str] = None
    recursive: Optional[bool] = True
    version: Optional[str] = None  # For upgrade command


class AgentConfigUpdateRequest(BaseModel):
    """Payload for pushing configuration to an agent."""
    config_yaml: str  # Raw YAML content to write to agent's config file
    filename: str = "default.yaml"  # Target config file name


class FusionConfigUpdateRequest(BaseModel):
    """Payload for updating Fusion's own configuration."""
    config_yaml: str
    filename: str = "default.yaml"


# ---------------------------------------------------------------------------
# Drivers discovery
# ---------------------------------------------------------------------------

@router.get("/drivers")
async def list_drivers():
    """List all available driver types for sources, senders, receivers, and views."""
    from fustor_core.transport.receiver import ReceiverRegistry
    from fustor_agent.services.drivers.source_driver import SourceDriverService
    from fustor_agent.services.drivers.sender_driver import SenderDriverService
    from ..view_manager.manager import _load_view_drivers

    # We instantiate services to get the registered drivers
    # Note: This assumes Agent packages are installed in the same environment
    source_service = SourceDriverService()
    sender_service = SenderDriverService()
    
    view_drivers = _load_view_drivers()

    return {
        "sources": source_service.list_available_drivers(),
        "senders": sender_service.list_available_drivers(),
        "receivers": ReceiverRegistry.available_drivers(),
        "views": list(view_drivers.keys())
    }


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
            agent_status = getattr(si, "agent_status", None)
            
            if agent_status and agent_status.get("agent_id"):
                agent_id = agent_status["agent_id"]
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
                "agent_status": getattr(si, "agent_status", None),
            }
            view_sessions.append(sess_dto)

            # Build agent index
            if agent_id:
                if agent_id not in agents_info:
                    agents_info[agent_id] = {
                        "agent_id": agent_id,
                        "client_ip": si.client_ip,
                        "sessions": [],
                        "status": getattr(si, "agent_status", None),
                    }
                agents_info[agent_id]["sessions"].append({
                    "session_id": sid,
                    "view_id": view_id,
                    "task_id": si.task_id,
                })
                # Keep latest status
                latest_status = getattr(si, "agent_status", None)
                if latest_status:
                    agents_info[agent_id]["status"] = latest_status

        sessions_info[view_id] = view_sessions

    return {
        "timestamp": time.time(),
        "views": views_info,
        "pipes": pipes_info,
        "sessions": sessions_info,
        "agents": agents_info,
        "jobs": session_manager.get_agent_jobs(),
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
    if payload.path:
        command_dict["path"] = payload.path
        command_dict["recursive"] = payload.recursive
    if payload.version:
        command_dict["version"] = payload.version

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

class AgentConfigStructuredUpdateRequest(BaseModel):
    """Payload for updating agent config via structured JSON."""
    sources: Optional[Dict[str, Any]] = None
    senders: Optional[Dict[str, Any]] = None
    pipes: Dict[str, Any]
    filename: str = "default.yaml"


@router.post("/agents/{agent_id}/config/structured")
async def update_agent_config_structured(agent_id: str, payload: AgentConfigStructuredUpdateRequest):
    """Update agent config with strict reference integrity validation."""
    import yaml
    all_sessions = await session_manager.get_all_active_sessions()

    current_si = None
    for view_id, sess_map in all_sessions.items():
        for sid, si in sess_map.items():
            if si.task_id and si.task_id.startswith(f"{agent_id}:"):
                if getattr(si, 'reported_config', None):
                    current_si = si
                    break
    
    if not current_si:
        raise HTTPException(status_code=404, detail="Could not find current agent config to merge.")

    try:
        config_dict = yaml.safe_load(current_si.reported_config)
        
        # New Sections from payload
        new_sources = payload.sources if payload.sources is not None else config_dict.get('sources', {})
        new_senders = payload.senders if payload.senders is not None else config_dict.get('senders', {})
        new_pipes = payload.pipes

        # --- Strict Reference Validation ---
        seen_pairs = {}
        for pid, pcfg in new_pipes.items():
            src_id = pcfg.get('source')
            snd_id = pcfg.get('sender')
            
            if src_id not in new_sources:
                raise HTTPException(status_code=400, detail=f"Agent Pipe '{pid}' references unknown Source '{src_id}'")
            if snd_id not in new_senders:
                raise HTTPException(status_code=400, detail=f"Agent Pipe '{pid}' references unknown Sender '{snd_id}'")
            
            # Uniqueness check
            pair = (src_id, snd_id)
            if pair in seen_pairs:
                raise HTTPException(status_code=400, detail=f"Redundant Agent Pipe: {pid} and {seen_pairs[pair]} share same Source/Sender.")
            seen_pairs[pair] = pid

        # Apply changes
        config_dict['sources'] = new_sources
        config_dict['senders'] = new_senders
        config_dict['pipes'] = new_pipes
        
        new_yaml = yaml.dump(config_dict, sort_keys=False)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process config: {e}")

    return await push_agent_config(agent_id, AgentConfigUpdateRequest(config_yaml=new_yaml, filename=payload.filename))


@router.post("/agents/{agent_id}/config")
async def push_agent_config(agent_id: str, payload: AgentConfigUpdateRequest):
    """
    Push a new configuration to an agent with strict validation.
    Only allows adding or removing pipes. Editing existing sections is forbidden.
    """
    import yaml
    all_sessions = await session_manager.get_all_active_sessions()

    matched = []
    current_config_yaml = None
    for view_id, sess_map in all_sessions.items():
        for sid, si in sess_map.items():
            if si.task_id:
                parts = si.task_id.split(":")
                if parts[0] == agent_id:
                    matched.append((view_id, sid, si))
                    if getattr(si, 'reported_config', None):
                        current_config_yaml = si.reported_config

    if not matched:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active sessions found for agent '{agent_id}'",
        )

    # --- Strict Validation Logic ---
    if current_config_yaml:
        try:
            old_cfg = yaml.safe_load(current_config_yaml) or {}
            new_cfg = yaml.safe_load(payload.config_yaml) or {}

            # 1. Ensure non-pipe sections are identical
            for key in set(old_cfg.keys()) | set(new_cfg.keys()):
                if key == 'pipes':
                    continue
                if old_cfg.get(key) != new_cfg.get(key):
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Modification of section '{key}' is restricted. Only 'pipes' can be modified."
                    )

            # 2. Ensure existing pipes are not modified (only add or delete allowed)
            old_pipes = old_cfg.get('pipes', {})
            new_pipes = new_cfg.get('pipes', {})
            for p_id in set(old_pipes.keys()) & set(new_pipes.keys()):
                if old_pipes[p_id] != new_pipes[p_id]:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Editing existing pipe '{p_id}' is restricted. To change it, please delete and re-add."
                    )
        except yaml.YAMLError:
            raise HTTPException(status_code=400, detail="Invalid YAML format")
    # --- End of Validation ---

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
async def get_agent_config(agent_id: str, trigger: bool = False, filename: str = "default.yaml"):
    """
    Get the cached configuration of an agent.
    If 'trigger' is True, queues a 'report_config' command to refresh the cache.
    """
    all_sessions = await session_manager.get_all_active_sessions()
    
    agent_sessions = []
    cached_config = None
    
    for view_id, sess_map in all_sessions.items():
        for sid, si in sess_map.items():
            if si.task_id and si.task_id.startswith(f"{agent_id}:"):
                agent_sessions.append((view_id, sid, si))
                if getattr(si, 'reported_config', None):
                    cached_config = si.reported_config
                    # Keep searching, maybe a newer session has a fresher config

    if not agent_sessions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active sessions found for agent '{agent_id}'",
        )

    if trigger:
        command_dict = {
            "type": "report_config",
            "filename": filename
        }
        # Trigger on all sessions to ensure at least one picks it up quickly
        for v_id, s_id, _ in agent_sessions:
            await session_manager.queue_command(v_id, s_id, command_dict)
        return {"status": "triggered", "message": "report_config command queued"}

    if cached_config:
        import yaml
        try:
            config_dict = yaml.safe_load(cached_config)
        except:
            config_dict = {}
        return {
            "status": "ok", 
            "config_yaml": cached_config, 
            "config_dict": config_dict
        }
    else:
        return {"status": "pending", "message": "Config not yet reported. Use ?trigger=true to request it."}


# ---------------------------------------------------------------------------
# Configuration view
# ---------------------------------------------------------------------------

class FusionConfigStructuredUpdateRequest(BaseModel):
    """Payload for updating Fusion config via structured JSON."""
    receivers: Dict[str, Any]
    views: Dict[str, Any]
    pipes: Dict[str, Any]
    filename: str = "default.yaml"


@router.post("/config/structured")
async def update_fusion_config_structured(payload: FusionConfigStructuredUpdateRequest):
    """Update Fusion config with strict reference integrity validation."""
    import yaml
    from fustor_core.common import get_fustor_home_dir
    
    receivers = payload.receivers
    views = payload.views
    pipes = payload.pipes

    # 1. Validate Pipes
    seen_configs = set()
    for pid, pcfg in pipes.items():
        # Check Receiver reference
        rcv_id = pcfg.get('receiver')
        if rcv_id not in receivers:
            raise HTTPException(status_code=400, detail=f"Pipe '{pid}' references unknown Receiver '{rcv_id}'")
        
        # Check Views references
        p_views = pcfg.get('views', [])
        if not p_views:
            raise HTTPException(status_code=400, detail=f"Pipe '{pid}' must have at least one View")
        for vid in p_views:
            if vid not in views:
                raise HTTPException(status_code=400, detail=f"Pipe '{pid}' references unknown View '{vid}'")

        # Uniqueness check (Receiver + sorted Views)
        config_sig = (rcv_id, tuple(sorted(p_views)))
        if config_sig in seen_configs:
            raise HTTPException(status_code=400, detail=f"Redundant Pipe config: Another pipe already exists with same Receiver and Views.")
        seen_configs.add(config_sig)

    # 2. Validate Receivers (API Keys consistency)
    for rid, rcfg in receivers.items():
        for ak in rcfg.get('api_keys', []):
            target_pipe = ak.get('pipe_id')
            if target_pipe and target_pipe not in pipes:
                raise HTTPException(status_code=400, detail=f"Receiver '{rid}' API key references unknown Pipe '{target_pipe}'")

    # 3. Write to file
    config_path = get_fustor_home_dir() / "fusion-config" / payload.filename
    try:
        config_dict = {
            "receivers": receivers,
            "views": views,
            "pipes": pipes
        }
        new_yaml = yaml.dump(config_dict, sort_keys=False)
        return await update_fusion_config(FusionConfigUpdateRequest(config_yaml=new_yaml, filename=payload.filename))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process Fusion config: {e}")


@router.get("/config")
async def get_config(filename: str = "default.yaml"):
    """Return the current Fusion configuration."""
    from fustor_core.common import get_fustor_home_dir
    import yaml
    
    config_path = get_fustor_home_dir() / "fusion-config" / filename
    raw_yaml = None
    config_dict = {}
    
    if config_path.exists():
        raw_yaml = config_path.read_text(encoding="utf-8")
        try:
            config_dict = yaml.safe_load(raw_yaml) or {}
        except:
            pass

    return {
        "status": "ok",
        "config_dict": config_dict,
        "raw_yaml": raw_yaml,
        "filename": filename
    }


@router.post("/config")
async def update_fusion_config(payload: FusionConfigUpdateRequest):
    """Update Fusion's own configuration file."""
    import yaml
    from fustor_core.common import get_fustor_home_dir
    import shutil

    # Validate YAML
    try:
        yaml.safe_load(payload.config_yaml)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}")

    # Sanitize filename
    safe_name = os.path.basename(payload.filename)
    if not safe_name.endswith((".yaml", ".yml")):
        safe_name += ".yaml"

    config_dir = get_fustor_home_dir() / "fusion-config"
    target_path = config_dir / safe_name
    backup_path = config_dir / f"{safe_name}.bak"

    try:
        if target_path.exists():
            shutil.copy2(target_path, backup_path)
        
        target_path.write_text(payload.config_yaml, encoding="utf-8")
        
        # Auto-trigger reload if it's the default config
        # or we can just return and let user click "Reload"
        return {"status": "ok", "message": f"Config written to {safe_name}. Click 'Reload Fusion' to apply."}
    except Exception as e:
        if backup_path.exists():
            shutil.copy2(backup_path, target_path)
        raise HTTPException(status_code=500, detail=f"Failed to write config: {e}")


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
