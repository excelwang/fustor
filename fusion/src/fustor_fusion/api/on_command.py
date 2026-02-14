# fusion/src/fustor_fusion/api/on_command.py
"""
On-Command Find Fallback Mechanism.

This module provides the logic to fallback to a realtime Agent command
when the memory view is incomplete or unavailable.
"""
import logging
import time
from typing import Any, Dict, List, Optional

from fustor_fusion.core.session_manager import session_manager
from fustor_fusion import runtime_objects

logger = logging.getLogger("fustor_fusion.api.on_command")

async def on_command_fallback(view_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a remote scan on the Agent when local view query fails.
    
    Args:
        view_id: The view identifier
        params: Query parameters (e.g., path, limit)
        
    Returns:
        Formatted view data (compatible with standard view response)
        
    Raises:
        Exception: If fallback also fails
    """
    start_time = time.time()
    
    # 1. Resolve Pipe and Bridge
    pipe_manager = runtime_objects.pipe_manager
    if not pipe_manager:
        raise RuntimeError("Pipe manager not initialized")
        
    pipe = pipe_manager.get_pipe(view_id)
    if not pipe:
        # Try finding by view_id if pipe_id != view_id
        # (This is a simplification, assumes 1:1 or accessible by view_id)
        # In V2, we might need a lookup map, but for now assuming pipe_id=view_id context
        raise RuntimeError(f"No active pipe found for view {view_id}")
        
    bridge = pipe_manager.get_bridge(pipe.id)
    if not bridge:
        # Try to find bridge by pipe ID if pipe ID != view ID
        bridge = pipe_manager.get_bridge(view_id)
        if not bridge:
             raise RuntimeError(f"No session bridge found for pipe {pipe.id}")

    # 2. Find Leader Session
    # We prefer the leader, but any active session with realtime capability works for a scan
    leader_session_id = pipe.leader_session
    target_session_id = leader_session_id
    
    if not target_session_id:
        # Fallback: pick any active session
        # We can implement a more sophisticated selection (e.g., lowest load) later
        active_sessions = await bridge.get_all_sessions()
        if active_sessions:
            target_session_id = next(iter(active_sessions))
            
    if not target_session_id:
        raise RuntimeError(f"No active sessions available for view {view_id} to execute fallback")

    logger.info(f"View {view_id}: Attempting fallback command 'remote_scan' via session {target_session_id}")

    # 3. Construct Command
    # map API params to Agent command params
    # API: path, recursive, limit, etc.
    # Agent Command: path, max_depth, limit
    cmd_params = {
        "path": params.get("path", "/"),
        "max_depth": 1 if not params.get("recursive", False) else params.get("depth", 10),
        "limit": params.get("limit", 1000),
        "pattern": params.get("pattern", "*")
    }

    # 4. Send and Wait
    try:
        result = await bridge.send_command_and_wait(
            session_id=target_session_id,
            command="scan",
            params={**cmd_params, "job_id": start_time}, # start_time as pseudo job_id or rely on bridge cmd_id
            timeout=10.0 # Longer timeout for disk I/O
        )
        
        # 5. Format Response to match View APIv1
        # Expect result structure from agent: {"files": [...], "root": "..."}
        # We need to wrap it to look like a standard view response
        
        duration = time.time() - start_time
        logger.info(f"View {view_id}: Fallback successful in {duration:.3f}s. Items: {len(result.get('files', []))}")
        
        return {
            "path": params.get("path", "/"),
            "entries": result.get("files", []),
            "metadata": {
                "source": "remote_fallback", # Marker to indicate this came from fallback
                "latency_ms": int(duration * 1000),
                "agent_id": result.get("agent_id", "unknown"),
                "timestamp": time.time()
            }
        }
        
    except Exception as e:
        logger.error(f"View {view_id}: Fallback failed: {e}")
        raise
