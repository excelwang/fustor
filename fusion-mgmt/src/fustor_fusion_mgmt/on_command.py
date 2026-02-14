# fusion-mgmt/src/fustor_fusion_mgmt/on_command.py
"""
On-Command Find Fallback Mechanism.

This module provides the logic to fallback to a realtime Agent command
when the memory view is incomplete or unavailable.
"""
import logging
import time
import asyncio
from typing import Any, Dict, List, Optional
from fastapi import HTTPException

from fustor_fusion.core.session_manager import session_manager
from fustor_fusion import runtime_objects

logger = logging.getLogger("fustor_fusion_mgmt.on_command")

async def on_command_fallback(view_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a remote scan on the Agent when local view query fails.
    """
    start_time = time.time()
    
    try:
        # 1. Resolve Pipes
        pipe_manager = runtime_objects.pipe_manager
        if not pipe_manager:
            raise RuntimeError("Pipe manager not initialized")

        p_ids = pipe_manager.resolve_pipes_for_view(view_id)
        if not p_ids:
            raise RuntimeError(f"No active pipes found to service view {view_id}")

        # 2. Prepare Parallel Execution
        from fustor_fusion.config.unified import fusion_config
        fallback_timeout = fusion_config.fusion.on_command_fallback_timeout

        async def execute_on_pipe(p_id: str) -> Optional[Dict[str, Any]]:
            pipe = pipe_manager.get_pipe(p_id)
            if not pipe: return None
            
            bridge = pipe_manager.get_bridge(p_id)
            if not bridge: return None

            # Use leader session or any active session
            target_session_id = pipe.leader_session
            if not target_session_id:
                active_sessions = await bridge.get_all_sessions()
                if active_sessions:
                    target_session_id = next(iter(active_sessions))
            
            if not target_session_id:
                return None

            # 1. Prepare Command
            target_path = params.get("path", "/")
            cmd_params = {
                "path": target_path,
                "max_depth": 1 if not params.get("recursive", False) else params.get("depth", 10),
                "limit": params.get("limit", 1000),
                "pattern": params.get("pattern", "*"),
            }

            # 2. Execute via Bridge (Synchronous-like wait)
            try:
                result = await bridge.send_command_and_wait(
                    session_id=target_session_id,
                    command="scan",
                    params=cmd_params,
                    timeout=fallback_timeout
                )
                if result:
                    result["success"] = True
                return result
            except Exception as e:
                logger.warning(f"Fallback command failed on session {target_session_id}: {e}")
                return None

        # 3. Dispatch and Wait
        results = await asyncio.gather(*[execute_on_pipe(pid) for pid in p_ids])
        
        # 4. Check Success
        best_result = None
        for r in results:
            if r and r.get("success"):
                best_result = r
                break

        if not best_result:
            raise HTTPException(
                status_code=502,
                detail=f"Fallback command failed on all {len(p_ids)} pipes for view {view_id}"
            )

        # 5. Process and Return Result
        duration = time.time() - start_time
        
        # Map result to API format
        entries = best_result.get("files", best_result.get("entries", []))
        
        return {
            "path": params.get("path", "/"),
            "entries": entries,
            "metadata": {
                "source": "remote_fallback",
                "latency_ms": int(duration * 1000),
                "pipes_queried": len(p_ids),
                "successful_pipes": len([r for r in results if r]),
                "timestamp": time.time(),
                "agent_id": best_result.get("agent_id")
            }
        }
    except Exception as e:
        logger.error(f"View {view_id}: Fallback FAILED: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Fallback command failed on view {view_id}: {str(e)}"
        )
