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
        from ..config.unified import fusion_config
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

            # Construct Command
            cmd_params = {
                "path": params.get("path", "/"),
                "max_depth": 1 if not params.get("recursive", False) else params.get("depth", 10),
                "limit": params.get("limit", 1000),
                "pattern": params.get("pattern", "*"),
                "job_id": str(start_time)
            }

            try:
                result = await bridge.send_command_and_wait(
                    session_id=target_session_id,
                    command="scan",
                    params=cmd_params,
                    timeout=fallback_timeout
                )
                return result
            except Exception as e:
                logger.warning(f"Fallback failed on pipe {p_id} (session {target_session_id}): {e}")
                return None

        # 3. Dispatch and Wait
        results = await asyncio.gather(*[execute_on_pipe(pid) for pid in p_ids])
        
        # 4. Aggregate Results
        all_entries = []
        seen_paths = set()
        found_success = False
        
        for res in results:
            if res and "files" in res:
                found_success = True
                for entry in res["files"]:
                    p = entry.get("path")
                    if p not in seen_paths:
                        all_entries.append(entry)
                        seen_paths.add(p)

        if not found_success:
            raise HTTPException(
                status_code=502,
                detail=f"Fallback command failed on all {len(p_ids)} pipes for view {view_id}"
            )

        duration = time.time() - start_time
        logger.info(f"View {view_id}: Fallback successful using {len(p_ids)} pipes. Items: {len(all_entries)}")

        return {
            "path": params.get("path", "/"),
            "entries": all_entries,
            "metadata": {
                "source": "remote_fallback",
                "latency_ms": int(duration * 1000),
                "pipes_queried": len(p_ids),
                "successful_pipes": len([r for r in results if r]),
                "timestamp": time.time()
            }
        }
    except Exception as e:
        logger.error(f"View {view_id}: Fallback FAILED: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Fallback command failed on view {view_id}: {str(e)}"
        )


from fastapi import HTTPException
import asyncio
