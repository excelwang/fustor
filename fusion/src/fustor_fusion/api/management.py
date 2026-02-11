# fusion/src/fustor_fusion/api/management.py
"""
Management API for dynamic view start/stop operations.
"""
import logging
from fastapi import APIRouter, HTTPException, status

from ..config.unified import fusion_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/management", tags=["Management"])


@router.get("/views")
async def list_views():
    """List all running views."""
    from ..runtime_objects import view_managers
    
    result = {}
    for v_group_id, vm in view_managers.items():
        result[v_group_id] = list(vm.driver_instances.keys())
    
    return {"running_views": result}



