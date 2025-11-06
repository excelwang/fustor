# src/fustor_agent/api/sync_instances_handlers.py

import logging
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List

from fustor_agent.app import App
from .dependencies import get_app
from fustor_core.models.states import SyncInstanceDTO, EventBusInstance

logger = logging.getLogger("fustor_agent")
router = APIRouter()

@router.get("/buses", response_model=List[EventBusInstance], summary="列出所有事件总线实例")
async def list_bus_instances(app: App = Depends(get_app)):
    """返回当前所有活跃或闲置的EventBus实例的状态快照。"""
    return [bus.get_dto() for bus in app.event_bus_service.list_instances()]

@router.get("/syncs", response_model=List[SyncInstanceDTO], summary="列出所有同步任务实例")
async def list_sync_instances(app: App = Depends(get_app)):
    """返回当前所有已创建的Sync实例的状态快照。"""
    return [sync.get_dto() for sync in app.sync_instance_service.list_instances()]

@router.post("/syncs/{id}/_actions/start", status_code=status.HTTP_202_ACCEPTED, summary="启动单个同步任务")
async def start_sync_instance(id: str, app: App = Depends(get_app)):
    """根据配置启动一个指定的同步任务实例。"""
    logger.debug(f"API HANDLER: Received request to start sync instance {id}. Logger name: {logger.name}, level: {logger.level}")
    try:
        await app.sync_instance_service.start_one(id)
        logger.debug(f"API HANDLER: Successfully initiated start for sync instance {id}")
        return {"message": f"Sync instance {id} start initiated."}
    except Exception as e:
        logger.error(f"Failed to start sync instance {id}: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/syncs/{id}/_actions/stop", status_code=status.HTTP_202_ACCEPTED, summary="停止单个同步任务")
async def stop_sync_instance(id: str, app: App = Depends(get_app)):
    """优雅地停止一个指定的、正在运行的同步任务实例。"""
    try:
        await app.sync_instance_service.stop_one(id)
        return {"message": f"Sync instance {id} stop initiated."}
    except Exception as e:
        logger.error(f"Failed to stop sync instance {id}: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))
