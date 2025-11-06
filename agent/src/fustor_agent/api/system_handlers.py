import logging
from fastapi import APIRouter, Depends, HTTPException, status

from fustor_agent.app import App
from .dependencies import get_app
from fustor_core.models.states import EventBusState, SyncState

logger = logging.getLogger("fustor_agent")
router = APIRouter()

@router.get("/status", summary="获取所有运行时实例的聚合状态")
async def get_all_instances_status(app: App = Depends(get_app)):
    """
    一个便捷的端点，用于一次性获取所有运行时实例（总线和同步任务）的
    当前状态、信息和统计数据。
    """
    all_sync_configs = app.sync_config_service.list_configs()
    running_sync_instances = {inst.id: inst for inst in app.sync_instance_service.list_instances()}
    
    pipelines = []
    for sync_id, sync_config in all_sync_configs.items():
        sync_instance = running_sync_instances.get(sync_id)
         
        if sync_instance:
            bus = sync_instance.bus
            overall_status = sync_instance.state
            if bus and bus.state == EventBusState.ERROR:
                overall_status = SyncState.ERROR
            
            pipeline_dto = {
                "id": sync_instance.id,
                "overall_status": overall_status.name,
                "source_id": sync_instance.config.source,
                "pusher_id": sync_instance.config.pusher,
                "bus_info": bus.get_dto().model_dump() if bus else None,
                "sync_info": sync_instance.get_dto().model_dump(),
                "is_disabled": app._app_config.check_sync_is_disabled(sync_id)
            }
        else:
            is_disabled = app._app_config.check_sync_is_disabled(sync_id)
            status_text = "任务已禁用" if is_disabled else "任务已停止"
            pipeline_dto = {
                "id": sync_id,
                "overall_status": "STOPPED",
                "source_id": sync_config.source,
                "pusher_id": sync_config.pusher,
                "bus_info": None,
                "sync_info": {
                    "id": sync_id,
                    "state": "STOPPED",
                    "info": status_text,
                    "statistics": {"events_pushed": 0, "last_pushed_event_id": None},
                    "bus_info": None,
                },
                "is_disabled": is_disabled
            }
        pipelines.append(pipeline_dto)
        
    # --- REFACTORED: Updated the list of "running" states to include the new v2 states ---
    running_states = ['SNAPSHOT_SYNC', 'MESSAGE_SYNC', 'RUNNING_CONF_OUTDATE']
    running_count = sum(1 for p in pipelines if p['overall_status'] in running_states)
    # --- END REFACTOR ---
    outdated_count = sum(1 for p in pipelines if p['overall_status'] == 'RUNNING_CONF_OUTDATE')
    error_count = sum(1 for p in pipelines if p['overall_status'] == 'ERROR')

    global_summary = {
        "running_pipelines": running_count,
        "outdated_pipelines": outdated_count,
        "error_pipelines": error_count
    }

    return {
        "global_summary": global_summary,
        "pipelines": pipelines
    }

@router.post("/_actions/apply_changes", status_code=status.HTTP_202_ACCEPTED, summary="应用所有待定的配置变更")
async def apply_all_pending_changes(app: App = Depends(get_app)):
    """查找所有状态为..._CONF_OUTDATE的Sync实例，并优雅地重启它们以应用新配置。"""
    try:
        restarted_count = await app.sync_instance_service.restart_outdated_syncs()
        return {"message": f"Initiated restart for {restarted_count} outdated sync tasks."}
    except Exception as e:
        logger.error(f"Failed to apply configuration changes: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to apply changes: {e}")