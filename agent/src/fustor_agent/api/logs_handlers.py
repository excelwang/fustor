from fastapi import APIRouter, Depends, Query
from typing import List, Optional

from fustor_agent.services.log import LogService
from fustor_core.models.log import LogEntry

router = APIRouter()

@router.get(
    "/", 
    response_model=List[LogEntry],
    summary="获取结构化的日志条目"
)
def list_logs(
    limit: int = Query(100, ge=1, le=1000, description="返回的日志条目最大数量"),
    level: Optional[str] = Query(None, description="按日志级别过滤 (e.g., 'INFO', 'ERROR')"),
    component_name: Optional[str] = Query(None, alias="component", description="按组件名称过滤 (e.g., 'fustor_agent.services.sync')"),
    before_line: Optional[int] = Query(None, ge=1, description="用于分页，获取指定行号之前的日志")
) -> List[LogEntry]:
    """
    提供强大的日志查询能力，支持分页和过滤。

    - **滚动加载/分页**: 使用 `before_line` 参数。首次请求不带此参数，获取最新的日志。
      后续请求将返回结果中最后一条日志的 `line_number` 作为下一次请求的 `before_line` 值，
      即可实现向下滚动加载历史日志。
    - **过滤**: 可通过 `level` 和 `component_name` 对日志进行精确筛选。
    """
    log_service = LogService()
    logs = log_service.get_logs(
        limit=limit,
        level=level,
        component=component_name,
        before_line=before_line
    )
    return logs