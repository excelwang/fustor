from fastapi import APIRouter
from .monitoring import monitoring_router
from .on_command import on_command_fallback
from fastapi.responses import HTMLResponse
import os
from fustor_fusion import runtime_objects

# Register fallback for core views
runtime_objects.on_command_fallback = on_command_fallback

router = APIRouter()

# Include monitoring endpoints
router.include_router(monitoring_router)

@router.get("/ui", response_class=HTMLResponse, tags=["Management"])
async def get_management_ui():
    """Serve the built-in management Web UI."""
    ui_path = os.path.join(os.path.dirname(__file__), "view.html")
    if not os.path.exists(ui_path):
        return HTMLResponse(content="UI not found", status_code=404)
    
    with open(ui_path, "r") as f:
        return HTMLResponse(content=f.read())
