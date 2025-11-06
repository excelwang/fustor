from fastapi import APIRouter, Depends
from fustor_agent.app import App
from .dependencies import get_app
from fustor_core.models.config import AppConfig

router = APIRouter()

@router.get("/", response_model=AppConfig, summary="Get the entire application configuration")
async def get_app_configuration(app: App = Depends(get_app)):
    return app._app_config