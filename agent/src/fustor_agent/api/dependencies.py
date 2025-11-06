from fastapi import Request
from fustor_agent.app import App

def get_app(request: Request) -> App:
    """FastAPI 依赖注入，用于从应用状态中获取 App 单例。"""
    return request.app.state.app