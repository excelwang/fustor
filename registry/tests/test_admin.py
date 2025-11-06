import pytest
pytestmark = pytest.mark.asyncio

from fustor_registry.models import UserLevel
from fustor_registry.admin.router import router as admin_router

base_path = f"/v1{admin_router.prefix}"

async def test_user_crud_flow(authorized_client):
    # 1. 创建用户（需要管理员权限）
    payload = {
        "name": "newuser",
        "email": "newuser@example.com",
        "password": "NewPass123!",
        "level": UserLevel.NORMAL.value # Use .value for enum
    }
    response = await authorized_client.post(f"{base_path}/users", json=payload)
    assert response.status_code == 201, response.json()
    
    user_id = response.json()["id"]
    
    # 2. 获取用户
    response = await authorized_client.get(f"{base_path}/users/{user_id}")
    assert response.status_code == 200
    assert response.json()["name"] == payload["name"]