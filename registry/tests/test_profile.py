import pytest
pytestmark = pytest.mark.asyncio

from fustor_registry.profile.router import router as profile_router

base_path = f"/v1{profile_router.prefix}"

async def test_user_crud_flow(authorized_client):
    # 1. 获取用户
    response = await authorized_client.get(base_path, follow_redirects=True)
    assert response.status_code == 200
    
    # 2. 更新用户
    update_payload = {
        "name": "New Name"
    }
    response = await authorized_client.put(base_path, json=update_payload, follow_redirects=True)
    assert response.status_code == 200, response.text

async def test_user_change_password(authorized_client):
    # Test successful password change
    valid_payload = {
        "old_password": "admin_password",
        "password": "pwdtesT2!"
    }
    response = await authorized_client.put(f"{base_path}/password", json=valid_payload)
    assert response.status_code == 200, response.text
    
    # Test with incorrect old password
    invalid_payload = {
        "old_password": "wrong_password", 
        "password": "pwdtesT2!"
    }
    response = await authorized_client.put(f"{base_path}/password", json=invalid_payload)
    assert response.status_code == 400, response.text
    assert "旧密码不正确" in response.text
