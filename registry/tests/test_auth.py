import pytest
pytestmark = pytest.mark.asyncio
from fustor_registry.auth.router import router as auth_router

base_path = f"/v1{auth_router.prefix}"

async def test_user_login(
    admin_test_client,
    test_admin_user # We need the user to get the email and password
):
    """测试用户登录"""
    response = admin_test_client.post(
        f"{base_path}/login",
        data={  # 使用表单数据而不是JSON
            "username": test_admin_user.email,
            "password": "admin_password"
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    assert response.status_code == 200, response.json()
    assert "access_token" in response.json()

async def test_invalid_token_access(
    test_client,
    invalid_token
):
    """测试使用无效令牌访问"""
    response = test_client.get(
        f"/v1/profile",
        headers={"Authorization": f"Bearer {invalid_token}"}
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "非法访问"

async def test_refresh_with_invalid_token(
    test_client,
    invalid_token
):
    """测试使用无效令牌刷新"""
    response = test_client.post(
        f"{base_path}/refresh",
        headers={"Authorization": f"Bearer {invalid_token}"}
    )
    assert response.status_code == 401

async def test_unauthenticated_access(test_client):
    """测试未认证访问"""
    response = test_client.get(f"/v1/profile")
    assert response.status_code == 401

async def test_logout_revoked_token(
    authorized_client # Use the standard authorized client
):
    """测试令牌注销机制"""
    # 访问需要验证的端点
    response = authorized_client.get("/v1/profile")
    assert response.status_code == 200, response.json()
    
    # 执行注销操作
    logout_res = authorized_client.get(f"{base_path}/logout")
    assert logout_res.status_code == 200, logout_res.text
    
    # 验证令牌已失效
    failed_res = authorized_client.get(f"/v1/profile")
    assert failed_res.status_code == 401, failed_res.json()
