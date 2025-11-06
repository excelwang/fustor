import pytest
pytestmark = pytest.mark.asyncio

from fustor_registry.apikey.router import router as apikey_router
from fustor_registry.schemas import ApiKeyCreate

base_path = f"/v1{apikey_router.prefix}"

async def test_create_and_list_apikey(authorized_client, test_datastore):
    # 测试创建API Key
    key_data = {
        "name": "test-key",
        "datastore_id": test_datastore["id"]
    }
    
    # 创建API Key
    response = authorized_client.post(f"{base_path}/", json=key_data)
    assert response.status_code == 201
    created_key = response.json()
    assert created_key["name"] == key_data["name"]
    assert len(created_key["key"]) == 43  # 验证生成的密钥长度
    
    # 测试同名Key冲突
    conflict_response = authorized_client.post(f"{base_path}/", json=key_data)
    assert conflict_response.status_code == 400
    assert "名称已存在" in conflict_response.json()["detail"]
    
    # 获取Key列表
    list_response = authorized_client.get(f"{base_path}/")
    assert list_response.status_code == 200
    keys = list_response.json()
    assert len(keys) >= 1
    assert keys[0]["name"] == key_data["name"]

async def test_create_with_datastore(authorized_client, test_datastore):
    # 测试带datastore_id的创建
    key_data = {
        "name": "with-datastore",
        "datastore_id": int(test_datastore["id"])
    }
    
    response = authorized_client.post(f"{base_path}/", json=key_data)
    assert response.status_code == 201
    assert response.json()["datastore_id"] == test_datastore["id"]

async def test_invalid_datastore_id(authorized_client):
    # 测试无效的datastore_id
    key_data = {
        "name": "invalid-datastore",
        "datastore_id": 999
    }
    
    response = authorized_client.post(f"{base_path}/", json=key_data)
    assert response.status_code == 400
    assert "无效的存储库ID" in response.json()["detail"]

async def test_delete_apikey(authorized_client, test_datastore):
    # 先创建测试Key
    create_response = authorized_client.post(
        f"{base_path}/",
        json={
            "name": "temp-key",
            "datastore_id": test_datastore["id"]
        }
    )
    key_id = create_response.json()["id"]
    
    # 删除Key
    delete_response = authorized_client.delete(f"{base_path}/{key_id}")
    assert delete_response.status_code == 204
    
    # 验证删除后不存在
    list_response = authorized_client.get(f"{base_path}/")
    assert all(key["id"] != key_id for key in list_response.json())

async def test_apikey_model_validation():
    # 测试模型校验逻辑
    with pytest.raises(ValueError):
        ApiKeyCreate(name="ab", datastore_id=1)  # 名称过短

