import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch
from fustor_view_fs.api import create_fs_router

@pytest.fixture
def mock_driver():
    driver = AsyncMock()
    # 模拟 tree 结构
    driver.get_directory_tree.return_value = {"path": "/", "children": []}
    driver.get_suspect_list.return_value = {"/file1": 123.456}
    driver.get_blind_spot_list.return_value = {"additions": []}
    return driver

@pytest.fixture
def client(mock_driver):
    app = FastAPI()
    
    async def get_driver(view_id): return mock_driver
    async def check_snapshot(view_id): return True
    def get_view_id(view_id: str = "v123"): return view_id
    
    router = create_fs_router(get_driver, check_snapshot, get_view_id)
    app.include_router(router, prefix="/fs")
    return TestClient(app)

def test_api_tree_dry_run(client):
    """测试 tree API 的 dry_run 模式"""
    response = client.get("/fs/tree?dry_run=true&view_id=v123")
    if response.status_code != 200:
        print(response.json())
    assert response.status_code == 200
    assert response.json()["message"] == "dry-run"

def test_api_tree_on_demand_scan_triggered(client, mock_driver):
    """测试触发按需扫描逻辑"""
    mock_driver.trigger_on_demand_scan = AsyncMock(return_value=(True, "job_1"))
    
    response = client.get("/fs/tree?on_demand_scan=true&path=/data&view_id=v123")
    assert response.status_code == 200
    assert response.json()["job_id"] == "job_1"
    mock_driver.trigger_on_demand_scan.assert_called_once_with("/data", recursive=True)

def test_api_tree_not_found_but_pending(client, mock_driver):
    """测试路径未找到但作业正在进行的特殊处理 (200 OK)"""
    mock_driver.get_directory_tree.return_value = None
    mock_driver.trigger_on_demand_scan = AsyncMock(return_value=(True, "job_2"))
    
    with patch("fustor_fusion.core.session_manager.session_manager.has_pending_job", new_callable=AsyncMock) as mock_pending:
        mock_pending.return_value = True
        
        response = client.get("/fs/tree?on_demand_scan=true&path=/pending&view_id=v123")
        assert response.status_code == 200
        assert "pending" in response.json()["message"]
        assert response.json()["job_pending"] is True

def test_api_update_suspect_list(client, mock_driver):
    """测试批量更新嫌疑列表"""
    payload = {
        "updates": [
            {"path": "/f1", "mtime": 100.0},
            {"path": "/f2", "current_mtime": 200.0}
        ]
    }
    response = client.put("/fs/suspect-list", json=payload)
    assert response.status_code == 200
    assert response.json()["updated_count"] == 2
    assert mock_driver.update_suspect.call_count == 2

def test_api_reset_fallback(client, mock_driver):
    """测试重置 API 的回退逻辑"""
    # 模拟 fustor_fusion 不存在导致的 ImportError
    with patch("fustor_fusion.view_manager.manager.reset_views", side_effect=ImportError):
        response = client.delete("/fs/reset")
        assert response.status_code == 204
        mock_driver.reset.assert_called_once()

def test_api_blind_spots(client, mock_driver):
    """测试盲点查询 API"""
    response = client.get("/fs/blind-spots")
    assert response.status_code == 200
    assert "additions" in response.json()
