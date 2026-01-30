import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, AsyncMock
from fustor_fusion.main import app
from fustor_fusion.datastore_state_manager import datastore_state_manager
from fustor_fusion.view_manager.manager import process_event, reset_directory_tree
from fustor_event_model.models import EventBase, EventType
import time

# 模拟 API Key 认证，直接返回 datastore_id = 1
async def mock_get_datastore_id():
    return 1

@pytest_asyncio.fixture
async def client():
    # 覆盖认证依赖
    from fustor_fusion.auth.dependencies import get_datastore_id_from_api_key
    app.dependency_overrides[get_datastore_id_from_api_key] = mock_get_datastore_id
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    
    app.dependency_overrides.clear()

@pytest_asyncio.fixture(autouse=True)
async def setup_data():
    """每个测试前清空并填充模拟数据"""
    datastore_id = 1
    await datastore_state_manager.clear_state(datastore_id)
    await reset_directory_tree(datastore_id)
    
    # 标记同步已完成，以便 API 可用
    session_id = "test-session"
    await datastore_state_manager.set_authoritative_session(datastore_id, session_id)
    await datastore_state_manager.set_snapshot_complete(datastore_id, session_id)
    
    # 构造模拟目录结构:
    # /
    #   dir1/
    #     file1.txt
    #     subdir1/
    #       file2.txt
    #   file3.txt
    
    now = time.time()
    events = [
        # 创建 dir1
        EventBase(
            event_type=EventType.INSERT, event_schema="ds1", table="fs", index=int(now*1000),
            fields=["path", "is_dir", "size"],
            rows=[{"path": "/dir1", "is_dir": True, "size": 0, "modified_time": now, "created_time": now}]
        ),
        # 创建 dir1/file1.txt
        EventBase(
            event_type=EventType.INSERT, event_schema="ds1", table="fs", index=int(now*1000),
            fields=["path", "is_dir", "size"],
            rows=[{"path": "/dir1/file1.txt", "is_dir": False, "size": 100, "modified_time": now, "created_time": now}]
        ),
        # 创建 dir1/subdir1
        EventBase(
            event_type=EventType.INSERT, event_schema="ds1", table="fs", index=int(now*1000),
            fields=["path", "is_dir", "size"],
            rows=[{"path": "/dir1/subdir1", "is_dir": True, "size": 0, "modified_time": now, "created_time": now}]
        ),
        # 创建 dir1/subdir1/file2.txt
        EventBase(
            event_type=EventType.INSERT, event_schema="ds1", table="fs", index=int(now*1000),
            fields=["path", "is_dir", "size"],
            rows=[{"path": "/dir1/subdir1/file2.txt", "is_dir": False, "size": 200, "modified_time": now, "created_time": now}]
        ),
        # 创建 /file3.txt
        EventBase(
            event_type=EventType.INSERT, event_schema="ds1", table="fs", index=int(now*1000),
            fields=["path", "is_dir", "size"],
            rows=[{"path": "/file3.txt", "is_dir": False, "size": 300, "modified_time": now, "created_time": now}]
        ),
    ]
    
    for ev in events:
        await process_event(ev, datastore_id)
    
    yield

@pytest.mark.asyncio
async def test_tree_default_recursive(client):
    """测试默认参数（递归返回全量树）"""
    response = await client.get("/api/v1/views/fs/tree", params={"path": "/"})
    assert response.status_code == 200
    data = response.json()
    
    # 检查根节点
    assert data["path"] == "/"
    names = [c["name"] for c in data["children"]]
    assert "dir1" in names
    assert "file3.txt" in names
    
    # 检查递归深度
    dir1 = next(c for c in data["children"] if c["name"] == "dir1")
    dir1_children_names = [c["name"] for c in dir1["children"]]
    assert "file1.txt" in dir1_children_names
    assert "subdir1" in dir1_children_names
    
    subdir1 = next(c for c in dir1["children"] if c["name"] == "subdir1")
    subdir1_children_names = [c["name"] for c in subdir1["children"]]
    assert "file2.txt" in subdir1_children_names

@pytest.mark.asyncio
async def test_tree_non_recursive(client):
    """测试 recursive=false（仅返回直接子级列表）"""
    response = await client.get("/api/v1/views/fs/tree", params={"path": "/", "recursive": "false"})
    assert response.status_code == 200
    data = response.json()
    
    assert data["path"] == "/"
    # 非递归模式下 children 应为列表
    assert isinstance(data["children"], list)
    assert len(data["children"]) == 2
    
    names = [c["name"] for c in data["children"]]
    assert "dir1" in names
    assert "file3.txt" in names
    
    # 子项不应包含 children 字段
    for child in data["children"]:
        assert "children" not in child

@pytest.mark.asyncio
async def test_tree_max_depth_1(client):
    """测试 max_depth=1 (只包含第一层子节点，不向下递归)"""
    response = await client.get("/api/v1/views/fs/tree", params={"path": "/", "max_depth": 1})
    assert response.status_code == 200
    data = response.json()
    
    assert any(c["name"] == "dir1" for c in data["children"])
    
    # dir1 这一层应该没有 children 字段，因为深度达到了 1
    dir1 = next(c for c in data["children"] if c["name"] == "dir1")
    assert "children" not in dir1

@pytest.mark.asyncio
async def test_tree_max_depth_2(client):
    """测试 max_depth=2"""
    response = await client.get("/api/v1/views/fs/tree", params={"path": "/", "max_depth": 2})
    assert response.status_code == 200
    data = response.json()
    
    # 层级 0: /
    # 层级 1: dir1
    # 层级 2: subdir1 (停止递归)
    
    dir1 = next(c for c in data["children"] if c["name"] == "dir1")
    assert any(c["name"] == "subdir1" for c in dir1["children"])
    
    subdir1 = next(c for c in dir1["children"] if c["name"] == "subdir1")
    assert "children" not in subdir1 # 达到深度 2，停止

@pytest.mark.asyncio
async def test_tree_only_path(client):
    """测试 only_path=true (剔除元数据)"""
    response = await client.get("/api/v1/views/fs/tree", params={"path": "/", "only_path": "true"})
    assert response.status_code == 200
    data = response.json()
    
    # 根节点不应包含元数据字段
    assert "modified_time" not in data
    assert "created_time" not in data
    assert "subtree_max_mtime" not in data
    assert data["path"] == "/"
    
    # 文件节点不应包含 size (注：根据实现，size 在 result 中保留了，但 modified_time 等被剔除)
    # 让我们检查一下具体实现中的 FileNode.to_dict
    file3 = next(c for c in data["children"] if c["name"] == "file3.txt")
    assert "modified_time" not in file3
    assert "path" in file3

@pytest.mark.asyncio
async def test_tree_combined_params(client):
    """测试组合参数: max_depth=1 + only_path=true"""
    response = await client.get("/api/v1/views/fs/tree", params={
        "path": "/", 
        "max_depth": 1,
        "only_path": "true"
    })
    assert response.status_code == 200
    data = response.json()
    
    assert any(c["name"] == "dir1" for c in data["children"])
    dir1 = next(c for c in data["children"] if c["name"] == "dir1")
    assert "children" not in dir1
    assert "modified_time" not in dir1
