import pytest
from fastapi import HTTPException
from unittest.mock import MagicMock, patch
from fustor_fusion.auth.dependencies import get_view_id_from_api_key
from fustor_fusion.config.unified import ViewConfig, ReceiverConfig, APIKeyConfig

@pytest.mark.asyncio
async def test_auth_via_dedicated_view_key():
    """验证通过 View 专有的 api_keys 进行授权。"""
    mock_view = ViewConfig(api_keys=["view-query-key-123"])
    
    # 我们需要 patch dependencies.py 中使用的 fusion_config
    with patch("fustor_fusion.auth.dependencies.fusion_config") as mock_config:
        mock_config.get_all_views.return_value = {"view-1": mock_view}
        mock_config.get_all_receivers.return_value = {}
        
        view_id = await get_view_id_from_api_key("view-query-key-123")
        assert view_id == "view-1"

@pytest.mark.asyncio
async def test_auth_via_receiver_key_fallback():
    """验证当 View 没有匹配 key 时，回退到 Receiver 的 api_keys 进行授权（兼容老版本）。"""
    mock_receiver = ReceiverConfig(
        api_keys=[APIKeyConfig(key="receiver-key-456", pipe_id="view-2")]
    )
    
    with patch("fustor_fusion.auth.dependencies.fusion_config") as mock_config:
        mock_config.get_all_views.return_value = {"view-1": ViewConfig()}
        mock_config.get_all_receivers.return_value = {"receiver-1": mock_receiver}
        
        view_id = await get_view_id_from_api_key("receiver-key-456")
        assert view_id == "view-2"

@pytest.mark.asyncio
async def test_auth_invalid_key_raises_401():
    """验证无效的 API Key 会抛出 401 异常。"""
    with patch("fustor_fusion.auth.dependencies.fusion_config") as mock_config:
        mock_config.get_all_views.return_value = {"view-1": ViewConfig(api_keys=["valid-key"])}
        mock_config.get_all_receivers.return_value = {}
        
        with pytest.raises(HTTPException) as excinfo:
            await get_view_id_from_api_key("invalid-key")
        
        assert excinfo.value.status_code == 401
        assert excinfo.value.detail == "Invalid or inactive X-API-Key"

@pytest.mark.asyncio
async def test_auth_multiple_views_dedicated_keys():
    """验证多个 View 拥有各自的 dedicated keys 时能正确路由。"""
    view1 = ViewConfig(api_keys=["key1"])
    view2 = ViewConfig(api_keys=["key2"])
    
    with patch("fustor_fusion.auth.dependencies.fusion_config") as mock_config:
        mock_config.get_all_views.return_value = {
            "view-1": view1,
            "view-2": view2
        }
        mock_config.get_all_receivers.return_value = {}
        
        assert await get_view_id_from_api_key("key1") == "view-1"
        assert await get_view_id_from_api_key("key2") == "view-2"

def test_view_config_parsing(tmp_path):
    """验证从 YAML 文件中正确解析 View 的 api_keys。"""
    from fustor_fusion.config.unified import FusionConfigLoader
    
    config_dir = tmp_path / "fusion-config"
    config_dir.mkdir()
    
    yaml_content = """
views:
  view-1:
    driver: fs
    api_keys:
      - query-key-alpha
      - query-key-beta
receivers:
  rec-1:
    driver: http
    api_keys:
      - {key: agent-key, pipe_id: view-1}
"""
    config_file = config_dir / "test.yaml"
    config_file.write_text(yaml_content)
    
    loader = FusionConfigLoader(config_dir=config_dir)
    loader.load_all()
    
    view = loader.get_view("view-1")
    assert view is not None
    assert "query-key-alpha" in view.api_keys
    assert "query-key-beta" in view.api_keys
    
    receiver = loader.get_receiver("rec-1")
    assert receiver is not None
    assert receiver.api_keys[0].key == "agent-key"
    assert receiver.api_keys[0].pipe_id == "view-1"
