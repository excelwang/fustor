import pytest
from unittest.mock import MagicMock, AsyncMock

from fustor_source_elasticsearch import ElasticsearchDriver
from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_event_model.models import InsertEvent

@pytest.fixture
def es_config():
    """Provides a default Elasticsearch SourceConfig."""
    return SourceConfig(
        driver="elasticsearch",
        uri="http://localhost:9200",
        credential=PasswdCredential(user="elastic", passwd="changeme"),
        driver_params={
            "index_name": "test-index",
            "timestamp_field": "@timestamp"
        }
    )

@pytest.mark.asyncio
async def test_test_connection(mocker):
    """Tests the test_connection class method."""
    mock_async_es_client = AsyncMock()
    mock_async_es_client.ping.return_value = True
    mocker.patch("fustor_source_elasticsearch.ElasticsearchDriver._get_async_es_client", return_value=mock_async_es_client)

    status, msg = await ElasticsearchDriver.test_connection(uri="http://localhost:9200", credential={})
    
    assert status is True
    assert "Successfully connected" in msg
    mock_async_es_client.close.assert_awaited_once()

@pytest.mark.asyncio
async def test_get_available_fields(mocker):
    """Tests the get_available_fields class method."""
    mock_async_es_client = AsyncMock()
    mock_async_es_client.indices.get_mapping.return_value = {
        "test-index": {
            "mappings": {
                "properties": {
                    "message": {"type": "text"},
                    "agent": {"properties": {"id": {"type": "keyword"}}}
                }
            }
        }
    }
    mocker.patch("fustor_source_elasticsearch.ElasticsearchDriver._get_async_es_client", return_value=mock_async_es_client)

    fields = await ElasticsearchDriver.get_available_fields(
        uri="http://localhost:9200", 
        credential={},
        driver_params={"index_name": "test-index"}
    )

    expected_properties = {
        "message": {"type": "text"},
        "agent.id": {"type": "keyword"},
        "_id": {"type": "keyword"},
        "_index": {"type": "keyword"}
    }
    assert fields == {"properties": expected_properties}
    mock_async_es_client.close.assert_awaited_once()

def test_get_snapshot_iterator(es_config, mocker):
    """Tests the get_snapshot_iterator instance method."""
    mock_sync_es_client = MagicMock()
    mock_sync_es_client.open_point_in_time.return_value = {"id": "pit-id"}
    mock_sync_es_client.search.side_effect = [
        {"hits": {"hits": [{"_source": {"@timestamp": "2023-01-01T12:00:00Z"}, "sort": [1]}]}},
        {"hits": {"hits": []}} # Second call returns no hits to terminate loop
    ]
    mocker.patch("fustor_source_elasticsearch.ElasticsearchDriver._get_sync_es_client", return_value=mock_sync_es_client)

    driver = ElasticsearchDriver("test-es-id", es_config)
    iterator = driver.get_snapshot_iterator()
    
    events = list(iterator)
    
    assert len(events) == 1
    assert isinstance(events[0], InsertEvent)
    mock_sync_es_client.search.assert_called()
    mock_sync_es_client.close_point_in_time.assert_called_once_with(id="pit-id")
