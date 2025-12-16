import pytest
from pydantic import ValidationError
from fustor_registry_client.models import ClientApiKeyResponse, ClientDatastoreConfigResponse

def test_client_api_key_response():
    response = ClientApiKeyResponse(key="test_key", datastore_id=1)
    assert response.key == "test_key"
    assert response.datastore_id == 1

def test_client_datastore_config_response():
    response = ClientDatastoreConfigResponse(datastore_id=1, allow_concurrent_push=True, session_timeout_seconds=60)
    assert response.datastore_id == 1
    assert response.allow_concurrent_push == True
    assert response.session_timeout_seconds == 60
