import pytest
from pydantic import ValidationError
from fustor_common.models import (
    ResponseBase,
    ApiKeyBase,
    MessageResponse,
    DatastoreBase,
    TokenResponse,
    LogoutResponse,
    Password,
    ValidationResponse,
    CleanupResponse,
    AdminCredentials,
    DatastoreConfig,
)

def test_response_base():
    response = ResponseBase()
    assert isinstance(response, ResponseBase)

def test_api_key_base():
    api_key = ApiKeyBase(name="test_key", datastore_id=1)
    assert api_key.name == "test_key"
    assert api_key.datastore_id == 1
    with pytest.raises(ValidationError):
        ApiKeyBase(name="a", datastore_id=1) # Too short
    with pytest.raises(ValidationError):
        ApiKeyBase(name="a"*51, datastore_id=1) # Too long

def test_message_response():
    message = MessageResponse(message="Success")
    assert message.message == "Success"

def test_datastore_base():
    datastore = DatastoreBase(name="my_datastore")
    assert datastore.name == "my_datastore"
    assert datastore.visible == False
    assert datastore.allow_concurrent_push == False
    assert datastore.session_timeout_seconds == 30

def test_token_response():
    token = TokenResponse(access_token="abc", token_type="bearer")
    assert token.access_token == "abc"
    assert token.token_type == "bearer"

def test_logout_response():
    logout = LogoutResponse(detail="注销成功")
    assert logout.detail == "注销成功"

def test_password_model():
    # Valid password
    password = Password(password="Password123!")
    assert password.password == "Password123!"

    # Invalid password - too short
    with pytest.raises(ValidationError):
        Password(password="Short1!")

    # Invalid password - too long
    with pytest.raises(ValidationError):
        Password(password="a"*65)

    # Invalid password - no letter
    with pytest.raises(ValidationError, match="必须包含至少一个字母"):
        Password(password="12345678!")

    # Invalid password - no digit
    with pytest.raises(ValidationError, match="必须包含至少一个数字"):
        Password(password="Password!!")

    # Invalid password - invalid characters
    with pytest.raises(ValidationError):
        Password(password="Password123^") # ^ is not in allowed pattern

def test_validation_response():
    response = ValidationResponse(success=True, message="Valid")
    assert response.success == True
    assert response.message == "Valid"

def test_cleanup_response():
    response = CleanupResponse(message="Cleaned", deleted_count=5, deleted_ids=["id1", "id2"])
    assert response.message == "Cleaned"
    assert response.deleted_count == 5
    assert response.deleted_ids == ["id1", "id2"]

def test_admin_credentials():
    creds = AdminCredentials(user="admin", passwd="password")
    assert creds.user == "admin"
    assert creds.passwd == "password"

def test_datastore_config():
    config = DatastoreConfig(datastore_id=1, allow_concurrent_push=True, session_timeout_seconds=60)
    assert config.datastore_id == 1
    assert config.allow_concurrent_push == True
    assert config.session_timeout_seconds == 60
