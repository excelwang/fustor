import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch, MagicMock
import asyncio
import logging

import pytest
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from fustor_registry.main import app
from fustor_registry.database import get_db
from fustor_registry.models import StateBase as Base
from fustor_registry.models import UserModel as User, UserLevel
from fustor_registry.security import create_access_token, hash_password

# Use an in-memory SQLite database for testing
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

@pytest_asyncio.fixture(scope="function")
async def test_db_engine():
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()

@pytest_asyncio.fixture(scope="function")
async def test_db_session(test_db_engine):
    async_session = sessionmaker(
        test_db_engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        yield session
        # Clean up after each test
        for table in reversed(Base.metadata.sorted_tables):
            await session.execute(table.delete())
        await session.commit()

@pytest_asyncio.fixture(scope="function")
async def override_get_db(test_db_session):
    async def _override_get_db():
        yield test_db_session
    app.dependency_overrides[get_db] = _override_get_db
    yield
    app.dependency_overrides.clear()

@pytest_asyncio.fixture(scope="function")
async def test_admin_user(override_get_db, test_db_session):
    hashed_password = hash_password("admin_password")
    admin_user = User(
        email="admin@example.com",
        password=hashed_password,
        level=UserLevel.ADMIN,
        name="Admin User"
    )
    test_db_session.add(admin_user)
    await test_db_session.commit()
    await test_db_session.refresh(admin_user)
    return admin_user

@pytest_asyncio.fixture(scope="function")
async def authorized_client(override_get_db, test_admin_user) -> AsyncClient:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # Log in the admin user to get a token
        login_data = {
            "username": test_admin_user.email,
            "password": "admin_password"
        }
        response = await client.post("/v1/auth/login", data=login_data, headers={"Content-Type": "application/x-www-form-urlencoded"})
        assert response.status_code == 200, response.text
        token = response.json()["access_token"]
        
        client.headers["Authorization"] = f"Bearer {token}"
        yield client

@pytest_asyncio.fixture(scope="function")
async def test_datastore(authorized_client):
    payload = {
        "name": "test_datastore",
    }
    response = await authorized_client.post("/v1/datastores", json=payload, follow_redirects=True)
    assert response.status_code == 201, response.text
    return response.json()

@pytest_asyncio.fixture(scope="function")
async def invalid_token():
    return "invalid_token_string"

@pytest_asyncio.fixture(scope="function")
async def unauthenticated_client() -> AsyncClient:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client