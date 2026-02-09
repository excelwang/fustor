import pytest
import pytest_asyncio
import os
import pymysql

from fustor_agent.app import App

@pytest_asyncio.fixture(scope="function")
async def test_db_setup(test_app_instance: App):
    # ... (this fixture is unchanged)
    source_config = test_app_instance.source_config_service.get_config('test-test')
    if not source_config:
        pytest.fail("Source config 'test-test' not found. Ensure config.yaml is correctly set up for tests.")

    mysql_root_password = os.getenv("MYSQL_ROOT_PASSWORD", "")
    if not mysql_root_password:
        pytest.skip("MYSQL_ROOT_PASSWORD environment variable not set, skipping integration test.")

    conn = pymysql.connect(
        host=source_config.uri.split(':')[0],
        port=int(source_config.uri.split(':')[1]),
        user="root",
        password=mysql_root_password,
        database="testdb"
    )
    table_name = "test_snapshot_table"
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                value INT
            );
        """)
        cursor.executemany(f"INSERT INTO {table_name} (name, value) VALUES (%s, %s)", [('record_1', 100), ('record_2', 200), ('record_3', 300)])
    conn.commit()
    conn.close()

    yield table_name

    conn = pymysql.connect(
        host=source_config.uri.split(':')[0],
        port=int(source_config.uri.split(':')[1]),
        user="root",
        password=mysql_root_password,
        database="testdb"
    )
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    conn.close()
