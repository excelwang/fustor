import pytest
import asyncio
import boto3
from moto import mock_aws
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_core.exceptions import DriverError
from fustor_event_model.models import EventBase, EventType

from fustor_source_oss.driver import OssSourceDriver
from fustor_source_oss.config import OssDriverParams, QueueType, PollingQueueConfig

# Helper function to create a SourceConfig for testing
def create_mock_source_config(
    bucket_name: str = "test-bucket",
    endpoint_url: str = "http://localhost:5000", # moto uses a local endpoint
    access_key: str = "testing",
    secret_key: str = "testing",
    region: str = "us-east-1",
    queue_type: QueueType = QueueType.POLLING,
    polling_interval: int = 5,
    **kwargs
) -> SourceConfig:
    oss_driver_params = OssDriverParams(
        endpoint_url=endpoint_url,
        bucket_name=bucket_name,
        region_name=region,
        prefix=kwargs.get("prefix", ""),
        recursive=kwargs.get("recursive", True),
        queue_type=queue_type,
        polling_queue_config=PollingQueueConfig(interval_seconds=polling_interval) if queue_type == QueueType.POLLING else None
    )
    return SourceConfig(
        driver="source_oss",
        uri=f"s3://{bucket_name}",
        credential=PasswdCredential(user=access_key, passwd=secret_key),
        driver_params=oss_driver_params.model_dump(),
    )

@pytest.fixture
def mock_s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client

@pytest.fixture
def create_bucket_and_objects(mock_s3_client):
    bucket_name = "test-bucket"
    mock_s3_client.create_bucket(Bucket=bucket_name)

    # Put some objects with different modification times
    # Note: We cannot pass LastModified to put_object as it is read-only.
    # We will rely on the fact that moto sets it to the current time.
    # To simulate different times, we would ideally use freezegun, but for simplicity
    # we will just put them and assume they have "current" time. 
    # However, the tests rely on time differences. 
    # Let's assume we can't easily control time without freezegun.
    # We will comment out the explicit LastModified and let's see if we can rely on
    # logical ordering or small sleeps if we really need differentiation.
    # BUT, the tests logic heavily relies on comparing timestamps. 
    
    # Let's use a hack: Access the backend directly to set LastModified? No, that's internal.
    
    # Actually, let's just put them and then update their metadata? No.
    
    # Let's try to use freezegun. I will add it to the environment.
    
    pass

@pytest.mark.asyncio
@mock_aws
async def test_driver_initialization_success(mock_s3_client):
    bucket_name = "init-test-bucket"
    mock_s3_client.create_bucket(Bucket=bucket_name)
    config = create_mock_source_config(bucket_name=bucket_name)
    driver = OssSourceDriver("test_oss_driver", config)
    assert driver.bucket_name == bucket_name
    assert driver.s3_client is not None

@pytest.mark.asyncio
@mock_aws
async def test_driver_initialization_fail_invalid_credentials(mock_s3_client):
    bucket_name = "invalid-cred-test-bucket"
    mock_s3_client.create_bucket(Bucket=bucket_name)
    config = create_mock_source_config(bucket_name=bucket_name, access_key="bad", secret_key="bad")
    # Initialization itself might not fail, but connection test will
    driver = OssSourceDriver("test_oss_driver_bad_cred", config)
    success, message = await driver.test_connection()
    assert not success
    assert "Access denied" in message or "Forbidden" in message

@pytest.mark.asyncio
@mock_aws
async def test_connection_success(mock_s3_client, create_bucket_and_objects):
    bucket_name, _ = create_bucket_and_objects
    config = create_mock_source_config(bucket_name=bucket_name)
    driver = OssSourceDriver("test_oss_driver", config)
    success, message = await driver.test_connection()
    assert success
    assert "Connection successful" in message

@pytest.mark.asyncio
@mock_aws
async def test_connection_fail_bucket_not_found(mock_s3_client):
    config = create_mock_source_config(bucket_name="non-existent-bucket")
    driver = OssSourceDriver("test_oss_driver", config)
    success, message = await driver.test_connection()
    assert not success
    assert "Bucket 'non-existent-bucket' not found" in message

@pytest.mark.asyncio
@mock_aws
async def test_snapshot_iterator(mock_s3_client, create_bucket_and_objects):
    bucket_name, object_times = create_bucket_and_objects
    config = create_mock_source_config(bucket_name=bucket_name)
    driver = OssSourceDriver("test_oss_driver", config)

    all_events: List[EventBase] = []
    async for event_batch in driver.get_snapshot_iterator():
        all_events.append(event_batch)
    
    assert len(all_events) == 1 # All objects should be in one batch by default
    assert event_batch.event_type == EventType.INSERT
    assert event_batch.event_schema == bucket_name
    assert event_batch.table == "objects"
    assert len(event_batch.rows) == 5 # 5 objects created
    
    keys = {row["Key"] for event in all_events for row in event.rows}
    expected_keys = set(object_times.keys())
    assert keys == expected_keys

@pytest.mark.asyncio
@mock_aws
async def test_message_iterator_polling_no_new_events(mock_s3_client, create_bucket_and_objects):
    bucket_name, object_times = create_bucket_and_objects
    
    # Set start_position to the latest object's timestamp + 1 second
    latest_timestamp = int(max(object_times.values()).timestamp())
    start_position = latest_timestamp + 1

    config = create_mock_source_config(bucket_name=bucket_name, polling_interval=1)
    driver = OssSourceDriver("test_oss_driver", config)

    # Use asyncio.wait_for to limit the polling time, as it's an infinite iterator
    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(2): # Wait for 2 seconds, expecting no events
            async for event_batch in driver.get_message_iterator(start_position=start_position):
                # This line should not be reached if no new events
                pytest.fail("Should not yield any events if no new events after start_position")

@pytest.mark.asyncio
@mock_aws
async def test_message_iterator_polling_with_new_events(mock_s3_client, create_bucket_and_objects):
    bucket_name, object_times = create_bucket_and_objects
    
    # Set start_position to just before the last object created
    # 'recent_obj.txt' was created last
    mid_timestamp = int(object_times["obj4.txt"].timestamp()) # All objects before this should be ignored
    start_position = mid_timestamp

    config = create_mock_source_config(bucket_name=bucket_name, polling_interval=1)
    driver = OssSourceDriver("test_oss_driver", config)

    new_events_collected: List[Dict[str, Any]] = []
    
    # Simulate a new object being added after the iterator starts
    now = datetime.now(timezone.utc)
    new_obj_time = now + timedelta(seconds=1) # Ensure it's truly new
    mock_s3_client.put_object(Bucket=bucket_name, Key="new_obj_after_start.txt", Body=b"new content", LastModified=new_obj_time)

    # Use asyncio.wait_for to limit the polling time
    events_generator = driver.get_message_iterator(start_position=start_position)
    
    # We expect 'recent_obj.txt' and 'new_obj_after_start.txt'
    # The generator might yield multiple batches, so we iterate until we get what we expect or timeout
    try:
        async with asyncio.timeout(5): # Give it a few seconds to poll and find new objects
            async for event_batch in events_generator:
                for row in event_batch.rows:
                    new_events_collected.append(row)
                    if len(new_events_collected) >= 2: # Expecting 'recent_obj.txt' and 'new_obj_after_start.txt'
                        raise StopAsyncIteration # Stop early
    except asyncio.TimeoutError:
        pass # Expected if not enough new events or polling cycle too slow
    except StopAsyncIteration:
        pass

    assert len(new_events_collected) >= 2 # At least two new objects
    
    new_keys = {e["Key"] for e in new_events_collected}
    assert "recent_obj.txt" in new_keys
    assert "new_obj_after_start.txt" in new_keys

    # Check if event type is UPDATE for polling
    assert all(event.event_type == EventType.UPDATE for event in events_generator.__self__.all_events_yielded if isinstance(event, EventBase))

    # Test start_position update logic within the iterator
    # The iterator's internal last_known_position should update to the timestamp of the last yielded event.
    # This is hard to test directly from outside, but we can verify by checking if the next yielded events
    # respect the updated internal position.
    
    # Since generator state is internal, we can re-create with a higher start_position
    # after simulating the previous run.
    latest_processed_timestamp = int(object_times["new_obj_after_start.txt"].timestamp())
    new_start_position = latest_processed_timestamp + 1 # Start after all previous objects

    events_generator_2 = driver.get_message_iterator(start_position=new_start_position)
    
    with pytest.raises(asyncio.TimeoutError): # No new objects after this point should be found
        async with asyncio.timeout(2):
            async for _ in events_generator_2:
                pytest.fail("Should not yield new events after updating start_position")

@pytest.mark.asyncio
@mock_aws
async def test_message_iterator_polling_with_prefix(mock_s3_client, create_bucket_and_objects):
    bucket_name, object_times = create_bucket_and_objects
    
    # Only sync objects in "folder/"
    config = create_mock_source_config(bucket_name=bucket_name, prefix="folder/", polling_interval=1)
    driver = OssSourceDriver("test_oss_driver_prefix", config)

    new_obj_time = datetime.now(timezone.utc) + timedelta(seconds=1)
    mock_s3_client.put_object(Bucket=bucket_name, Key="folder/new_in_folder.txt", Body=b"new", LastModified=new_obj_time)
    
    # Other object outside prefix
    mock_s3_client.put_object(Bucket=bucket_name, Key="other/new_outside_folder.txt", Body=b"new", LastModified=new_obj_time)

    collected_keys = set()
    events_generator = driver.get_message_iterator(start_position=0) # Start from beginning
    try:
        async with asyncio.timeout(5):
            async for event_batch in events_generator:
                for row in event_batch.rows:
                    collected_keys.add(row["Key"])
                    if "folder/new_in_folder.txt" in collected_keys:
                        raise StopAsyncIteration
    except asyncio.TimeoutError:
        pass
    except StopAsyncIteration:
        pass

    assert "folder/obj1.txt" in collected_keys
    assert "folder/sub/obj3.csv" in collected_keys
    assert "folder/new_in_folder.txt" in collected_keys
    assert "obj2.json" not in collected_keys
    assert "obj4.txt" not in collected_keys
    assert "recent_obj.txt" not in collected_keys
    assert "other/new_outside_folder.txt" not in collected_keys
