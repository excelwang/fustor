import logging
import asyncio
from typing import Dict, Any, Iterator, Tuple, Optional
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

from fustor_core.drivers import SourceDriver
from fustor_core.models.config import SourceConfig, PasswdCredential
from fustor_core.exceptions import DriverError
from fustor_core.event import EventBase, EventType

from .config import OssDriverParams, QueueType
from .mapper import map_s3_objects_to_events_batch

logger = logging.getLogger(__name__)

class OssSourceDriver(SourceDriver):
    """
    Fustor Source Driver for S3-compatible object storage.
    Supports snapshot (full listing) and polling-based incremental synchronization.
    """

    def __init__(self, id: str, config: SourceConfig):
        super().__init__(id, config)
        self.logger = logging.getLogger(f"{__name__}.{id}")

        # Parse OSS specific parameters from driver_params
        try:
            self.oss_params = OssDriverParams(**config.driver_params)
        except Exception as e:
            raise DriverError(f"OSS configuration invalid: {e}")

        # Parse credentials
        if not isinstance(config.credential, PasswdCredential):
            raise DriverError("OSS driver requires PasswdCredential (Access Key ID / Secret Access Key).")
        
        self.ak = config.credential.user
        self.sk = config.credential.passwd

        self.bucket_name = self.oss_params.bucket_name
        self.endpoint_url = self.oss_params.endpoint_url
        self.region_name = self.oss_params.region_name
        self.prefix = self.oss_params.prefix

        # Initialize S3 client
        try:
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.ak,
                aws_secret_access_key=self.sk,
                region_name=self.region_name,
            )
            self.logger.info(f"Initialized S3 client for bucket '{self.bucket_name}' at '{self.endpoint_url}'")
        except Exception as e:
            raise DriverError(f"Failed to initialize S3 client: {e}")

    async def get_snapshot_iterator(self, **kwargs) -> Iterator[EventBase]:
        """
        Performs a one-time, full snapshot of the source data by listing all objects
        matching the prefix in the specified S3 bucket.
        """
        self.logger.info(f"Starting snapshot for bucket '{self.bucket_name}' with prefix '{self.prefix}'")
        
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=self.prefix,
            PaginationConfig={"PageSize": kwargs.get("batch_size", 1000)} # Default batch size
        )

        for page in page_iterator:
            contents = page.get("Contents", [])
            if not contents:
                continue

            # Convert to Fustor EventBase batch
            try:
                event_batch = map_s3_objects_to_events_batch(
                    contents,
                    self.bucket_name,
                    "objects", # Logical table name for S3 objects
                    EventType.INSERT
                )
                yield event_batch
            except Exception as e:
                self.logger.error(f"Error mapping S3 objects to event batch: {e}", exc_info=True)
                continue
        
        self.logger.info(f"Snapshot completed for bucket '{self.bucket_name}' with prefix '{self.prefix}'")

    def is_position_available(self, position: int) -> bool:
        """
        For OSS, positions are timestamps. We assume any past timestamp is "available"
        as long as the object was not deleted. Polling logic will handle filtering.
        """
        return True # For polling, we can always attempt to resume from a timestamp


    async def get_message_iterator(self, start_position: int = -1, **kwargs) -> Iterator[EventBase]:
        """
        Performs incremental data capture (CDC) for OSS.
        
        In polling mode, this method periodically lists objects and filters them
        by LastModified timestamp greater than start_position.
        """
        if self.oss_params.queue_type != QueueType.POLLING:
            raise DriverError(f"Unsupported queue_type '{self.oss_params.queue_type}' for get_message_iterator. "
                              "Only POLLING is currently implemented.")
        
        polling_interval = self.oss_params.polling_queue_config.interval_seconds \
                           if self.oss_params.polling_queue_config else 30
        
        self.logger.info(f"Starting message polling for bucket '{self.bucket_name}' with prefix '{self.prefix}' "
                         f"from position {start_position}, interval: {polling_interval}s")

        last_known_position = start_position
        while True:
            try:
                # S3 ListObjectsV2 doesn't support filtering by LastModified directly.
                # We must list and then filter client-side.
                # This can be inefficient for very large buckets/prefixes.
                paginator = self.s3_client.get_paginator("list_objects_v2")
                page_iterator = paginator.paginate(
                    Bucket=self.bucket_name,
                    Prefix=self.prefix,
                )
                
                new_or_modified_objects = []
                current_max_timestamp = last_known_position

                for page in page_iterator:
                    contents = page.get("Contents", [])
                    for s3_obj in contents:
                        last_modified: datetime = s3_obj.get("LastModified")
                        if last_modified:
                            obj_timestamp = int(last_modified.timestamp())
                            
                            # Filter for objects modified AFTER the last_known_position
                            # We use '>=' to potentially include items modified in the same second as the last checkpoint
                            # The consumer (Agent) should handle exact duplicates if any.
                            if obj_timestamp >= last_known_position:
                                new_or_modified_objects.append(s3_obj)
                                if obj_timestamp > current_max_timestamp:
                                    current_max_timestamp = obj_timestamp
                
                if new_or_modified_objects:
                    self.logger.debug(f"Found {len(new_or_modified_objects)} new/modified objects.")
                    # Sort by timestamp to maintain order
                    new_or_modified_objects.sort(key=lambda x: int(x.get("LastModified", datetime.min).timestamp()))

                    # Group into batches
                    batch_size = kwargs.get("batch_size", 100) # Use the batch_size from config
                    for i in range(0, len(new_or_modified_objects), batch_size):
                        batch_objects = new_or_modified_objects[i:i + batch_size]
                        
                        try:
                            event_batch = map_s3_objects_to_events_batch(
                                batch_objects,
                                self.bucket_name,
                                "objects",
                                EventType.UPDATE # Treat as update/insert for changes
                            )
                            yield event_batch
                            last_known_position = int(event_batch.index) # Update position after yielding a batch
                        except Exception as e:
                            self.logger.error(f"Error mapping S3 objects to event batch during polling: {e}", exc_info=True)
                            continue
                else:
                    self.logger.debug("No new or modified objects found in this polling cycle.")
                
                # Update last_known_position to the highest timestamp seen in this cycle
                # This prevents re-processing objects already processed in this polling interval
                last_known_position = current_max_timestamp
                
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "NoSuchBucket":
                    raise DriverError(f"S3 Bucket '{self.bucket_name}' not found: {e}")
                elif error_code == "AccessDenied":
                    raise DriverError(f"Access denied to S3 Bucket '{self.bucket_name}': {e}")
                else:
                    raise DriverError(f"S3 client error during polling: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error during S3 polling: {e}", exc_info=True)
                raise DriverError(f"Unexpected error during S3 polling: {e}")

            await asyncio.sleep(polling_interval)

    async def test_connection(self, **kwargs) -> Tuple[bool, str]:
        """
        Tests the connection to the S3 service by performing a head_bucket operation.
        """
        self.logger.info(f"Testing connection to S3 bucket '{self.bucket_name}' at '{self.endpoint_url}'")
        try:
            await asyncio.to_thread(self.s3_client.head_bucket, Bucket=self.bucket_name)
            self.logger.info("S3 connection test successful.")
            return True, "Connection successful."
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            error_message = e.response.get("Error", {}).get("Message")
            if error_code == "404": # Bucket not found
                return False, f"Bucket '{self.bucket_name}' not found. Error: {error_message}"
            elif error_code == "403": # Access denied
                return False, f"Access denied to bucket '{self.bucket_name}'. Please check credentials and permissions. Error: {error_message}"
            else:
                return False, f"S3 connection failed with client error: {error_code} - {error_message}"
        except Exception as e:
            self.logger.error(f"S3 connection test failed with unexpected error: {e}", exc_info=True)
            return False, f"Connection failed: {str(e)}"

    @classmethod
    async def get_available_fields(cls, **kwargs) -> Dict:
        """
        Declares the data fields that this source can provide.
        For S3, these are common S3 object metadata fields.
        """
        return {
            "type": "object",
            "properties": {
                "Key": {"type": "string", "description": "Object key (path)."},
                "Size": {"type": "integer", "description": "Object size in bytes."}, 
                "LastModified": {"type": "string", "format": "date-time", "description": "Last modified timestamp."}, 
                "ETag": {"type": "string", "description": "ETag of the object."}, 
                "StorageClass": {"type": "string", "description": "Storage class of the object."}, 
                "OwnerId": {"type": "string", "description": "Canonical user ID of the object owner."}, 
                "OwnerDisplayName": {"type": "string", "description": "Display name of the object owner."}, 
            },
            "required": ["Key", "Size", "LastModified"]
        }
    
