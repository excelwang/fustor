from typing import Dict, Any, List
from datetime import datetime
from fustor_event_model.models import EventBase, EventType

def map_s3_object_to_event(
    s3_object: Dict[str, Any], 
    bucket_name: str, 
    table_name: str, 
    event_type: EventType = EventType.INSERT
) -> EventBase:
    """
    Maps a single S3 object dictionary to a Fustor EventBase.

    Args:
        s3_object: A dictionary representing an S3 object (from boto3 ListObjectsV2).
        bucket_name: The name of the S3 bucket.
        table_name: The logical table name for the event (e.g., 'objects').
        event_type: The type of event (INSERT, UPDATE, DELETE).

    Returns:
        An EventBase instance.
    """
    
    # Extract relevant metadata from the S3 object
    key = s3_object.get("Key")
    if not key:
        raise ValueError("S3 object must have a 'Key'.")

    size = s3_object.get("Size")
    last_modified: datetime = s3_object.get("LastModified")
    etag = s3_object.get("ETag")
    storage_class = s3_object.get("StorageClass")
    owner_id = s3_object.get("Owner", {}).get("ID")
    owner_display_name = s3_object.get("Owner", {}).get("DisplayName")

    # The index will be the Unix timestamp of LastModified for ordering
    # Use -1 if LastModified is not available, though it usually is for objects.
    index = int(last_modified.timestamp()) if last_modified else -1

    # Prepare the row data
    row_data = {
        "Key": key,
        "Size": size,
        "LastModified": last_modified.isoformat() if last_modified else None,
        "ETag": etag,
        "StorageClass": storage_class,
        "OwnerId": owner_id,
        "OwnerDisplayName": owner_display_name,
        # Add any other relevant S3 object metadata here
    }

    # Define the fields present in the row
    fields = list(row_data.keys())

    return EventBase(
        event_type=event_type,
        event_schema=bucket_name,  # Use bucket name as the schema
        table=table_name,
        index=index,
        fields=fields,
        rows=[row_data], # Each S3 object becomes one row in an event batch
    )

def map_s3_objects_to_events_batch(
    s3_objects: List[Dict[str, Any]],
    bucket_name: str,
    table_name: str,
    event_type: EventType = EventType.INSERT
) -> EventBase:
    """
    Maps a list of S3 objects to a single EventBase with multiple rows.
    This is useful for batching inserts/updates from a paginator.
    """
    if not s3_objects:
        raise ValueError("s3_objects list cannot be empty.")

    # Assume all objects in the batch belong to the same schema and table
    # and have similar structure for fields.
    first_object = s3_objects[0]
    
    # The index for a batch event could be the largest LastModified timestamp
    # or the last object's timestamp in the batch. Let's use the last object for now.
    last_modified = s3_objects[-1].get("LastModified")
    index = int(last_modified.timestamp()) if last_modified else -1

    rows: List[Dict[str, Any]] = []
    for s3_obj in s3_objects:
        row_data = {
            "Key": s3_obj.get("Key"),
            "Size": s3_obj.get("Size"),
            "LastModified": s3_obj.get("LastModified").isoformat() if s3_obj.get("LastModified") else None,
            "ETag": s3_obj.get("ETag"),
            "StorageClass": s3_obj.get("StorageClass"),
            "OwnerId": s3_obj.get("Owner", {}).get("ID"),
            "OwnerDisplayName": s3_obj.get("Owner", {}).get("DisplayName"),
        }
        rows.append(row_data)
    
    # All rows in the batch are assumed to have the same fields, derive from first one
    fields = list(rows[0].keys()) if rows else []

    return EventBase(
        event_type=event_type,
        event_schema=bucket_name,
        table=table_name,
        index=index,
        fields=fields,
        rows=rows,
    )
