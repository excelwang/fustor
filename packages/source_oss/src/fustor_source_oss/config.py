from pydantic import BaseModel, Field, AnyHttpUrl, ValidationError, model_validator
from typing import Optional, Literal, Dict, Any
from enum import Enum

class QueueType(str, Enum):
    """Defines the supported queue types for incremental synchronization."""
    POLLING = "polling"  # Simple polling based on object modification time
    SQS = "sqs"          # AWS SQS for event notifications
    AMQP = "amqp"        # AMQP (e.g., RabbitMQ) for event notifications

class SQSQueueConfig(BaseModel):
    """Configuration for AWS SQS queue."""
    queue_url: str = Field(..., description="URL of the SQS queue.")
    region_name: Optional[str] = Field(None, description="AWS region of the SQS queue.")
    visibility_timeout: int = Field(30, ge=0, description="Message visibility timeout in seconds.")

class AMQPQueueConfig(BaseModel):
    """Configuration for AMQP (RabbitMQ) queue."""
    host: str = Field(..., description="AMQP broker hostname.")
    port: int = Field(5672, gt=0, description="AMQP broker port.")
    username: str = Field("guest", description="AMQP username.")
    password: str = Field("guest", description="AMQP password.")
    queue_name: str = Field(..., description="Name of the queue to consume.")
    
class PollingQueueConfig(BaseModel):
    """Configuration for polling-based incremental sync."""
    interval_seconds: int = Field(30, ge=5, description="Polling interval in seconds.")

class OssDriverParams(BaseModel):
    """
    Configuration parameters specific to the OSS Source Driver.
    These parameters are stored within the 'driver_params' field of the core SourceConfig.
    """
    endpoint_url: str = Field(..., description="S3-compatible service endpoint URL.")
    bucket_name: str = Field(..., min_length=3, description="Name of the S3 bucket.")
    region_name: Optional[str] = Field(None, description="AWS region of the S3 bucket.")
    prefix: str = Field("", description="Optional object key prefix to filter files.")
    recursive: bool = Field(True, description="Whether to recursively list objects within the prefix.")
    
    # Incremental synchronization strategy
    queue_type: QueueType = Field(QueueType.POLLING, description="Strategy for incremental synchronization.")
    
    # Specific queue configurations, validated conditionally
    sqs_queue_config: Optional[SQSQueueConfig] = Field(None, description="SQS queue configuration if queue_type is SQS.")
    amqp_queue_config: Optional[AMQPQueueConfig] = Field(None, description="AMQP queue configuration if queue_type is AMQP.")
    polling_queue_config: Optional[PollingQueueConfig] = Field(None, description="Polling queue configuration if queue_type is POLLING.")

    # Custom validator to ensure correct queue config is provided based on queue_type
    @classmethod
    def validate_queue_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        queue_type = values.get("queue_type")
        if queue_type == QueueType.SQS:
            if not values.get("sqs_queue_config"):
                raise ValueError("sqs_queue_config must be provided for SQS queue_type.")
            # Ensure other queue configs are not present if SQS is chosen
            values["amqp_queue_config"] = None
            values["polling_queue_config"] = None
        elif queue_type == QueueType.AMQP:
            if not values.get("amqp_queue_config"):
                raise ValueError("amqp_queue_config must be provided for AMQP queue_type.")
            # Ensure other queue configs are not present if AMQP is chosen
            values["sqs_queue_config"] = None
            values["polling_queue_config"] = None
        elif queue_type == QueueType.POLLING:
            # Polling config is optional, but if present, others are cleared
            values["sqs_queue_config"] = None
            values["amqp_queue_config"] = None
        return values
    
    model_validator(mode='before')(validate_queue_config)