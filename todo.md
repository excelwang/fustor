# Fustor Todo List

## Source OSS Driver Implementation

- [ ] **Phase 1: Package Setup & Core Driver**
    - [ ] Create directory structure `packages/source_oss`.
    - [ ] Configure `pyproject.toml` for `fustor-source-oss` with `boto3` dependency.
    - [ ] Add `fustor-source-oss` to `fustor_monorepo` workspace (root `pyproject.toml`).
    - [ ] Create `src/fustor_source_oss` package skeleton (`__init__.py`).

- [ ] **Phase 2: Configuration & Data Models**
    - [ ] Implement `config.py` with `OssDriverParams` Pydantic model.
        - [ ] Fields: `endpoint_url`, `bucket_name`, `region_name`, `prefix`, `recursive`.
        - [ ] Strategy fields: `queue_type` (default "polling"), `queue_config`.
    - [ ] Implement internal mapping logic (OSS Object -> `EventBase`).

- [ ] **Phase 3: Driver Logic (S3 Integration)**
    - [ ] Implement `OssSourceDriver` class inheriting from `SourceDriver`.
    - [ ] Implement `__init__`: Initialize `boto3` client using `driver_params` and `PasswdCredential`.
    - [ ] Implement `get_snapshot_iterator`: Use `boto3` Paginator to list objects.
    - [ ] Implement `get_message_iterator` (Polling Mode):
        - [ ] Create logic to poll `list_objects_v2` periodically.
        - [ ] Implement filtering by `LastModified > start_position`.
    - [ ] Implement `test_connection` and `get_available_fields`.
    - [ ] Implement `get_wizard_steps` for UI integration.

- [ ] **Phase 4: Integration & Testing**
    - [ ] Create unit tests using `moto` to mock S3 services.
        - [ ] Test configuration parsing.
        - [ ] Test snapshot iteration.
        - [ ] Test polling iteration.
    - [ ] Register the driver in Agent's `SourceDriverService` (if needed, or rely on dynamic loading).
    - [ ] Manual verification: Connect to a real MinIO/S3 bucket.

- [ ] **Phase 5 (Future): SQS/AMQP Support**
    - [ ] Refactor `get_message_iterator` to support `QueueConsumer` adapter pattern.
    - [ ] Implement `SQSConsumer`.
    - [ ] Implement `RabbitMQConsumer` (with `aio-pika`).
