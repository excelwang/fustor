# Refactor Global TODO

- [x] **Feature**: Implement Schema Discovery in `SourceHandlerAdapter`.
    - [x] Modify `SourceHandlerAdapter.initialize` to handle `require_schema_discovery`.
    - [x] Add integration test `agent/tests/runtime/test_source_handler_adapter_discovery.py`.
- [x] **Reliability**: Verify Heartbeat Error Handling in `AgentPipeline`.
    - [x] Add integration test `agent/tests/runtime/test_agent_pipeline_heartbeat_reliability.py` to simulate heartbeat failures and verify backoff/recovery.
