# Refactor Global TODO

- [x] **Fix `SenderHandlerAdapter` Test Coverage**
    - [x] Update `MockSender` to support Audit signal spying (spy `signal_audit_start`/`end`)
    - [x] Verify `signal_audit_start` and `signal_audit_end` in `test_send_batch_audit_sync`
    - [x] Add tests for `get_latest_committed_index` (with and without underlying support)
- [x] **Add `sender-http` Test Coverage**
    - [x] Create `packages/sender-http/tests` directory
    - [x] Add `test_http_sender.py` verifying basic operations and error handling
- [x] **Add `sender-openapi` Test Coverage**
    - [x] Create `packages/sender-openapi/tests` directory
    - [x] Add `test_openapi_sender.py` verifying spec parsing and request logic
- [x] **Cleanup Terminology**
    - [x] Rename "pusher" to "sender" in `packages/sender-echo/tests`
    - [x] Rename "pusher" to "sender" in `packages/core/tests`
