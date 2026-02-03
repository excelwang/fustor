# Refactor Global TODO

- [x] **Fix `SenderHandlerAdapter` Test Coverage**
    - [x] Update `MockSender` to support Audit signal spying (spy `signal_audit_start`/`end`)
    - [x] Verify `signal_audit_start` and `signal_audit_end` in `test_send_batch_audit_sync`
    - [x] Add tests for `get_latest_committed_index` (with and without underlying support)
