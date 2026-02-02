# Refactor Global TODOs

> Source: `refactored-branch-review.md` & User Feedback
> Goal: Restore the high-throughput `InMemoryEventQueue` mechanism to the V2 architecture.

## Phase 1: Restore Event Queueing (Completed)

- [x] **1.1 Re-implement/Verify `InMemoryEventQueue`**
    - Verified `FusionPipeline` uses `asyncio.Queue` internally for buffering.
    - Added `queue_size` to stats.

- [x] **1.2 Integrate Queue into `HTTPReceiver`**
    - Verified `HTTPReceiver` pushes to `FusionPipeline` which buffers events.
    - Confirmed non-blocking behavior via tests.

- [x] **1.3 Implement Queue Consumer in `FusionPipeline`**
    - Verified `_processing_loop` consumes queue.

- [x] **1.4 Restore Monitoring**
    - Added `queue_size` to `FusionPipeline.get_aggregated_stats()`.
    - Note: `get_aggregated_stats` is now async.

## Phase 2: Verification (Completed)

- [x] **2.1 Verify Ingestion Throughput**
    - Verified non-blocking behavior via `test_fusion_pipeline_queue.py`.

- [x] **2.2 Verify Correctness**
    - Passed integration tests for queue processing.

