# FusionPipe Deadlock with `wait_for_drain`

## Issue Description

When handling an event batch in `FusionPipe.process_events`, if the event itself requires the pipe to be "drained" before processing continues (e.g. Audit End or Snapshot End signals), a deadlock can occur.

The deadlock happens because `process_events` tracks active pushes:

```python
# fusion_pipe.py (Before Fix)
# self._active_pushes += 1 (Incremented on entry)
# ...
if needs_drain:
    await self.wait_for_drain() # ERROR: Waits for queue=0 AND active_pushes=0
```

However, since `process_events` is currently executing, `_active_pushes` is at least 1. Waiting for 0 will cause it to wait for *itself* to finish, creating a deadlock.

## The Fix

`wait_for_drain` must accept a `target_active_pushes` parameter.

If calling from inside a push handler (`process_events`), you MUST account for the current active push:

```python
# Correct usage inside process_events or other push handlers
await self.wait_for_drain(timeout=30.0, target_active_pushes=1)
```

If calling from an external controller (e.g. during shutdown), standard behavior is correct:

```python
# Correct usage outside of push handlers
await self.wait_for_drain(timeout=30.0) # targets 0 by default
```

## Pattern: Polling vs Event

- **Target=0**: Uses `asyncio.Event` (`_queue_drained`) for efficiency.
- **Target>0**: Uses polling check because the event is only set when count reaches exactly 0.

## Related Code

- `fustor_fusion/runtime/fusion_pipe.py`: `process_events` and `wait_for_drain`.
