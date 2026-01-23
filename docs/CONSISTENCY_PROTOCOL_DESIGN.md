# Generic Consistency Verification Protocol Design

## Problem
The initial implementation of the Sentinel Sweep relied on `get_suspect_list` and `update_suspect_list` methods. These are specific to the File System (FS) domain (dealing with mtime latency and open files) and pollute the generic `SourceDriver` and `PusherDriver` interfaces, or rely on loose `send_command` calls.

## Objective
Decouple the core Agent orchestration (`SyncInstance`) from specific consistency logic (like "Suspect Lists") while maintaining a standard lifecycle hook for "Consistency Verification".

## Proposed Architecture

### 1. Abstract Driver Interface (`fustor_core.drivers`)

We introduce a generic **Verification Protocol** consisting of three methods:

#### `PusherDriver`
*   `async def get_consistency_tasks(self, **kwargs) -> Optional[Dict[str, Any]]`:
    *   Queries the upstream/backend for any data items that require active verification by the source.
    *   Returns a semi-opaque `task_batch` dictionary. For FS, this contains the `suspect_list`.
    *   If `None` or empty, no checks are needed.

*   `async def submit_consistency_results(self, results: Dict[str, Any], **kwargs) -> bool`:
    *   Submits the results of a verification back to the upstream.

#### `SourceDriver`
*   `def perform_consistency_check(self, task_batch: Dict[str, Any]) -> Dict[str, Any]`:
    *   Receives the `task_batch`.
    *   Inspects the `type` or content of the batch to decide how to verify.
    *   For FS: Iterates over paths in `task_batch` and performs `os.stat`.
    *   Returns a `results` dictionary.

### 2. Agent Orchestration (`SyncInstance`)

The `_run_sentinel_sweep` (renamed to `_run_consistency_check_loop`) becomes generic:

```python
async def _run_consistency_check(self):
    # 1. Ask Pusher what needs checking
    tasks = await self.pusher_driver_instance.get_consistency_tasks(source_id=self.config.source)
    if not tasks:
        return

    # 2. Ask Source to check it
    # Run in thread as source drivers might be blocking (like FS)
    results = await asyncio.to_thread(
        self.source_driver_instance.perform_consistency_check, 
        tasks
    )

    if not results:
        return

    # 3. Report back
    await self.pusher_driver_instance.submit_consistency_results(results)
```

### 3. Concrete Implementation Mapping

#### FS Source (`fustor_source_fs`)
*   `perform_consistency_check` logic:
    *   Input: `{"task_type": "suspect_verification", "paths": ["/a/b.txt", ...]}`
    *   Action: `verify_files(paths)`
    *   Output: `{"task_type": "suspect_verification_result", "updates": [{"path":..., "mtime":...}]}`

#### Fusion Pusher (`fustor_pusher_fusion`)
*   `get_consistency_tasks`: Calls `GET /api/view/fs/suspect-list`. Wraps response in Generic Task format.
*   `submit_consistency_results`: Calls `PUT /api/view/fs/suspect-list` with unwrapped payload.

## Benefits
*   **Decoupling**: Agent doesn't know about "suspects" or "mtimes".
*   **Extensibility**: If we add a Database generic driver later, we can implement "Checksum Verification" using the exact same Agent loop.
