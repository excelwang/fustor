# Consistency & Sentinel Protocol Design

## Problem
The initial implementation of the Sentinel Sweep relied on domain-specific verification methods and unstructured command mechanisms. This polluted the generic Driver interfaces and made the system hard to extend to non-FS sources.

## Objective
Establish a standardized, standard-based protocol for "Consistency Verification" (Sentinel) and "Full Reconciliation" (Audit) that is decoupled from specific data source implementations.

## Architecture

### 1. Sentinel Protocol (Fast Verification)

The **Sentinel** mechanism allows the Fusion backend to request the Agent to quickly verify specific data items (e.g. "Suspect" files found during real-time processing).

#### Driver Interface (`fustor_core.drivers`)

**PusherDriver**
*   `async def get_sentinel_tasks(self, **kwargs) -> Optional[Dict[str, Any]]`:
    *   Queries `GET /ingestor-api/v1/consistency/sentinel/tasks`.
    *   Returns a task batch (e.g., `{'type': 'suspect_check', 'paths': [...]}`).
*   `async def submit_sentinel_results(self, results: Dict[str, Any], **kwargs) -> bool`:
    *   Submits feedback via `POST /ingestor-api/v1/consistency/sentinel/feedback`.

**SourceDriver**
*   `def perform_sentinel_check(self, task_batch: Dict[str, Any]) -> Dict[str, Any]`:
    *   Receives the task batch.
    *   Performs source-specific verification (e.g. `os.stat` for FS).
    *   Returns results (e.g., `{'type': 'suspect_update', 'updates': [...]}`).

### 2. Audit Protocol (Full Reconciliation)

The **Audit** mechanism triggers a full scan of the source data to reconcile generic differences and close "Blind Spots".

#### Control Flow
1.  **Start**: Agent calls `pusher.signal_audit_start()`.
    *   API: `POST /ingestor-api/v1/consistency/audit/start`
    *   Action: Fusion clears transient Blind Spot lists.
2.  **Scan**: Agent iterates through all data (Snapshot/Audit Iterator) and pushes events with `source_type='audit'`.
3.  **End**: Agent calls `pusher.signal_audit_end()`.
    *   API: `POST /ingestor-api/v1/consistency/audit/end`
    *   Action: Fusion compares the Audit set against its Memory Tree to identify and mark missing files.

### 3. Leader Election & Failover

To ensure only one Agent performs these maintenance tasks (Audit/Sentinel) per Datastore:

*   **Heartbeat**: Agent sends periodic heartbeats to `/ingestor-api/v1/sessions/heartbeat`.
*   **Role Logic**:
    *   Fusion uses a Time-To-Live (TTL) lock for Leadership.
    *   If Leader is missing/timeout, the next heartbeat from any Agent promotes it to Leader.
    *   Response contains `{"role": "leader" | "follower"}`.
*   **Agent Behavior**:
    *   **Follower**: Only pushes Realtime events.
    *   **Leader**: Spawns additional `_run_audit_loop` and `_run_sentinel_loop` tasks.

### 4. API Specification

All consistency endpoints are standardized under `/ingestor-api/v1/consistency/`:

*   `POST /audit/start`: Signal audit cycle start.
*   `POST /audit/end`: Signal audit cycle end.
*   `GET /sentinel/tasks`: Retrieve pending verification tasks (e.g. Suspect List).
*   `POST /sentinel/feedback`: Submit verification results.

## Benefits
*   **Decoupling**: Agent core loop is generic and unaware of "Suspects" or "Files".
*   **Extensibility**: Database sources can implement `perform_sentinel_check` via Row Checksums without changing Agent code.
*   **Robustness**: Automatic failover ensures consistency tasks resume even if an Agent crashes.
