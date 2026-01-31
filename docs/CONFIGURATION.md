# Fustor Configuration Guide

Fustor uses a decentralized, YAML-based configuration architecture. All configurations are stored in the `$FUSTOR_HOME` directory (defaults to `~/.fustor`).

## Directory Structure

```text
$FUSTOR_HOME/
├── datastores-config.yaml       # Datastore definitions & API keys
├── views-config/                # View configurations
│   ├── research-fs.yaml
│   └── ...
├── syncs-config/                # Sync task configurations
│   ├── sync-research.yaml
│   └── ...
├── agent.log                    # Agent logs
├── fusion.log                   # Fusion logs
└── agent.pid / fusion.pid       # Process IDs
```

---

## 1. Datastore Configuration

Located at `$FUSTOR_HOME/datastores-config.yaml`. Defines data repositories and access keys.

```yaml
datastores:
  research-data:
    session_timeout_seconds: 30
    allow_concurrent_push: false
    api_key: fk_your_secure_api_key_here
```

- **api_key**: A secret key used by Agents to authenticate when pushing data to this datastore.

**CLI Command:**
```bash
fustor-fusion create-api-key --datastore 1
```

---

## 2. View Configuration

Located in `$FUSTOR_HOME/views-config/*.yaml`. Defines how data is exposed (e.g., as a file system).

**Example: `views-config/research-fs.yaml`**

```yaml
id: research-fs
datastore_id: 1
driver: fs
disabled: false
driver_params:
  hot_item_threshold: 30
```

- **id**: Unique identifier (URL-safe string).
- **datastore_id**: Must match an ID in `datastores-config.yaml`.
- **driver**: The view driver to use (e.g., `fs`).

**CLI Commands:**
```bash
fustor-fusion start-view research-fs
fustor-fusion stop-view research-fs
```

---

## 3. Sync Configuration

Located in `$FUSTOR_HOME/syncs-config/*.yaml`. Defines data synchronization tasks.

**Example: `syncs-config/sync-research.yaml`**

```yaml
id: sync-research
source: fs-research-source
pusher: fusion-main
disabled: false
audit_interval_sec: 600
sentinel_interval_sec: 120
heartbeat_interval_sec: 10
```

- **source**: ID of a source configuration (defined in Agent's source config).
- **pusher**: ID of a pusher configuration (typically pointing to Fusion).

**CLI Commands:**
```bash
fustor-agent start-sync sync-research
fustor-agent stop-sync sync-research
```

---

## 4. Batch Operations

You can start or stop all configured services at once:

```bash
# Start all syncs
fustor-agent start

# Stop all syncs
fustor-agent stop

# Start all views
fustor-fusion start

# Stop all views
fustor-fusion stop
```

## 5. Lifecycle Rules

- **Syncs**: Tie an Agent task to a Fusion Datastore via a Session.
- **Sessions**: Created per-sync. If a Sync stops, the Session ends.
- **Views**: Attached to Datastores. 
  - `start-view`: If a Session is active, triggers `on_session_start` (clears blind spots).
  - `stop-view`: Stops the view. If **no other views** are using the Datastore, terminates all Sessions for that Datastore.
