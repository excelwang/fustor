# Fustor Configuration Guide

Fustor uses a decentralized, YAML-based configuration architecture. All configurations are stored in the `$FUSTOR_HOME` directory (defaults to `~/.fustor`).

## Directory Structure

```text
$FUSTOR_HOME/
├── datastores-config.yaml    # [Fusion] Datastore definitions & API keys
├── views-config/             # [Fusion] View configurations
│   ├── research-fs.yaml
│   └── ...
├── sources-config.yaml       # [Agent] Data source definitions
├── pushers-config.yaml       # [Agent] Push destination definitions  
├── syncs-config/             # [Agent] Sync task configurations
│   ├── sync-research.yaml
│   └── ...
├── agent.id                  # Agent unique identifier
├── agent.log / fusion.log    # Logs
└── agent.pid / fusion.pid    # Process IDs
```

---

## Fusion Configuration

### 1. Datastore Configuration

Located at `$FUSTOR_HOME/datastores-config.yaml`. Defines data repositories and access keys.

```yaml
datastores:
  research-data:
    session_timeout_seconds: 30
    allow_concurrent_push: false
    api_key: fk_your_secure_api_key_here
```

| Field | Description |
|-------|-------------|
| `name` | Human-readable name for the datastore |
| `session_timeout_seconds` | Session idle timeout before auto-disconnect |
| `allow_concurrent_push` | Whether multiple agents can push simultaneously |
| `api_key` | Secret key for Agent authentication |

**CLI Command:**
```bash
fustor-fusion create-api-key --datastore 1
```

---

### 2. View Configuration

Located in `$FUSTOR_HOME/views-config/*.yaml`. Defines how data is exposed (e.g., as a file system).

**Example: `views-config/research-fs.yaml`**

```yaml
id: research-fs
datastore_id: 1
driver: fs
disabled: false
driver_params:
  hot_file_threshold: 30
```

| Field | Description |
|-------|-------------|
| `id` | Unique identifier (URL-safe string) |
| `datastore_id` | Must match an ID in `datastores-config.yaml` |
| `driver` | The view driver to use (e.g., `fs`) |
| `driver_params.hot_file_threshold` | Seconds to consider a file "hot" (suspect) |

**CLI Commands:**
```bash
fustor-fusion start-view research-fs
fustor-fusion stop-view research-fs
```

---

## Agent Configuration

### 3. Source Configuration

Located at `$FUSTOR_HOME/sources-config.yaml`. Defines data sources that the Agent monitors.

```yaml
fs-research:
  driver: fs
  uri: /data/research
  disabled: false
  credential:
    user: research-user
  driver_params:
    throttle_interval_sec: 1.0
```

| Field | Description |
|-------|-------------|
| `driver` | Source driver type (`fs`, etc.) |
| `uri` | Path or URI to the data source |
| `disabled` | If true, this source is ignored |
| `driver_params.throttle_interval_sec` | Minimum interval between file system scans |

---

### 4. Pusher Configuration

Located at `$FUSTOR_HOME/pushers-config.yaml`. Defines push destinations where Agent sends data.

```yaml
fusion-main:
  driver: fusion
  endpoint: http://localhost:8102
  disabled: false
  credential:
    key: fk_your_api_key_here
  driver_params:
    datastore_id: 1
```

| Field | Description |
|-------|-------------|
| `driver` | Pusher driver type (`fusion`, etc.) |
| `endpoint` | Fusion server URL |
| `credential.key` | API key for authentication |
| `driver_params.datastore_id` | Target datastore ID |

---

### 5. Sync Configuration

Located in `$FUSTOR_HOME/syncs-config/*.yaml`. Defines data synchronization tasks.

**Example: `syncs-config/sync-research.yaml`**

```yaml
id: sync-research
source: fs-research
pusher: fusion-main
disabled: false
audit_interval_sec: 600
sentinel_interval_sec: 120
heartbeat_interval_sec: 10
```

| Field | Description |
|-------|-------------|
| `id` | Unique sync task identifier |
| `source` | ID of a source in `sources-config.yaml` |
| `pusher` | ID of a pusher in `pushers-config.yaml` |
| `audit_interval_sec` | Full directory scan interval |
| `sentinel_interval_sec` | Suspect file verification interval |
| `heartbeat_interval_sec` | Session keep-alive interval |

**CLI Commands:**
```bash
fustor-agent start-sync sync-research
fustor-agent stop-sync sync-research
```

---

## Batch Operations

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

## Lifecycle Rules

- **Syncs**: Tie an Agent task to a Fusion Datastore via a Session.
- **Sessions**: Created per-sync. If a Sync stops, the Session ends.
- **Views**: Attached to Datastores. 
  - `start-view`: If a Session is active, triggers `on_session_start` (clears blind spots).
  - `stop-view`: Stops the view. If **no other views** are using the Datastore, terminates all Sessions for that Datastore.

---

## Legacy Configuration Support

For backward compatibility, the Agent still supports the legacy `agent-config.yaml` format:

```yaml
sources:
  fs-research:
    driver: fs
    uri: /data/research

pushers:
  fusion-main:
    driver: fusion
    endpoint: http://localhost:8102
    credential:
      key: fk_your_api_key

syncs:
  sync-research:
    source: fs-research
    pusher: fusion-main
```

**Note:** If `sources-config.yaml` and `pushers-config.yaml` are not found, the Agent will fall back to loading from `agent-config.yaml`.
