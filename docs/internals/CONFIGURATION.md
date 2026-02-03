# Fustor Configuration Guide

Fustor uses a decentralized, YAML-based configuration architecture. All configurations are stored in the `$FUSTOR_HOME` directory (defaults to `~/.fustor`).

## Directory Structure

```text
$FUSTOR_HOME/
├── receivers-config.yaml       # [Fusion] Ingress definitions & API keys
├── views-config/               # [Fusion] View configurations
│   ├── research-fs.yaml
│   └── ...
├── sources-config.yaml         # [Agent] Data source definitions
├── senders-config.yaml         # [Agent] Data destination definitions
├── pipelines-config/         # [Agent] Pipeline task definitions
│   ├── pipe-research.yaml
│   └── ...
├── agent.id                    # Agent unique identifier
├── agent.log / fusion.log      # Logs
└── agent.pid / fusion.pid      # Process IDs
```

### Deprecated (V1)
- `views-config.yaml` (Replaced by `receivers-config.yaml` + `views-config`)
- `pushers-config.yaml` (Replaced by `senders-config.yaml`)
- `pipelines-config/` (Replaced by `pipelines-config/`)

---

## Fusion Configuration

### 1. Receiver Configuration (Ingress & Auth)

Located at `$FUSTOR_HOME/receivers-config.yaml`. Defines how Fusion receives data and authenticates Agents.

```yaml
receivers:
  default-http:
    driver: "http"
    port: 8000
    host: "0.0.0.0"
    api_keys:
      "fk_secure_key_1":
        role: "admin"
        view_mappings: ["research-fs"]
```

| Field | Description |
|-------|-------------|
| `driver` | Transport driver (e.g., `http`) |
| `port` | Listening port |
| `api_keys` | Map of keys to roles and accessible views |

### 2. View Configuration

Located in `$FUSTOR_HOME/views-config/*.yaml`. Defines the storage backend for data.

**Example: `views-config/research-fs.yaml`**

```yaml
view_id: research-fs
driver: fs
disabled: false
config:
  root_path: "/data/fusion/research"
  create_dirs: true
  hot_file_threshold: 30
```

| Field | Description |
|-------|-------------|
| `view_id` | Unique identifier (must match request target) |
| `driver` | Storage driver (e.g., `fs`) |
| `config.root_path` | Physical path to store data |

---

## Agent Configuration

### 3. Source Configuration

Located at `$FUSTOR_HOME/sources-config.yaml`. Defines data sources the Agent monitors.

```yaml
fs-research:
  driver: fs
  uri: /data/research
  disabled: false
  driver_params:
    throttle_interval_sec: 1.0
```

### 4. Sender Configuration (Transport)

Located at `$FUSTOR_HOME/senders-config.yaml`. Defines where the Agent sends data.

```yaml
fusion-main:
  driver: http
  endpoint: http://localhost:8000
  credential:
    key: fk_secure_key_1
```

### 5. Pipeline Configuration (Tasks)

Located in `$FUSTOR_HOME/pipelines-config/*.yaml`. Defines the synchronization tasks.

**Example: `pipelines-config/pipe-research.yaml`**

```yaml
pipeline_id: pipe-research
source: fs-research
sender: fusion-main
disabled: false
audit_interval_sec: 600
fields_mapping:
  - source: ["path"]
    to: "path"
```

| Field | Description |
|-------|-------------|
| `pipeline_id` | Unique task identifier |
| `source` | ID from `sources-config.yaml` |
| `sender` | ID from `senders-config.yaml` |
| `fields_mapping` | (Optional) List of field transformations |

---

## Batch Operations

```bash
# Start all pipelines
fustor-agent start

# Stop all pipelines
fustor-agent stop

# Start all views/receivers
fustor-fusion start

# Stop all views/receivers
fustor-fusion stop
```