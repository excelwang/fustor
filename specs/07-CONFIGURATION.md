# Configuration Reference (V2)

> **Status**: Draft
> **Version**: 2.0

## 1. Agent Configuration

Located in `config/agent/`.

### 1.1 Sources (`sources-config.yaml`)

Defines data sources.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `driver` | enum | required | `fs`, `oss` |
| `uri` | string | required | Root path or URI |
| `credential` | dict | {} | Credentials if needed |
| `driver_params` | dict | {} | Driver-specific settings |

**Driver Params (fs)**:
- `throttle_interval_sec`: Inotify event throttle (default: 5.0)
- `max_scan_workers`: Thread pool size for scanning (default: CPU cores)
- `min_monitoring_window_days`: Time window for hot set (default: 30)

### 1.2 Senders (`senders-config.yaml`)

Defines transport destinations.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `driver` | enum | required | `http`, `grpc` |
| `endpoint` | string | required | Fusion URL |
| `credential.key` | string | required | API Key |
| `driver_params` | dict | {} | Transport tuning |

**Driver Params (http)**:
- `batch_size`: Max events per request (default: 100)
- `timeout`: Request timeout (default: 30s)

### 1.3 Pipelines (`agent-pipes-config/*.yaml`)

Binds Source to Sender.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `id` | string | required | Pipeline ID |
| `source` | string | required | Source ID reference |
| `sender` | string | required | Sender ID reference |
| `heartbeat_interval_sec` | int | 10 | Heartbeat frequency |
| `audit_interval_sec` | int | 600 | Full scan frequency (0=disable) |
| `sentinel_interval_sec` | int | 120 | Sentinel check frequency |

---

## 2. Fusion Configuration

Located in `config/fusion/`.

### 2.1 Receivers (`receivers-config.yaml`)

Defines listening ports.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `driver` | enum | required | `http`, `grpc` |
| `port` | int | 8101 | Listening port |
| `api_keys` | list | [] | List of valid keys |

### 2.2 Views (`views-config/*.yaml`)

Defines data views.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `driver` | enum | required | `fs` |
| `hot_file_threshold` | float | 30.0 | Seconds to consider file "hot" |
| `live_mode` | bool | false | If true, clear view when sessions close |

### 2.3 Pipelines (`fusion-pipes-config/*.yaml`)

Binds Receiver to Views.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `id` | string | required | Pipeline ID |
| `receiver` | string | required | Receiver ID reference |
| `views` | list | required | List of View IDs to feed |
| `session_timeout_seconds` | int | 30 | Session expiration time |
