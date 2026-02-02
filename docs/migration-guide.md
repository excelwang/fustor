# Fustor V1 to V2 Configuration Migration Guide

This guide details how to migrate your compilation from Fustor V1 (Monolith) to Fustor V2 (Pipeline Architecture).

## Summary of Changes

| Concept | V1 Term | V2 Term | Configuration Location |
|---------|---------|---------|------------------------|
| **Data Source** | Datastore | View / Source | `views-config/*.yaml`, `agent-pipes-config/*.yaml` |
| **Ingress/Auth** | `datastores-config.yaml` | Receiver | `receivers-config.yaml` |
| **Agent Task** | Sync | Pipeline | `agent-pipes-config/*.yaml` |
| **Pusher** | Pusher | Sender | `senders-config.yaml` |

## Migration Steps

### 1. Migrate `datastores-config.yaml`

The `datastores-config.yaml` is **deprecated**. Its responsibilities have been split between **Receivers** (API/Auth) and **Views** (Storage).

#### Old Format (V1)
`datastores-config.yaml`:
```yaml
datastores:
  research-data:
    api_key: "fk_secret_key"
    session_timeout_seconds: 30
```

#### New Format (V2)

**Step A: Create `receivers-config.yaml`**
This defines the listening endpoints and authentication.

```yaml
receivers:
  default-http:
    driver: "http"
    port: 8000
    api_keys:
      "fk_secret_key":
        role: "admin"
        view_mappings:
          - "research-data"
```

**Step B: Create `views-config/research-data.yaml`**
This defines the backend storage logic for the view.

```yaml
view_id: research-data
driver: "fs"
config:
  root_path: "/data/research"
  create_dirs: true
```

### 2. Migrate Agent Configuration

- Rename `syncs-config/` to `agent-pipes-config/`.
- Update config files to use `pipeline_id` instead of `sync_id`.
- Ensure `sender` references a valid entries in `senders-config.yaml`.
- [Optional] Tune reliability parameters:
  ```yaml
  request_timeout_seconds: 30
  max_retries: 5
  ```

### 3. Migrate `pushers-config.yaml`

The `pushers-config.yaml` is now **deprecated** and replaced by `senders-config.yaml`. The term "Pusher" has been replaced with "Sender" to align with the symmetric architecture.

#### Old Format (V1)
`pushers-config.yaml`:
```yaml
fusion-http:
  driver: http
  endpoint: http://fusion.local:8102
  api_key: fk_research_key
```

#### New Format (V2)
`senders-config.yaml`:
```yaml
fusion-http:
  driver: http
  endpoint: http://fusion.local:8102
  credential:
    key: fk_research_key
```

### 4. Cleanup

After migration is complete and verified:
1.  Delete the old `datastores-config.yaml`.
2.  Delete the old `pushers-config.yaml`.
3.  Delete any other legacy files warned about in the logs (e.g., `ingest-config.yaml`).

## Common Pitfalls

- **API Path Changes**: Ensure the Sender is using `/api/v1/pipe/` prefix instead of `/api/v1/ingest/` if manually configured.
- **View Mappings**: If you migrate an API key in `receivers-config.yaml`, ensure the `view_mappings` list correctly references the IDs in `views-config/*.yaml`.
