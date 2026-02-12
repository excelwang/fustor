# Agent Management Plane Design

## 1. Goal
Provide a centralized way to manage Agent configuration, state monitoring, and software upgrades from the Fusion server.

## 2. Architecture

### 2.1 Centralized Configuration
- **Agent Side**:
    - Extend `AgentConfigLoader` to support multiple providers (File, Remote).
    - Implement `RemoteConfigProvider` that polls Fusion for configuration.
    - Uses "Configuration Signature" (hash of config) to detect changes and trigger Hot Reload.
- **Fusion Side**:
    - New API: `GET /api/v1/management/config/{agent_id}`.
    - Configuration storage backend (DB or Git-backed).

### 2.2 Centralized State Monitoring
- **Agent Side**:
    - Enhance existing Heartbeat to include system metrics (CPU/RAM), version info, and detailed pipe status.
- **Fusion Side**:
    - Dashboard API: `GET /api/v1/management/agents`.
    - Alerting integration based on agent health.

### 2.3 Software Upgrade (Self-Updater)
- **Agent Side**:
    - Implement a `SelfUpdater` service.
    - Listens for `UPGRADE` command from Fusion (via Heartbeat response or separate long-poll).
    - Supports pluggable strategies:
        - `Docker`: Pull new image and restart container.
        - `Git`: `git pull` && restart service.
        - `Pip`: `pip install -U fustor-agent` && restart.
- **Fusion Side**:
    - API to trigger upgrade for specific agents or groups.

## 3. Implementation Steps
- [ ] Implement "Configuration Signature" Hot Reload (Simpler prerequisite).
- [ ] Design and implement Fusion Management API (Config + State).
- [ ] Implement `RemoteConfigProvider` in Agent.
- [ ] Implement `SelfUpdater` service in Agent.
