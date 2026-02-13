# Control/Data Plane Separation

> Version: 1.0.0
> Date: 2026-02-13
> Status: Draft

## 1. Overview

Currently, Fustor Agents are configured via local YAML files. This decentralization makes managing a fleet of agents (e.g., 20+ NFS nodes) difficult. Updating a collection policy requires SSH-ing into every node.

This document proposes a **Control/Data Plane Separation** architecture where:
- **Fusion** acts as the **Control Plane**, hosting and serving configurations for all Agents.
- **Agents** act as the **Data Plane**, pulling their configuration from Fusion and executing data pipelines.

## 2. Architecture

### 2.1 Current State (Decentralized)

```mermaid
graph LR
    User -->|Edit YAML| AgentA[Agent A (Local Config)]
    User -->|Edit YAML| AgentB[Agent B (Local Config)]
    AgentA -->|Data| Fusion
    AgentB -->|Data| Fusion
```

### 2.2 Target State (Centralized)

```mermaid
graph TB
    User -->|API/UI| Fusion[Fusion (Control Plane)]
    Fusion -->|Config (Push/Poll)| AgentA[Agent A]
    Fusion -->|Config (Push/Poll)| AgentB[Agent B]
    AgentA -->|Data| Fusion
    AgentB -->|Data| Fusion
```

## 3. Detailed Design

### 3.1 Fusion Control Plane

Fusion will expose a new set of APIs for configuration management.

#### 3.1.1 Config Storage
Fusion needs a backend to store Agent configurations.
- **MVP**: File-based storage (`$FUSTOR_HOME/fusion-configs/agents/{agent_id}.yaml`).
- **Future**: Database (SQLite/PostgreSQL).

#### 3.1.2 Management API (Admin)

```http
POST /api/v1/control/configs/{agent_id}
Content-Type: application/yaml
Body: <Full Agent Config>
```

```http
GET /api/v1/control/configs/{agent_id}
```

#### 3.1.3 Agent Polling API (Agent-facing)

Agents poll this endpoint to check for updates.

```http
GET /api/v1/control/configs/pull?agent_id={agent_id}&current_signature={hash}
```

- **Headers**: `Authorization: Bearer <Enrollment-Key>`
- **Response**:
    - `304 Not Modified`: If `current_signature` matches server version.
    - `200 OK`: If config changed. Body contains full YAML/JSON and new `X-Config-Signature`.

### 3.2 Agent Remote Provider

The Agent's `ConfigLoader` will be extended to support a `RemoteProvider`.

#### 3.2.1 Startup Flow
1. **Bootstrap**: Agent starts with a minimal `bootstrap.yaml` containing:
    - `agent_id`
    - `control_plane_uri` (e.g., `http://fusion:8102`)
    - `auth_token`
2. **Fetch**: Agent requests config from Control Plane.
3. **Apply**: Agent applies config (starts Pipes/Sources).

#### 3.2.2 Hot Reload Loop
1. Agent runs a background task (e.g., every 30s).
2. Polls Fusion with current config hash.
3. If changed, triggers `reload()` mechanism (existing logic in `App.reload_config`).

## 4. Security & Enrollment

- **Enrollment Key**: A shared secret or token used by Agents to authenticate with the Control Plane initially.
- **mTLS**: Recommended for production to secure the command channel.

## 5. Implementation Plan

### Phase 1: Config Signature & Hot Reload (Prerequisite)
- [ ] Implement `ConfigSignature` calculation in `AgentConfigLoader`.
- [ ] Refactor `App.reload_config` to use signature comparison instead of just checking file mtime/diff.

### Phase 2: Fusion Config API
- [ ] Implement `ConfigStore` in Fusion.
- [ ] Expose `/api/v1/control` endpoints.

### Phase 3: Agent Remote Polling
- [ ] Create `RemoteConfigProvider` in Agent.
- [ ] Implement Polling Loop in Agent `App`.

## 6. Migration

To support gradual migration, Agent should support hybrid mode:
- If `control_plane_uri` is set, try remote fetch.
- Fallback to local `agent-config/*.yaml` if remote is unreachable or not configured.
