# Fusion Management Module (V1 Archive)

This document describes the functionality of the `fustor-management` module that was removed from the Fusion service in v0.9.x. This serves as a reference for reimplementing or extending these features in future iterations.

## 1. Module Overview

The Management Module provided a centralized control plane for the Fustor cluster. It consisted of two parts:
1.  **Backend API (`fusion/api/management.py`)**: A set of REST endpoints integrated into the Fusion service.
2.  **Frontend UI (`management-ui/`)**: A static web interface for visualization and configuration.

### Key Responsibilities
*   **Cluster Monitoring**: Real-time dashboard of all connected Agents, active Pipes, and client Sessions.
*   **Configuration Management**: Centralized editing of `fusion.yaml` and remote `agent.yaml` configurations.
*   **Remote Command Dispatch**: Ability to send control commands (reload, upgrade, scan) to Agents via the existing heartbeat channel.

---

## 2. Core Features

### 2.1 Dashboard & Monitoring
The dashboard provided a real-time snapshot of the system state by querying the in-memory `SessionManager` and `PipeManager`.

*   **Agents**:
    *   List all connected Agents (grouped by `agent_id`).
    *   Show connection status (Active/Idle), Client IP, and Version.
    *   Track number of active sessions per agent.
*   **Pipes**:
    *   List all configured Fusion Pipes.
    *   Show state (`RUNNING`, `STOPPED`, `ERROR`).
    *   Show associated Views and Receivers.
*   **Sessions**:
    *   Detailed list of all active sessions (User reads & Agent pushes).
    *   Metrics: `age_seconds`, `idle_seconds`, `events_pushed`.
    *   **Role Tracking**: Identified which session was the **Leader** for a given View.

### 2.2 Configuration Management

#### Fusion Configuration
*   **Direct Edit**: Allowed editing of the `fusion.yaml` file on the server.
*   **Validation**: Implemented strict reference integrity checks:
    *   Pipes must reference valid Receivers and Views.
    *   Receivers must use valid Drivers.
*   **Hot Reload**: Triggered a process reload (via `SIGHUP`) after saving configuration.

#### Agent Configuration (Remote)
*   **Fetch**: Agents could be commanded to report their current configuration (`report_config`).
*   **Push Update**: Admins could push a new configuration to an Agent.
*   **Safety Constraints**:
    *   **Source/Sender Protection**: To prevent bricking an Agent, the API restricted modifications to `sources` and `senders` sections.
    *   **Pipe Management**: Allowed adding/removing Pipes dynamically.

### 2.3 Remote Agent Control
The module used the `SessionManager`'s command queue to send instructions to Agents. Commands were piggybacked on the Agent's heartbeat response.

**Supported Commands:**
*   `reload_config`: Force Agent to reload its configuration from disk.
*   `report_config`: Request Agent to upload its current YAML config.
*   `upgrade`: Trigger Agent self-update (with version argument).
*   `scan`: Trigger a file system scan (Snapshot) for a specific path.

---

## 3. Architecture & Data Flow

### 3.1 Command Queue Pattern
Since Agents sit behind firewalls (NAT), Fusion cannot initiate connections to them. The Management module used a **Reverse Command Pattern**:

1.  **User Action**: Admin clicks "Reload Agent" in UI.
2.  **Queue**: API adds a command object to `SessionInfo.pending_commands` in memory.
3.  **Heartbeat**: Agent sends a heartbeat (or data push) to Fusion.
4.  **Dispatch**: Fusion checks the queue and attaches the command to the HTTP response.
5.  **Execution**: Agent receives the response, extracts the command, and executes it locally.

### 3.2 Security
*   **Authentication**: All management endpoints were protected by a `X-Management-Key` header (HMAC validation against `fusion.yaml` config).
*   **Network**: The Management API ran on the same port as the Data API but under a distinct `/api/v1/management` path.

---

## 4. API Reference (Summary)

### Dashboard
*   `GET /api/v1/management/dashboard`: Full system state (Agents, Pipes, Views, Sessions).
*   `GET /api/v1/management/drivers`: List available drivers for all subsystems.

### Agent Control
*   `POST /api/v1/management/agents/{agent_id}/command`: Queue a command (reload, scan, upgrade).
*   `GET /api/v1/management/agents/{agent_id}/config`: Get cached Agent config.
*   `POST /api/v1/management/agents/{agent_id}/config`: Push new config (raw YAML).
*   `POST /api/v1/management/agents/{agent_id}/config/structured`: Push new config (JSON).

### Fusion Control
*   `GET /api/v1/management/config`: Get current `fusion.yaml`.
*   `POST /api/v1/management/config`: Update `fusion.yaml`.
*   `POST /api/v1/management/reload`: Trigger SIGHUP signal.

---

## 5. Known Limitations (Reasons for Removal)

1.  **State Ephemerality**: Agent configurations were cached in memory (in `SessionInfo`). Restarting Fusion meant losing the ability to view Agent configs until they re-reported.
2.  **Concurrency Risks**: Configuration updates (file writes) lacked locking or versioning, leading to potential race conditions.
3.  **Deployment Coupling**: The UI was bundled as a static asset within the Python package, making frontend updates difficult.
4.  **Security Model**: Shared API key was insufficient for granular access control.

## 6. Future Recommendations

For reimplementation, consider:
*   **Decoupled UI**: Build a standalone React/Vue app that talks to the API.
*   **State Persistence**: Store Agent configurations and Command history in a database (SQLite/Postgres).
*   **WebSocket Control**: Use WebSockets for real-time command delivery instead of HTTP heartbeat polling.
*   **OpLock**: Implement Optimistic Locking (ETag/Version) for configuration updates.
