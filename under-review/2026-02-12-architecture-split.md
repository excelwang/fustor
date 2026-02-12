# Architecture Proposal: Control Plane vs. Data Plane Separation

## 1. Context
Currently, Fustor's architecture couples Control Plane (Management) and Data Plane (IO/Processing) logic within the **Agent** and **Fusion** binaries.
- **Agent**: Handles Config Loading (Control) + Source Reading (Data).
- **Fusion**: Handles Session/Topology (Control) + View/Consistency (Data).

As we move towards centralized management, this coupling becomes a bottleneck for scalability, security, and stability.

## 2. Proposed Architecture

We propose explicitly separating the system into **Control Plane** and **Data Plane**.

```mermaid
graph TD
    subgraph "Control Plane (Low Volume, High Criticality)"
        C[Fusion Controller]
        DB[(Config/State DB)]
        Git[GitOps Repo]
    end

    subgraph "Data Plane (High Volume, High Throughput)"
        A[Agent (Data Forwarder)]
        F[Fusion (IO Gateway)]
    end

    C -->|Config/Commands| A
    C -->|Config/Topology| F
    A -->|Heartbeat/Metrics| C
    F -->|Session State| C
    
    A ==Data Stream==> F
```

### 2.1 The Fusion Controller (New Component)
A lightweight service dedicated to cluster management.
- **Responsibilities**:
    - **Configuration Authority**: Stores and serves config (from Git or DB).
    - **Identity & Auth**: Issues certificates/tokens to Agents.
    - **Orchestration**: Assigns tasks (Pipes) to Agents.
    - **Monitoring**: Aggregates heartbeats and health status.
    - **Upgrade Manager**: Coordinates rolling updates.
- **Characteristics**: Stateless (mostly), highly available, low bandwidth.

### 2.2 The "Dumb" Agent
Refactor Agent to be a pure execution unit.
- **No Local Config**: Starts with just identity and Controller URL.
- **Bootstrap**: Connects to Controller, downloads "Manifest" (What pipes to run).
- **Execution**: Runs the Pipes defined in the Manifest.
- **Hot Reload**: Implicit handling of Manifest updates pushed by Controller.

### 2.3 The "Pure" Fusion Gateway
Refactor Fusion Node to focus on IO.
- **View Serving**: Optimized for FS operations and consistency.
- **Cluster State**: Offloads global topology management to Controller (or shared state via Etcd/Redis).

## 3. Benefits

| Dimension | Current (Coupled) | Proposed (Split) |
| :--- | :--- | :--- |
| **Upgrade Safety** | Management logic update requires Data Plane restart. | Update Management (Controller) without touching Data Plane. |
| **Stability** | Config parsing/management crash kills Data Pipe. | Control plane crash doesn't stop data flow (cached config). |
| **Security** | Agent needs access to all secrets in local config. | Agent only receives specific secrets for assigned tasks. |
| **Scalability** | Fusion Node handles both IO and Cluster State. | Fusion focuses on IO; Controller scales independently. |

## 4. Migration Strategy

1.  **Phase 1 (Hybrid)**: Implement "Remote Config Provider" in Agent. Agent can still run with local files (Legacy) or connect to a Controller Mock (New).
2.  **Phase 2 (Controller)**: Build `fustor-controller` as a separate binary (extracting SessionManager/Config logic).
3.  **Phase 3 (Switch)**: Default to using Controller.

## 5. Recommendation
**Adopt the Split.** It is the standard pattern for distributed systems (K8s, Istio) and essential for "Centralized Management" requirements.
