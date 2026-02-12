# Fustor 2.0 Architecture Diagram (Control/Data Plane Split)

This diagram illustrates the separation of concerns between the **Control Plane** (Management) and **Data Plane** (Execution).

```mermaid
graph TD
    %% ---------------------------------------------------------
    %% Actors & External Systems
    %% ---------------------------------------------------------
    User((Ops / Admin))
    Git[(Git Config Repo\n"Source of Truth")]
    
    %% ---------------------------------------------------------
    %% Control Plane
    %% ---------------------------------------------------------
    subgraph Control_Plane ["Control Plane (Fusion Controller)"]
        direction TB
        API[Management API]
        ConfigMgr[Config Manager]
        HealthMgr[Health Monitor]
        Orch[Orchestrator]
        
        API --> ConfigMgr
        API --> HealthMgr
        ConfigMgr -- Pull --> Git
        ConfigMgr --> Orch
    end
    
    %% ---------------------------------------------------------
    %% Data Plane (Edge / Agent)
    %% ---------------------------------------------------------
    subgraph Agent_Side ["Data Plane (Edge)"]
        Agent[**Fustor Agent**\nDumb Execution Unit]
        Source[Data Source\n(Ext4/S3/DB)]
        
        Source -- Read --> Agent
    end
    
    %% ---------------------------------------------------------
    %% Data Plane (Core / Fusion)
    %% ---------------------------------------------------------
    subgraph Fusion_Side ["Data Plane (Core)"]
        Fusion[**Fusion Gateway**\nHigh Throughput IO]
        View[(Unified View)]
        
        Fusion -- Write --> View
    end
    
    %% ---------------------------------------------------------
    %% Interactions
    %% ---------------------------------------------------------
    
    %% Management Flows
    User -- "1. Update Config" --> Git
    User -- "2. Trigger Reload /\nView Status" --> API
    
    %% Control Flows (RPC / HTTP)
    Orch -- "3. Push Manifest\n(What to run)" --> Agent
    Orch -- "3. Push Topology\n(How to merge)" --> Fusion
    
    Agent -- "4. Heartbeat &\nMetrics" --> HealthMgr
    Fusion -- "4. Session State &\nHealth" --> HealthMgr
    
    %% Data Pipeline (High Bandwidth)
    Agent == "5. Data Stream\n(Events / Chunks)" ==> Fusion
    
    %% Styling
    classDef control fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef data fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
    classDef ext fill:#fff3e0,stroke:#ef6c00,stroke-width:2px;
    
    class Control_Plane,API,ConfigMgr,HealthMgr,Orch control;
    class Agent_Side,Agent,Fusion_Side,Fusion data;
    class User,Git,Source,View ext;
```

## Key Components

1.  **Fusion Controller (Control Plane)**
    *   **Single Source of Truth**: Fetches configuration from Git.
    *   **Stateful Manager**: Maintains the "Desired State" vs "Actual State" of the cluster.
    *   **Endpoint**: `fustor-controller` (New binary).

2.  **Fustor Agent (Data Plane)**
    *   **Stateless**: Starts with only Controller URL and Identity.
    *   **Dynamic**: Receives "Pipe Manifest" from Controller.
    *   **Focus**: Pure data reading and forwarding.

3.  **Fusion Gateway (Data Plane)**
    *   **Focus**: High-performance event processing and view materialization.
    *   **Decoupled**: Does not manage Agent lifecycle directly anymore.
