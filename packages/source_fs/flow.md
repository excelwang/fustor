# NFS监控事件流处理流程图

```mermaid
flowchart TD
    A[NFS Server<br/>(10M files)] --> B{File System<br/>Change?}
    B -->|Yes| C[Server A<br/>fuagent-1]
    B -->|Yes| D[Server B<br/>fuagent-2]
    C --> E[FSDriver<br/>detects change]
    D --> F[FSDriver<br/>detects change]
    E --> G[Convert to Event<br/>(path, type, metadata)]
    F --> H[Convert to Event<br/>(path, type, metadata)]
    G --> I[Send Event Stream<br/>Server A events]
    H --> J[Send Event Stream<br/>Server B events]
    I --> K[Consumer<br/>Aggregator]
    J --> K
    K --> L[Event Deduplication<br/>by ID/Timestamp]
    L --> M[Merge events<br/>from both sources]
    M --> N[Update Directory<br/>Tree View]
    N --> O[Verify<br/>Consistency]
    O --> P[Provides consistent<br/>directory view API]
```