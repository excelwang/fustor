# Fustor 高级部署指南 (FS Scenario - Advanced)

本文档面向高级用户和系统架构师，详细介绍 Fustor 在复杂场景下的部署模式与性能调优策略。

## 1. 部署模式详解

Fustor 支持灵活的部署架构，以适应不同的业务需求（如单点备份、高可用集群、多源聚合）。

### 1.1 单机模式 (Standalone Mode)
**适用场景**：个人开发、测试环境、非关键业务的小型文件同步。

*   **架构**：1 个 Agent -> 1 个 Fusion
*   **配置要点**：
    *   Agent 与 Fusion 可部署在同一台机器或不同机器。
    *   无需配置复杂的选主策略。

### 1.2 高可用集群模式 (High Availability / HA Mode)
**适用场景**：生产环境的关键业务，要求服务不中断 (Zero Downtime)。

*   **架构**：N 个 Agent (监控同一存储) -> 1 个 Fusion (集群)
*   **工作原理**：
    *   多个 Agent 均挂载相同的后端存储 (如 NFS/Ceph/NAS)。
    *   Agent 之间自动通过 Fusion 进行协调，选举出一个 **Leader** (负责全量扫描与审计) 和多个 **Follower** (负责实时监听与冗余备份)。
    *   当 Leader 宕机时，Follower 会在秒级内自动接管，成为新的 Leader。
*   **配置要点**：
    *   所有 Agent 的 `source.uri` 必须指向逻辑上相同的文件系统内容 (即使挂载路径不同，Fustor 也能通过相对路径归一化处理)。
    *   建议配置 3 个以上的 Agent 节点以防止脑裂 (Split-Brain) 并提供更高的容错性。

### 1.3 聚合模式 (Aggregation Mode)
**适用场景**：统一数据湖视图、多部门数据汇总。

*   **架构**：多个 Agent (监控不同存储) -> 1 个 Fusion (统一视图)
*   **工作原理**：
    *   Agent A 监控 `/data/department_a`
    *   Agent B 监控 `/data/department_b`
    *   它们都向同一个 Fusion View 推送数据。
*   **结果**：
    *   Fusion 的视图中将同时包含来自 Department A 和 Department B 的文件，形成一个统一的全局命名空间。
    *   **注意**：需确保不同源的文件路径不冲突，或者接受“后到覆盖”的合并策略。

### 1.4 分发模式 (Fan-Out Mode)
**适用场景**：一处采集，多处使用（如同时用于实时大屏展示和长期冷备）。

*   **架构**：1 个 Agent -> 1 个 Fusion (Pipe) -> N 个 Views
*   **工作原理**：
    *   Agent 采集数据并通过 Pipe 发送到 Fusion。
    *   在 Fusion 的 `pipes` 配置中，将单个 Pipe 关联到多个 `views`。
    *   数据会被复制并分发到所有关联的视图中。
*   **配置示例 (Fusion)**:
    ```yaml
    pipes:
      ingest-pipe:
        receiver: http-receiver
        views:
          - realtime-view  # 用于高性能查询 (内存限制较小)
          - archive-view   # 用于全量备份 (内存限制较大)
        allow_concurrent_push: true # 推荐开启并发推送
    ```

## 2. 性能调优与限制 (Performance Tuning)

在处理海量文件 (百万级/千万级) 场景时，需注意以下参数调整。

### 2.1 内存与元数据限制 (max_tree_items)
*   **背景**：Fusion 的 `view-fs` 驱动将所有文件元数据存储在内存中。如果文件数量极其庞大 (如 > 1000 万)，可能会耗尽服务器内存或导致 API 响应极慢。
*   **解决方案**：使用 `max_tree_items` 参数限制单次查询返回的条目数。
*   **配置示例 (Fusion)**:
    ```yaml
    views:
      massive-view:
        driver: fs
        driver_params:
          # 限制单次 API 响应最多返回 50 万个条目
          # 超过此限制时，API 将返回 400 错误，提示用户缩小查询范围 (如查询特定子目录)
          max_tree_items: 500000 
          hot_file_threshold: 60.0 # [默认 30.0]
    ```

### 2.2 内核参数调优 (Inotify)
*   **背景**：Linux 默认的 `inotify` 句柄通过 `max_user_watches` 限制。默认值 (8192) 对于生产环境远远不够。
*   **建议值**：
    *   **百万级文件**：设置 `fs.inotify.max_user_watches = 1048576`
    *   **千万级文件**：设置 `fs.inotify.max_user_watches = 10485760`
*   **配置方法**：
    ```bash
    echo "fs.inotify.max_user_watches=10485760" | sudo tee -a /etc/sysctl.conf
    sudo sysctl -p
    ```

### 2.3 网络与并发 (Pipe Tuning)
*   **allow_concurrent_push**:
    *   在 Fusion `pipes` 配置中设置 `allow_concurrent_push: true` (默认即为 true)。
    *   允许 Agent 并发推送事件，极大提高大量小文件变更时的同步吞吐量。
*   **session_timeout_seconds**:
    *   建议设置为 `3600` (1小时) 或更长。
    *   防止网络短暂波动导致 Agent 被误判下线，从而触发不必要的重新全量扫描。

### 2.4 配置热管理 (Reload)
在集群模式下，频繁重启服务会带来不必要的扫描。利用 `reload` 命令可以实现“零停机”更新：
- **调整日志级别**: 集群出现异常时，可通过重载将 `logging` 设为 `"DEBUG"`。
- **增量新增管道**: 在不影响现有同步流的情况下，通过更新 YAML 并 `fustor-fusion reload` 来新增接收视图。

---

## 3. 安全建议

*   **API Key 隔离**：为不同的 Pipe 分配不同的 API Key，确保业务隔离。
*   **网络隔离**：Fusion 服务应部署在内网，避免 API 端口直接暴露在公网，除非配合 Nginx 反向代理与 HTTPS 加密。
*   **热更新密钥**: 使用 `reload` 功能可以周期性更换 API Key 而无需中断同步会话。
