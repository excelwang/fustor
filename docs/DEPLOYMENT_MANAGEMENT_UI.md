# Fustor 集中化部署与管理指南 (UI 中心版)

本指南介绍了如何利用 Fustor 集中管理控制台（Management Console）实现全网 Agent 的快速部署、远程配置、热升级及多级视图聚合。

## 1. 部署架构说明

本示例展示一个典型的生产级高可用与聚合配置：
*   **节点组成**：1 个 Fusion 核心节点，4 个 Agent 边缘节点（Agent-1~4）。
*   **逻辑拓扑**：
    *   `view-fs-1` (FS 视图)：负责监听并聚合 **Agent-1** 和 **Agent-2** 的数据。
    *   `view-fs-2` (FS 视图)：负责监听并聚合 **Agent-3** 和 **Agent-4** 的数据。
    *   `multi-view-all` (聚合视图)：将 `view-fs-1` 和 `view-fs-2` 合并为一个统一的逻辑根目录。

---

## 2. 基础服务启动

### 2.1 启动 Fusion 管理中心
在 Fusion 节点安装并启动服务。管理界面默认随服务开启：
```bash
pip install fustor-fusion
# 启动 Fusion 并开启管理 API (默认端口 8102)
fustor-fusion start -D
```
**访问地址**：`http://<fusion-ip>:8102/management`

### 2.2 启动 Agent 骨架
在所有 Agent 节点，只需安装基础包并确保定义了唯一的 `agent_id`：
```bash
pip install fustor-agent
# 修改 ~/.fustor/default.yaml，仅保留 agent_id 即可：
# agent_id: agent-1
fustor-agent start -D
```
*提示：此时 Agent 已连接 Fusion，但处于“空跑”状态，不执行任何同步。*

---

## 3. 集中化远程配置 (UI 操作)

打开 Fusion 管理界面，进入 **"Connected Agents"** 面板。此时你应该能看到已上线的 4 个 Agent。

### 3.1 批量注入 Agent 配置
无需逐台登录 Agent 节点，在 UI 上点击 **"Config"** 按钮即可完成下发：

1.  **配置 Agent-1 和 Agent-2**：
    粘贴以下 YAML 并点击 **"Push Config"**。Agent 将在下一次心跳（约 3s 内）接收配置并自动热加载：
    ```yaml
    agent_id: agent-1  # 对应节点 ID
    sources:
      src-1:
        driver: fs
        uri: "file:///data/research"
    senders:
      fusion-main:
        driver: http
        uri: "http://<fusion-ip>:8102"
        credential: {key: "your-api-key"}
    pipes:
      pipe-fs-1:  # Agent-1 和 Agent-2 必须使用相同的 pipe_id
        source: src-1
        sender: fusion-main
    ```

2.  **配置 Agent-3 和 Agent-4**：
    执行同样操作，但将 `pipes` 下的 ID 修改为 `pipe-fs-2`。

---

## 4. 视图聚合配置 (Fusion 侧)

在 Fusion 节点的 `~/.fustor/views-config/` 目录下创建以下配置，定义数据的呈现方式：

### 4.1 创建基础 FS 视图
*   **view-fs-1.yaml** (对应 Agent 1&2):
    ```yaml
    id: view-fs-1
    driver: fs
    driver_params: { root_path: "/mnt/fusion/view1" }
    ```
*   **view-fs-2.yaml** (对应 Agent 3&4):
    ```yaml
    id: view-fs-2
    driver: fs
    driver_params: { root_path: "/mnt/fusion/view2" }
    ```

### 4.2 创建 Multi-FS 聚合视图
*   **multi-view-all.yaml**:
    ```yaml
    id: multi-view-all
    driver: multi-fs
    driver_params:
      members: ["view-fs-1", "view-fs-2"]
    ```

**生效操作**：在管理界面点击右上角的 **"Reload Fusion"**。此时，用户访问 `multi-view-all` 即可同时看到 4 个 Agent 的数据。

---

## 5. 运维操作方法

### 5.1 远程版本升级 (Upgrade)
当 Fustor 发布新版本时：
1.  在 UI 的 **Connected Agents** 列表中找到目标 Agent。
2.  点击 **"Upgrade"** 按钮。
3.  输入目标版本号（例如 `0.9.1`）。
4.  **底层原理**：Agent 接收指令后会在 venv 中执行 `pip install` -> 子进程逻辑校验 -> `os.execv` 无缝替换当前进程。升级过程中同步任务会自动重连，无需人工干预。

### 5.2 远程配置热更新
如果需要更改 Agent 的采集路径或排除规则：
1.  点击对应 Agent 的 **"Config"**。
2.  在编辑器中修改 YAML 内容后 **"Push Config"**。
3.  点击该 Agent 的 **"Reload"** 按钮发送 SIGHUP 信号。Agent 将重新加载配置，当前同步 Session 不会中断。

---

## 6. 核心功能说明

### 6.1 On-Demand Scan (按需扫描)
除了自动的实时同步和周期审计，Fustor 支持通过 API 手动触发特定路径的深度扫描。

*   **触发方式**：
    使用 HTTP 请求 View API（带有 `force_scan=true` 参数）：
    ```bash
    GET /api/v1/views/view-fs-1/tree?path=/important/data&force_scan=true
    ```
*   **功能逻辑**：
    1.  Fusion 接收到请求后，通过心跳通道向该视图关联的所有 Agent 发送 `scan` 命令。
    2.  Agent 收到命令，立即对物理路径 `/important/data` 进行全量索引比对。
    3.  任何遗漏或不一致的数据将通过高优先级批次立即补推。
*   **适用场景**：手动拷贝了大量文件未触发 FS 事件、存储系统 Inotify 丢失事件、重要科研数据的一致性最终校验。

### 6.2 Multi-FS 聚合逻辑
*   **逻辑合并**：`multi-view-all` 作为一个虚拟层，它会并行请求成员视图（`view-fs-1`, `view-fs-2`）的元数据。
*   **透明访问**：对于最终用户（如通过 NFS 或 Web 访问），数据仿佛存储在一个巨大的单机文件系统中，无需感知底层的 Agent 分布。
*   **故障隔离**：如果其中一个视图（或 Agent）离线，Multi-View 依然能返回其余正常视图的数据，并标记故障部分。
