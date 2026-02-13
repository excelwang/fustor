# Fustor Fusion 用户操作手册 (API 使用指南)

欢迎使用 Fustor Fusion 存储引擎。本手册旨在详细介绍面向用户的 API 接口及其使用方法，帮助您快速集成和管理文件系统视图。

---

## 1. 核心概念与架构简述

Fustor 将后端存储（如 NFS, S3, Local FS）抽象为 **View (视图)**。所有的用户查询（查询树、搜索、元数据读取）都通过特定的 `view_id` 进行隔离。

- **Fusion**: 中央协调与索引引擎。
- **Agent**: 部署在各存储节点的代理，通过 **Session (会话)** 与 Fusion 通信。
- **View ID**: 标识一个独立的文件索引命名空间。
- **API Key**: 用于鉴权，通常一个 Key 绑定一个或多个 View。

---

## 2. 身份验证与视图发现

在使用任何 API 之前，您需要有一个 `X-API-Key` 并在请求头中携带。

### 视图发现 (Common Entry)
如果您不确定当前的 API Key 对应哪个 `view_id`，可以调用此接口：

- **接口**: `GET /api/v1/pipe/session/`
- **请求头**: `X-API-Key: <YOUR_KEY>`
- **作用**: 自动通过 API Key 识别绑定的视图。
- **示例响应**:
  ```json
  {
    "view_id": "my-nfs-view",
    "status": "authorized"
  }
  ```

---

## 3. 视图查询接口 (User-Facing)

以下接口均位于 `/api/v1/views/{view_id}/` 路径下，用于消费 Fustor 维护的全局一致性索引。

### 3.1 获取目录树 (`/tree`)
最常用的接口，用于展示文件系统结构。

- **接口**: `GET /api/v1/views/{view_id}/tree`
- **参数**:
  - `path`: (string) 目标路径，默认 `/`。
  - `recursive`: (bool) 是否递归获取子目录，默认 `true`。
  - `max_depth`: (int) 递归深度。
  - `only_path`: (bool) 若为 `true`，仅返回目录结构和文件名，不含大小/时间等元数据（轻量级）。
  - `on_demand_scan`: (bool) 若为 `true`，且当前路径在视图中不存在，触发 Agent 端进行按需扫描（补偿机制）。
- **示例**: `curl -H "X-API-Key: ..." "http://fusion:8102/api/v1/views/my-view/tree?path=/data&max_depth=2"`

#### 响应结构示例
```json
{
  "name": "data",
  "content_type": "directory",
  "path": "/data",
  "modified_time": 1700001234.56,
  "children": [
    {
      "name": "config.yaml",
      "content_type": "file",
      "size": 1024,
      "integrity_suspect": false
    }
  ]
}
```

### 3.2 搜索文件 (`/search`)
基于文件名的快速搜索。

- **接口**: `GET /api/v1/views/{view_id}/search`
- **参数**: `query`: (string) 搜索模式，支持标准 Glob 通配符 (`*`, `?`, `[]`, `**`)。
- **示例**: 
  - 精确搜索: `GET /api/v1/views/my-view/search?query=backup.log`
  - 通配符搜索: `GET /api/v1/views/my-view/search?query=*.log`
  - 递归搜索: `GET /api/v1/views/my-view/search?query=**/config.yaml`

### 3.3 获取元数据 (`/metadata`)
获取单个文件的详细属性（大小、修改时间、一致性标志）。

- **接口**: `GET /api/v1/views/{view_id}/metadata`
- **参数**: `path`: (string) 必填。
- **亮点**: 
  - `integrity_suspect`: 若为 `true`，表示该文件正处于“温吞态”（可能正在被写入或在盲区被修改），建议稍后重试。
  - `audit_skipped`: 若为 `true`，表示该目录在最近一次审计中被跳过（可能因为权限问题或文件系统错误），其内容可能不完整。

### 3.4 视图健康统计 (`/stats`)
监控视图的同步进度、延迟和健康度。

- **接口**: `GET /api/v1/views/{view_id}/stats`
- **关键性能指标 (KPI)**:
  - `latency_ms`: 事件处理延迟。
  - `staleness_seconds`: 视图数据与真实物理时间的逻辑偏差（通常应 < 5s）。
  - `suspect_file_count`: 当前未确认为最终一致的文件数量。
  - `audit_cycle_count`: 已完成的审计扫描轮数。

---

## 4. 管理与监控接口

### 4.1 会话管理 (`/sessions`)
查看哪些 Agent 正在向本视图推送数据。

- **接口**: `GET /api/v1/views/{view_id}/sessions`
- **输出**:
  - `active_sessions`: 包含 Agent ID、IP、角色（Leader/Follower）及权限。
- **作用**: 确认集群选主是否正常，是否有掉线节点。

### 4.2 任务列表 (`/jobs`)
查看 Agent 当前正在执行的后台任务（如大规模审计扫描）。

- **列出视图任务**: `GET /api/v1/views/{view_id}/jobs`
- **查询任务详情**: `GET /api/v1/views/{view_id}/jobs/{job_id}`
  - 获取任务进度百分比及各 Agent 的完成情况。

### 4.3 视图驱动状态 (Management)
对于高级管理员，Fusion 提供了查询当前加载的视图驱动状态的接口。

- **列出所有运行中的视图驱动**: `GET /api/v1/management/views`

---

## 5. 开发者高级接口 (Pipe Ingest)

如果您正在开发自定义 Agent 或 Sender，以下接口供推流使用：

- **创建会话**: `POST /api/v1/pipe/session/` (分配 UUID 并进行选主)。
- **推送事件**: `POST /api/v1/pipe/ingest/{session_id}/events` (高频推流核心)。
- **一致性审计**: `POST /api/v1/pipe/consistency/audit/start` (告知 Fusion 开始一轮物理扫描)。

---

## 6. 常见问题 (FAQ)

**Q: 为什么我看到的文件状态是 `integrity_suspect=true`？**
A: 这表示 Fustor 发现该文件在非受控环境（如直接在 NFS 下修改）发生了变动，或者正在被写入。Fusion 正在等待审计扫描或 Realtime 事件来校对它的最终属性。

**Q: 创建会话时返回 409 Conflict？**
A: 这表示该视图设置了严格的并发控制 (`allow_concurrent_push: false`)，且当前已有其他 Agent (Session) 正在持有该视图的锁。请等待现有会话结束或手动终止它。

**Q: 查询接口会发生阻塞吗？**
A: 不会。所有的查询都是基于内存索引的，具有极高的响应速度。

**Q: API Key 权限不足怎么办？**
A: 请检查 Fusion 配置文件 `default.yaml` 中的 `api_keys` 部分，确保对应的 Key 已绑定到目标 `pipe_id` 或 `view_id`。

---
*文档版本：v1.0.1 | 更新日期：2026-02-11*
