# Fustor 一致性设计方案

## 1. 概述

### 1.1 目标场景

多台计算服务器通过 NFS 挂载同一共享目录。部分服务器部署了 Agent，部分没有（盲区）。

```
                    ┌─────────────────────────────┐
                    │      Fusion Server          │
                    │   (中央数据视图 + 一致性裁决)   │
                    └──────────────┬──────────────┘
                                   │
          ┌────────────────────────┼────────────────────────┐
          │                        │                        │
    ┌─────▼─────┐            ┌─────▼─────┐            ┌─────▼─────┐
    │ Server A  │            │ Server B  │            │ Server C  │
    │  Agent ✅ │            │  Agent ✅ │            │  Agent ❌ │
    └─────┬─────┘            └─────┬─────┘            └─────┬─────┘
          │                        │                        │
          └────────────────────────┼────────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │     NFS / 共享存储           │
                    └─────────────────────────────┘
```

### 1.2 核心挑战

| 挑战 | 描述 |
|------|------|
| **inotify 本地性** | Agent 只能感知本机发起的文件操作 |
| **NFS 缓存滞后** | 不同客户端看到的目录状态可能有秒级甚至分钟级的延迟 |
| **感知盲区** | 没有部署 Agent 的节点产生的文件变更无法实时感知 |
| **时序冲突** | 快照/审计扫描期间发生的实时变更可能导致数据矛盾 |

### 1.3 设计目标

| 目标 | 描述 |
|------|------|
| **实时性优先** | Realtime 消息具有最高优先级 |
| **盲区可发现** | 通过定时审计发现盲区变更，并明确标记 |
| **视图即真相** | Fusion 内存树是经过仲裁后的最终状态 |
| **IO 可控** | 只有 Leader Agent 执行 Snapshot/Audit |

### 1.4 Path Normalization Contract (路径归一化契约)

为确保多 Agent (尤其是 Shared Storage 场景) 视图的一致合并，所有 Source Driver **必须** 遵循以下路径生成规则：
1.  **Relative Path**: 输出路径必须相对于配置的 `root_path` / `uri`。
2.  **Leading Slash**: 归一化后的路径必须以 `/` 开头 (例如 `/foo/bar.txt`)。
    - Agent A (`/mnt/data`) 和 Agent B (`/home/user/share`) 监控同一 NFS 目录时，必须都能生成相同的 `/foo/bar.txt` 键值。
3.  **Consistency**: Realtime, Snapshot, Audit 三种模式生成的路径必须完全一致。

---

## 2. 架构：Leader/Follower 模式

### 2.1 角色定义

| 角色 | Realtime Sync | Snapshot Sync | Audit Sync | Sentinel Sweep |
|------|---------------|---------------|------------|----------------|
| **Leader** | ✅ | ✅ | ✅ | ✅ |
| **Follower** | ✅ | ❌ | ❌ | ❌ |

### 2.2 Leader 选举

- **先到先得**：第一个建立 Session 的 Agent 成为 Leader
- **故障转移**：仅当 Leader 心跳超时或断开后，Fusion 才释放 Leader 锁
- **实现**：通过 `ViewStateManager` 管理 Leader 锁，`SessionManager` 管理会话生命周期

---

## 3. 消息类型

Agent 向 Fusion 发送的消息分为三类，通过 `message_source` 字段区分：

| 类型 | 来源 | 说明 |
|------|------|------|
| `realtime` | inotify 事件 | 单个文件的增删改，优先级最高 |
| `snapshot` | Agent 启动时全量扫描 | 初始化内存树 |
| `audit` | 定时审计扫描 | 发现盲区变更 |

### 3.1 Audit 快速扫描算法 (Agent 端)

利用 POSIX 语义：创建/删除文件只更新**直接父目录**的 mtime。

实现特点（`source-fs` 驱动）：
- **并行扫描**：使用线程池并行处理子目录
- **"真正的静默" (True Silence)**：目录 mtime 与缓存一致时，跳过文件扫描，但仍递归检查子目录
- **审计跳过标记**：静默目录发送 `audit_skipped=True` 标记，保护子项不被误删

```python
def audit_worker():
    current_dir_mtime = os.stat(root).st_mtime
    cached_mtime = mtime_cache.get(root)
    is_silent = cached_mtime is not None and cached_mtime == current_dir_mtime
    
    if is_silent:
        # 静默目录：发送带 audit_skipped=True 的事件，保护子项
        send_audit_event(root, audit_skipped=True)
    else:
        # 目录变化：完整扫描所有子文件
        for child in os.scandir(root):
            if child.is_file():
                send_audit_event(child.path, parent_path=root, parent_mtime=current_dir_mtime)
    
    # 始终递归检查子目录（即使静默）
    for subdir in subdirs:
        work_queue.put(subdir)
```

### 3.2 Audit 消息格式

Audit 消息复用标准 Event 结构，包含以下额外信息：

```json
{
  "message_source": "audit",
  "event_type": "UPDATE",
  "index": 1706000000000,
  "rows": [
    {
      "path": "/data/file.txt",
      "modified_time": 1706000123.0,
      "size": 10240,
      "parent_path": "/data",
      "parent_mtime": 1706000100.0,
      "audit_skipped": false
    }
  ]
}
```

**关键字段**：
- `parent_path` / `parent_mtime`: 用于 Parent Mtime Check (Rule 3)
- `audit_skipped`: 标记目录是否因 mtime 未变而跳过扫描
- `index`: 物理采集时刻（毫秒级 Unix 时间戳）

---

## 4. 状态管理

Fusion 通过 `FSState` 类维护以下状态：

### 4.1 内存树 (Memory Tree)

存储文件/目录的元数据，每个节点（`DirectoryNode` / `FileNode`）包含：

| 字段 | 类型 | 描述 |
|------|------|------|
| `path` | `str` | 文件绝对路径 |
| `modified_time` | `float` | 最后修改时间（来自存储系统的 mtime） |
| `size` | `int` | 文件大小（字节） |
| `last_updated_at` | `float` | Fusion 本地物理时间戳，记录最后确认时刻 |
| `integrity_suspect` | `bool` | 是否为可疑热文件 (由原子写标记或时效判定) |
| `known_by_agent` | `bool` | 是否被 Realtime 事件确认 |
| `audit_skipped` | `bool` | (仅目录) 是否在审计中因静默被跳过 |

### 4.2 墓碑表 (Tombstone List)

- **用途**：记录被 Realtime 删除的文件，防止滞后的 Snapshot/Audit 使其复活
- **结构**：`Dict[Path, Tuple[LogicalTime, PhysicalTime]]`
  - `LogicalTime`: 删除时刻的逻辑时间戳（用于转世判定）
  - `PhysicalTime`: 删除时刻的物理时间戳（用于 TTL 清理）
- **生命周期**：
  - **创建**：处理 Realtime Delete 时，记录双时间戳
  - **即时清除**：当更新的事件满足 `mtime > tombstone_logical_ts` 时，Tombstone 被清除（文件转世 Reincarnation）
  - **TTL 清理**：Audit-End 时清理 `physical_ts > 1 hour` 的过期墓碑

```python
# 创建墓碑
logical_ts = self.state.logical_clock.get_watermark()
physical_ts = time.time()
self.state.tombstone_list[path] = (logical_ts, physical_ts)

# TTL 清理 (Audit-End)
tombstone_ttl_seconds = 3600.0  # 1 hour
self.state.tombstone_list = {
    path: (l_ts, p_ts) for path, (l_ts, p_ts) in self.state.tombstone_list.items()
    if (now_physical - p_ts) < tombstone_ttl_seconds
}
```

### 4.3 可疑名单 (Suspect List)

- **用途**：标记可能正处于 NFS 客户端缓存中、尚未完全刷新到存储中心的文件、未结束写入的不完整文件
- **结构**：`Dict[Path, Tuple[ExpiryMonotonic, RecordedMtime]]`
  - `ExpiryMonotonic`: TTL 到期时刻（基于 `time.monotonic()`）
  - `RecordedMtime`: 加入名单时记录的文件 mtime
- **来源**：
  1. **原子写完整性 (Atomic Write Integrity)**: 当 `is_atomic_write=False` (部分写入/Modify事件) 时，文件立即进入 Suspect List
  2. **时效判定 (Temporal Check)**: 任何 Snapshot/Audit 发现 `(LogicalWatermark - mtime) < hot_file_threshold` 的文件
- **稳定性判定模型 (Stability-based Model)**：
  1. **原子写清除**：收到 `is_atomic_write=True` (Close/Create) 事件时，**立即移除** Suspect 标记
  2. **实时移除**：收到文件 Realtime Delete 时立即从名单移除
  - **物理过期检查**：后台任务定期检查物理 TTL 已到期的条目
    - **稳定 (Stable)**：TTL到期时，若 `node.mtime == recorded_mtime`，则判定为"已校准"，正式移除。此判定不考虑 logical age (hot/cold)，以兼容 mtime 跳向未来的文件能快速清除标记。
    - **活跃 (Active/Hot)**：若 mtime 发生变化，**续期**一个完整 TTL 周期，并更新 `recorded_mtime`
- **堆优化**：使用 `heapq` 管理 `suspect_heap`，实现 O(log n) 高效过期检查
- **API 标记**：`integrity_suspect: true`

### 4.4 盲区名单 (Blind-spot List)

包含两个子集：
- `blind_spot_additions`: 盲区新增的文件
- `blind_spot_deletions`: 盲区删除的文件

**生命周期**：
- **持久化**：跨 Audit 周期持久保留，不使用 TTL 自动过期（防止有效数据丢失）
- **清除条件**：
  - 收到 Realtime Delete/Update 时移除相关条目
  - Audit 再次看到文件时从 `blind_spot_deletions` 移除
  - **Session 生命周期控制**：`on_session_start` 时清空列表以重新发现盲区

---

## 5. 仲裁算法

核心原则：**Realtime 优先，Mtime 仲裁**

由 `FSArbitrator` 类实现。

### 5.1 Realtime 消息处理

```python
if event.message_source == MessageSource.REALTIME:
    if event_type in [INSERT, UPDATE]:
        # 更新内存树
        tree_manager.update_node(payload, path)
        node.last_updated_at = time.time()  # 物理时间戳
        node.known_by_agent = True
        
        # 一致性状态维护
        is_atomic = payload.get('is_atomic_write', True)
        if is_atomic:
            # Clean write (Close/Create) -> Clear suspect
            suspect_list.pop(path, None)
            node.integrity_suspect = False
        else:
            # Partial write (Modify) -> Mark/Renew suspect
            # 即使是 update 也可能是 partial write
            expiry = time.monotonic() + self.hot_file_threshold
            suspect_list[path] = (expiry, mtime)
            node.integrity_suspect = True

        blind_spot_deletions.discard(path)
        blind_spot_additions.discard(path)
    
    elif event_type == DELETE:
        # 从内存树删除
        tree_manager.delete_node(path)
        
        # 创建墓碑（双时间戳）
        logical_ts = logical_clock.get_watermark()
        physical_ts = time.time()
        tombstone_list[path] = (logical_ts, physical_ts)
        
        # 清理其他状态
        suspect_list.pop(path, None)
        blind_spot_deletions.discard(path)
        blind_spot_additions.discard(path)
```

### 5.2 Snapshot 消息处理

```python
if event.message_source == MessageSource.SNAPSHOT:
    if path in tombstone_list:
        tombstone_ts, _ = tombstone_list[path]
        
        # 单条件转世判定：仅检查 mtime > tombstone_ts
        # 移除了旧的 watermark > tombstone_ts 条件以增强保守性
        if mtime > tombstone_ts:
            # 文件转世：清除墓碑，接受更新
            tombstone_list.pop(path, None)
        else:
            # 僵尸复活：丢弃
            return
    
    # 添加/更新内存树
    tree_manager.update_node(payload, path)
    
    # Suspect 判定
    watermark = logical_clock.get_watermark()
    if (watermark - mtime) < hot_file_threshold:
        node.integrity_suspect = True
        suspect_list[path] = (time.monotonic() + remaining_life, mtime)
```

> [!NOTE]
> **设计决策**：墓碑转世判定使用 `mtime > tombstone_ts` 单条件：
> 1. **保守性原则**：只有当文件 mtime 明确新于墓碑时间时才允许复活
> 2. **语义清晰**：移除了 `watermark > tombstone_ts` 隐式触发器，避免已删除文件"自动复活"
> 3. **简化调试**：单条件判定更易于追踪


### 5.3 Audit 消息处理

#### 场景 1: Audit 报告"存在文件 X" (Smart Merge)

```python
if event.message_source == MessageSource.AUDIT:
    # Rule 1: Tombstone Protection
    if path in tombstone_list:
        tombstone_ts, _ = tombstone_list[path]
        if mtime > tombstone_ts:
            # 文件转世：接受
            tombstone_list.pop(path, None)
        else:
            # 僵尸复活：丢弃
            return
    
    existing = state.get_node(path)
    
    # Rule 2: Mtime Arbitration
    if existing:
        if existing.modified_time >= mtime and not payload.get('audit_skipped'):
            # 内存中的版本更新或相同：维持现状
            return
    
    # Rule 3: Parent Mtime Check (仅对内存中不存在的文件)
    elif existing is None:
        parent_path = payload.get('parent_path')
        parent_mtime_audit = payload.get('parent_mtime')
        memory_parent = directory_path_map.get(parent_path)
        
        if memory_parent and memory_parent.modified_time > (parent_mtime_audit or 0):
            # 内存父目录更新：丢弃（X 是旧文件）
            return
        
        # 通过检查：加入 Blind-spot List（盲区新增）
        blind_spot_additions.add(path)
        node.known_by_agent = False
    
    # 更新内存树
    tree_manager.update_node(payload, path)
    
    # Suspect 判定
    if (watermark - mtime) < hot_file_threshold:
        node.integrity_suspect = True
        suspect_list[path] = (expiry, mtime)
```

#### 场景 2: Audit 报告"目录 D 缺少文件 B" (Missing Item Detection)

在 `handle_audit_end` 中执行：

```python
for path in audit_seen_paths:
    dir_node = directory_path_map.get(path)
    
    # 保护 1: 审计跳过保护
    if dir_node and getattr(dir_node, 'audit_skipped', False):
        continue  # 静默目录的子项不参与 Missing 判定
    
    # 保护 2: 未扫描目录保护（隐式）
    # 如果目录不在 audit_seen_paths 中，其子项不会被检查
    
    if dir_node:
        for child_name, child_node in dir_node.children.items():
            if child_node.path not in audit_seen_paths:
                # 保护 3: Tombstone 保护
                if child_node.path in tombstone_list:
                    continue
                
                # 保护 4: 陈旧证据保护 (Stale Evidence Protection)
                if child_node.last_updated_at > last_audit_start:
                    continue  # 节点在审计后有更新，保护实时权威
                
                # 执行删除
                tree_manager.delete_node(child_node.path)
                blind_spot_deletions.add(child_node.path)
```

### 5.4 特殊场景：旧属性注入 (Old Mtime Injection)

使用 `cp -p`、`rsync -a` 或 `tar -x` 等操作时，新文件会继承源文件的旧 `mtime`。

**问题背景**：
- **$T_1$ (Audit 开始)**：Fusion 记录 `last_audit_start`
- **$T_2$ (实时创建)**：Agent 发现 `cp -p` 创建的新文件，mtime 为一年前
- **$T_2$ (Fusion 同步)**：Fusion 接受文件，记录 `last_updated_at = T_2`
- **$T_3$ (Audit 判定)**：审计扫描列表（物理扫描在 $T_2$ 前完成）中没有该文件

**裁决逻辑保护**：
若只对比 mtime，文件会被误判为"审计前本应存在但实际缺失"而被删除。通过检查 `last_updated_at > last_audit_start`，Fusion 识别出审计报告是**陈旧证据**，放弃删除操作。

---

## 6. 双轨时间系统 (Dual-Track Time System)

详细设计请参考：[LOGICAL_CLOCK_DESIGN.md](./LOGICAL_CLOCK_DESIGN.md)

### 6.1 时间轨道概览

| 轨道 | 定义 | 来源 | 核心用途 |
| :--- | :--- | :--- | :--- |
| **Physical Time** | 全局物理流逝参考 | Fusion/Agent 本地时钟 | 1. 事件索引 (index)<br>2. LRU 归一化<br>3. 陈旧证据保护<br>4. Tombstone TTL 清理 |
| **Logical Clock (Watermark)** | NFS 数据域逻辑时间 | 统计校准合成 | 1. Data Age 计算<br>2. Suspect 状态判定<br>3. 墓碑逻辑时间戳 |

### 6.2 应用场景裁决表

| 判定需求 | 时间源 | 判定逻辑 |
| :--- | :--- | :--- |
| **热文件判定 (Suspect Age)** | `Logical Time` | `age = watermark - file.mtime` |
| **墓碑转世判定** | `Logical Time` | `if event.mtime > tombstone_logical_ts: reincarnate` |
| **墓碑 TTL 清理** | `Physical Time` | `if (now - tombstone_physical_ts) > 1hr: purge` |
| **陈旧证据保护** | `Physical Time` | `if node.last_updated_at > audit_start: skip_deletion` |
| **Suspect TTL 过期** | `Monotonic Time` | `if time.monotonic() > expiry_monotonic: check_stability` |

### 6.3 NFS Clock Drift Compensation (NFS 时钟漂移补偿)

由于 Agent 运行在物理机上的时钟可能与 NFS Server 的时钟（即文件 mtime 的来源）存在偏差，为了保证物理时间戳（index）与逻辑时间戳（mtime）的可比性，Source Driver 必须执行漂移补偿。

**机制 (Shadow Reference Frame)**：
- **Sampling**: Agent 启动时执行 Pre-scan，收集所有目录的 recursive mtime。
- **Reference Selection**: 选取 P99 分位的 mtime 作为 `latest_mtime_stable` (排除未来时间或极端异常值)。
- **Drift Calculation**: `drift = latest_mtime_stable - time.time()`。这里假设最活跃的目录 mtime 极其接近 NFS Server 当前时间。
- **Correction**: 生成事件时，物理时间戳 `index` = `(time.time() + drift) * 1000`。
- **目的**: 确保 Fusion 收到的事件 `index` 大致对齐到 NFS 的时间轴，防止因 Agent 时钟大幅落后导致事件被误判为"陈旧"而被丢弃。

---

## 7. 审计生命周期

Agent 通过 API 发送生命周期信号，触发 Fusion 的一致性处理：

| API | 时机 | Fusion 动作 |
|-----|------|-------------|
| `POST /consistency/audit/start` | 审计开始 | 调用 `handle_audit_start()`，记录 `last_audit_start = time.time()` |
| `POST /consistency/audit/end` | 审计结束 | 等待队列排空后调用 `handle_audit_end()`，执行 Missing 判定和 Tombstone 清理 |

**Audit-End 处理流程**：
1. 等待事件队列排空（最多 10 秒）
2. 执行 Tombstone TTL 清理（物理时间 > 1 小时）
3. 执行 Missing Item Detection
4. 重置 `last_audit_start` 和 `audit_seen_paths`

---

## 8. 哨兵巡检 (Sentinel Sweep)

- **触发者**：Leader Agent
- **频率**：2 分钟/次 (可配置 `sentinel_interval_sec`)
- **目的**：验证 Suspect List 中文件的 mtime 稳定性
- **实现**：Agent 调用 `FSDriver.perform_sentinel_check()` 获取文件最新状态

### API

```
# 获取待巡检任务
GET  /api/v1/ingest/consistency/sentinel/tasks
     Response: {"type": "suspect_check", "paths": ["/file1.txt", ...], "source_id": 1}

# 提交巡检结果
POST /api/v1/ingest/consistency/sentinel/feedback
     Body: {"type": "suspect_update", "updates": [{"path": "...", "mtime": 123.0, "status": "exists"}, ...]}
```

Fusion 收到反馈后通过 `driver.update_suspect()` 执行稳定性判定。若反馈证明文件稳定，则立即清除可疑标记（加速收敛）。

---

## 9. API 反馈

| 级别 | 条件 | 返回字段 |
|------|------|----------|
| 全局级 | Blind-spot List 非空 | `has_blind_spot: true` (通过 `/views/{view_id}/tree/stats`) |
| 文件级 | 文件在 Suspect List 中 | `integrity_suspect: true` |
| 盲区查询 | 需获取详细盲区文件列表 | 使用 `/views/{view_id}/tree/blind-spots` API |

### 9.2 Standard Response Format (标准响应格式)

核心数据接口 (如 `/tree`) **必须** 使用信封结构包裹返回结果，以支持元数据扩展：

```json
{
  "data": { ... core_domain_object ... },
  "scan_pending": boolean,  // True if a realtime scan was triggered and pending
  "meta": { ... }           // Optional additional metadata
}
```

客户端SDK负责自动解包 `data` 字段，向上层应用提供纯净的领域对象。

### 9.1 主动查询 (Real-Time Query)

用户可通过 API 强制触发实时扫描：

```http
GET /api/v1/views/{view_id}/tree?path=/data/logs&force-real-time=true
```

**处理流程**：
1. Fusion 接收请求，挂起 HTTP 响应
2.通过 Heartbeat Response 向 Leader Agent 下发 `scan` 命令
3. Agent 执行 `scan_path("/data/logs")` 并推送事件
4. Fusion 接收事件更新视图
5. (可选) Fusion 返回更新后的结果或超时
```

---

## 10. 扩展性要求

所有 SourceDriver 和 ViewDriver 必须支持 `message_source` 字段：

- **SourceDriver**: 能够生成 `message_source` 为 `realtime`, `snapshot`, `audit` 类型的事件
- **ViewDriver**: 在 `process_event()` 中根据 `message_source` 执行不同的处理逻辑

### ViewDriver 生命周期钩子

| 钩子 | 时机 | 用途 |
|------|------|------|
| `on_session_start` | 新 Session 序列开始 | 重置盲区列表与 Audit 缓冲区 |
| `on_session_close` | Session 结束 | 执行必要的清理操作 |
| `handle_audit_start` | 审计开始 | 记录 `last_audit_start` 物理时间戳 |
| `handle_audit_end` | 审计结束 | 执行 Missing 判定和 Tombstone 清理 |
| `cleanup_expired_suspects` | 后台定时任务 (每 0.5 秒) | 执行 mtime 稳定性检查 |

---

## 11. 实现细节 (Implementation Notes)

### 11.1 陈旧证据保护 (Stale Evidence Protection)

每个节点维护 `last_updated_at` 物理时间戳（`time.time()`），记录最后一次被 Fusion 确认更新的时刻。

**关键行为**：
- **Realtime 事件**：更新 `last_updated_at = time.time()`
- **Snapshot/Audit 事件**：**不更新** `last_updated_at`（保留原值）

```python
# arbitrator.py
if not is_realtime and old_last_updated_at > 0:
    node.last_updated_at = old_last_updated_at  # 保留旧值
```

### 11.2 审计跳过保护 (Audit Skipped Protection)

当父目录被标记为 `audit_skipped=True` 时（因 mtime 未变而跳过扫描），其子项不会被 Missing 判定误删。

```python
# audit.py
if dir_node and not getattr(dir_node, 'audit_skipped', False):
    # 只对完整扫描的目录执行 Missing 判定
    ...
```

### 11.3 Suspect 堆优化 (Heap-based TTL)

使用 `heapq` 管理 Suspect 条目的 TTL 到期时间：

```python
# state.py
self.suspect_list: Dict[str, Tuple[float, float]] = {}  # path -> (expiry_monotonic, recorded_mtime)
self.suspect_heap: List[Tuple[float, str]] = []         # (expiry_monotonic, path)

# arbitrator.py - 添加
heapq.heappush(self.state.suspect_heap, (expiry, path))

# arbitrator.py - 清理
while suspect_heap and suspect_heap[0][0] <= time.monotonic():
    expires_at, path = heapq.heappop(suspect_heap)
    # 验证并处理...
```

### 11.4 并发控制

`FSViewDriver` 使用两级并发控制：
- **全局信号量** (`_global_semaphore`): 限制并发事件处理数量
- **全局独占锁** (`_global_exclusive_lock`): 用于 Audit Start/End 等需要独占访问的操作

```python
async def process_event(self, event):
    async with self._global_semaphore:
        return await self.arbitrator.process_event(event)

async def handle_audit_end(self):
    async with self._global_exclusive_lock():
        await self.audit_manager.handle_end()
```
