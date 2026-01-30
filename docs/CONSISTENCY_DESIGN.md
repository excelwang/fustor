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

---

## 3. 消息类型

Agent 向 Fusion 发送的消息分为四类，通过 `message_source` 字段区分：

| 类型 | 来源 | 说明 |
|------|------|------|
| `realtime` | inotify 事件 | 单个文件的增删改，优先级最高 |
| `snapshot` | Agent 启动时全量扫描 | 初始化内存树 |
| `audit` | 定时审计扫描 | 发现盲区变更 |

### 3.1 Audit 快速扫描算法 (Agent 端)

利用 POSIX 语义：创建/删除文件只更新**直接父目录**的 mtime。

```python
def audit_directory(dir_path, cache):
    current_mtime = os.stat(dir_path).st_mtime
    cached = cache.get(dir_path)
    
    # 目录 mtime 未变 → 直接子项无增删，但仍需递归检查子目录
    if cached and cached.mtime == current_mtime:
        for child in os.listdir(dir_path):
            child_path = os.path.join(dir_path, child)
            if os.path.isdir(child_path):
                audit_directory(child_path, cache)  # 递归
        return
    
    # 目录 mtime 变了 → 完整扫描
    for child in os.listdir(dir_path):
        child_path = os.path.join(dir_path, child)
        stat = os.stat(child_path)
        
        if os.path.isdir(child_path):
            audit_directory(child_path, cache)
        else:
            # 发送 audit 消息
            send_audit_event(child_path, stat.st_mtime, stat.st_size)
    
    cache[dir_path] = CacheEntry(mtime=current_mtime)
```

### 3.2 Audit 消息格式

Audit 消息复用标准 Event 结构，但必须包含以下额外信息：

```json
{
  "message_source": "audit",
  "event_type": "UPDATE",  // INSERT / UPDATE / DELETE
  "rows": [
    {
      "path": "/data/file.txt",
      "mtime": 1706000123.0,
      "size": 10240,
      "parent_path": "/data",
      "parent_mtime": 1706000100.0  // 关键：用于 Parent Mtime Check
    }
  ]
}
```

**关键字段**：
- `parent_mtime`: 扫描时父目录的 mtime，用于 Fusion 判断消息时效性

---

## 4. 状态管理

Fusion 维护以下状态：

### 4.1 内存树 (Memory Tree)

存储文件/目录的元数据，每个节点包含：
- `path`: 文件路径
- `mtime`: 最后修改时间（来自存储系统）
- `size`: 文件大小
- `last_updated_at`: Fusion 最后确认该文件状态点的逻辑时间戳（用于陈旧证据保护）

### 4.2 墓碑表 (Tombstone List)

- **用途**：记录被 Realtime 删除的文件，防止滞后的 Snapshot/Audit 使其复活
- **结构**：`Map<Path, DeleteTime>`
- **生命周期**：
  - 创建：处理 Realtime Delete 时
  - 销毁：**基于 TTL (Time-To-Live)**。默认保留 1 小时。
  
### 4.3 可疑名单 (Suspect List)

- **用途**：标记可能正处于 NFS 客户端缓存中、尚未完全刷新到存储中心的文件、未结束写入的不完整文件。
- **来源**：任何 Snapshot/Audit 发现 `(LogicalWatermark - mtime) < threshold` 的文件。
- **稳定性判定模型 (Stability-based Model)**：
  - **实时移除**：收到文件 Realtime Update/Delete 时立即从名单移除并清除标记。
  - **物理过期检查**：后台任务定期检查物理 TTL (基于 `monotonic_now`) 已到期的条目。
    - **稳定 (Stable/Cold)**：比较当前 `node.mtime` 与加入名单时记录的 `recorded_mtime`。若一致，判定为“已冷却”，正式移除条目并清除 `integrity_suspect` 标记。
    - **活跃 (Active/Hot)**：若 `node.mtime` 发生了变化，说明文件仍处于活跃变动中。此时为条目**续期**一个完整的物理 TTL 周期，并更新 `recorded_mtime`（无需重新计算逻辑 Age）。
- **API 标记**：`integrity_suspect: true`

### 4.4 盲区名单 (Blind-spot List)

- **用途**：标记在无 Agent 客户端发生变更的文件
- **来源**：Audit 发现的新增/删除，但不在 Tombstone 中且不是实时新增
- **生命周期**：
  - **持久化**：不随每次 Audit 清空，也不使用 TTL 自动过期（防止有效数据丢失）
  - **清除**：
    - 收到 Realtime Delete/Update 时移除相关条目
    - Audit 再次看到文件时移除相关条目
    - **Session 重置**：当检测到新的 Agent Session (如重启或Leader切换) 时，盲区可能会被重新发现，清空整个列表

---

## 5. 仲裁算法

核心原则：**Realtime 优先，Mtime 仲裁**

### 5.1 Realtime 消息处理

```
收到 Realtime 消息:
    if INSERT / UPDATE:
        → 更新内存树 (覆盖 mtime)
        → 从 Suspect List 移除
        → 从 Blind-spot List 移除
    
    if DELETE:
        → 从内存树删除
        → 加入 Tombstone List
        → 从 Blind-spot List 移除
```

### 5.2 Snapshot 消息处理

```
收到 Snapshot 消息 (文件 X):
    if X in Tombstone:
        → 丢弃 (防止僵尸复活)
    else:
        → 添加到内存树
        → 如果 (HybridNow - X.mtime) < 10min: 加入 Suspect List
```

### 5.3 Audit 消息处理

#### 场景 1: Audit 报告"存在文件 X" (Smart Merge)

```
# Rule 1: Tombstone Protection
if X in Tombstone:
    if Audit.X.mtime > Tombstone.X.ts:
        → 接受 (新文件转世 Reincarnation)
        → 从 Tombstone 中移除 X
        → 执行写入/更新
    else:
        → 丢弃 (确认是僵尸复活 Zombie)
        return

# Rule 2: Mtime Arbitration
elif X in 内存树:
    if Audit.X.mtime > Memory.X.mtime:
        → 更新内存树 (盲区修改)
    else:
        → 维持现状 (Snapshot/Audit 不覆盖较新的状态)

else:  # 内存中无 X
    if Audit.Parent.mtime < Memory.Parent.mtime:
        → 丢弃 (父目录已更新，X 是旧文件)
    else:
        → 添加到内存树
        → 加入 Blind-spot List (盲区新增)
        → 如果 (LogicalWatermark - X.mtime) < threshold: 加入 Suspect List
```

#### 场景 2: Audit 报告"目录 D 缺少文件 B" (Blind Spot Deletion)

```
if Parent D reported as "Skipped" (audit_skipped=True):
    → 维持现状 (认为是 NFS 缓存导致的静默目录，保护子项不被误删)

elif Parent D was Not Scanned in this cycle:
    → 忽略 (不触发 Missing 判定)

else (Full Scan on D confirmed):
    if B in Memory Tree:
        # Rule 3: 陈旧证据保护 (Stale Evidence Protection)
        if B.last_updated_at > current_audit_start_logical_time:
             → 丢弃删除指令 (该文件在审计开始后有过实时更新，审计视图已落后)
        elif B in Tombstone:
             → 忽略 (已处理)
        else:
             → 将 B 从内存树中删除 (保证视图即真相)
             → 将 B 加入 Blind Spot Deletion List
```

### 5.4 特殊场景：旧属性注入 (Old Mtime Injection)

在使用 `cp -p`、`rsync -a` 或 `tar -x` 等操作时，新创建的文件会继承源文件的旧 `mtime`。这会导致简单的基于 `mtime` 的审计仲裁逻辑判定失效。

**问题背景：**
- **$T_1$ (Audit 开始)**：Fusion 记录审计开始水位线。
- **$T_2$ (实时创建)**：Agent 通过 Inotify 发现一个 `cp -p` 创建的新文件，其 `mtime` 被保留为一年前。
- **$T_2$ (Fusion 同步)**：Fusion 接受该文件，并记录其 `last_updated_at = T_2`。
- **$T_3$ (Audit 逻辑判定)**：由于审计的实际物理扫描发生在 $T_2$ 之前，其提交的扫描列表中没有该文件。

**裁决逻辑保护：**
若只对比 `mtime`，$T_{old} < T_1$ 会导致该文件被误判为“审计前本应存在但实际缺失”，从而被删除。引入 `last_updated_at` 后，Fusion 会检测到 `File.last_updated_at (T_2) > Audit.Start (T_1)`，从而识别出审计报告是**陈旧证据**，放弃删除操作，确保实时事件的绝对权威。

---

## 6. 混合时钟与逻辑时间 (Logical Time & Hybrid Clock)

为解决分布式环境中 Agent 与 Fusion 服务器之间的时钟漂移 (Clock Drift) 问题，系统引入了基于观测水位线 (Watermark) 的逻辑时钟机制。

### 6.1 逻辑时钟 (Logical Clock / Observation Watermark)

Fusion 为每个数据源维护一个单调递增的逻辑时钟 `L`。
- **驱动源**：
  1. **消息序列号 (Message Index)**：Agent 发送事件时携带的物理发送时间戳。`L = max(L, event.index)`
  2. **观测到的 mtime**：处理插入/更新事件时携带的文件修改时间。`L = max(L, event.mtime)`
- **意义**：表示 Fusion 系统目前“观测到”的最新的时间点。即使物理服务器时钟滞后，逻辑时钟也能保证不回退。

### 6.2 时间类型定义

| 类型 | 时间源 | 特性 | 核心用途 |
|------|------------|------|------|
| **Logical Time (L)** | `LogicalClock` | 随数据流演进，无视漂移 | 时效性判定、陈旧证据保护 (`last_updated_at`) |
| **Physical Time (P)** | `time.time()` | 挂钟时间，受 NTP 影响可能回退 | 外部 API 展示、Session 租约检查 |
| **Monotonic Time (M)**| `time.monotonic()`| 纳秒级单调递增，不受挂钟调整影响 | 本地倒计时 (TTL Expiry)、频率限制 (Rate Limiting) |

### 6.3 混合当前时间 (Hybrid Now)

用于判定文件“新鲜度”的关键指标：
```python
hybrid_now = max(time.time(), logical_clock.get_watermark())
```
这确保了即使 Fusion 服务器物理时间极度滞后（如系统启动初期），也能通过 Agent 上报的时间戳将视角拉回到全球数据流的当前状态。

### 6.4 应用场景裁决表

| 场景 | 使用时间源 | 判定逻辑 |
|------|------------|------|
| **热文件判定 (Suspect Age)** | `Logical Time` | `age = watermark - file.mtime` |
| **墓碑时效清理 (Tombstone TTL)**| `Logical Time` | `if watermark - ts > 1hr: purge` |
| **陈旧证据保护 (Audit End)** | `Logical Time` | `if node.last_updated_at > audit_start: skip` |
| **Suspect 稳定性续期 (Physical TTL)** | `Monotonic Time`| `if monotonic_now > expires_at: perform_mtime_stability_check()` |
| **Session 超时 (Heartbeat)** | `Physical Time` | `physical_now - last_heartbeat > timeout` |
| **清理频率限制 (Rate Limit)**| `Monotonic Time`| `if monotonic_now - last_cleanup < 0.5s: skip` |

---

---

## 7. 审计生命周期

为支持 Tombstone 的精确清理，Agent 需发送生命周期信号：

| 信号 | 时机 | Fusion 动作 |
|------|------|-------------|
| `Audit-Start` | 审计开始 | 记录 `scan_start_time` |
| `Audit-End` | 审计结束 | 清理 `create_time < scan_start_time` 的 Tombstone |

---

## 8. 哨兵巡检 (Sentinel Sweep)

- **触发者**：Leader Agent
- **频率**：2 分钟/次
- **目的**：更新 Suspect List 中文件的 mtime
- **消息类型**：`snapshot`

### API (View-Specific)

```
GET  /api/view/fs/suspect-list?source_id={id}
PUT  /api/view/fs/suspect-list
     Body: [{path, current_mtime, status}, ...]
```

Fusion 收到 PUT 后仅更新 mtime，不执行移除。移除由 TTL 或 Realtime 事件触发。

---

## 9. API 反馈

| 级别 | 条件 | 返回字段 |
|------|------|----------|
| 全局级 | Blind-spot List 非空 | `has_blind_spot: true` (通过 `/fs/stats`) |
| 文件级 | 文件在 Suspect List 中 | `integrity_suspect: true` |
| 盲区查询 | 需获取详细盲区文件列表 | 使用 `/fs/blind-spots` API |

---

## 10. 扩展性要求

所有 Driver 和 Parser 必须支持 `message_source` 字段：

- **Driver**: 能够生成 `realtime`, `snapshot`, `audit` 类型的事件
- **Parser**: 在 `process_event` 中根据 `message_source` 执行不同的处理逻辑
