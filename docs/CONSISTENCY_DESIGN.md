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

### 4.2 墓碑表 (Tombstone List)

- **用途**：记录被 Realtime 删除的文件，防止滞后的 Snapshot/Audit 使其复活
- **结构**：`Map<Path, DeleteTime>`
- **生命周期**：
  - 创建：处理 Realtime Delete 时
  - 销毁：收到 `Audit-End` 信号后，清理所有在此次 Audit 开始前创建的 Tombstone

### 4.3 可疑名单 (Suspect List)

- **用途**：标记可能正在写入的文件
- **来源**：Snapshot/Audit 发现 `(Now - mtime) < 10min` 的文件
- **淘汰**：静默 10 分钟后移除，或收到 Realtime Update 后移除
- **API 标记**：`integrity_suspect: true`

### 4.4 盲区名单 (Blind-spot List)

- **用途**：标记在无 Agent 客户端发生变更的文件
- **来源**：Audit 发现的新增/删除，但不在 Tombstone 中且不是实时新增
- **生命周期**：
  - **持久化**：不随每次 Audit 清空，也不使用 TTL 自动过期（防止有效数据丢失）
  - **清除**：
    - 收到 Realtime Delete/Update 时移除相关条目
    - Audit 再次看到文件时移除相关条目
    - **Session 重置**：当检测到新的 Agent Session (如重启或Leader切换) 时，视为全量同步开始，清空整个列表
- **API 标记**：`agent_missing: true` (新增), `/blind-spots` 列表 (删除)

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
        → 如果 (Now - X.mtime) < 10min: 加入 Suspect List
```

### 5.3 Audit 消息处理

#### 场景 1: Audit 报告"存在文件 X"

```
if X in Tombstone:
    → 丢弃 (僵尸复活)

elif X in 内存树:
    if Audit.X.mtime > Memory.X.mtime:
        → 更新内存树 (盲区修改)
    else:
        → 丢弃 (Realtime 更新)

else:  # 内存中无 X
    if Audit.Parent.mtime < Memory.Parent.mtime:
        → 丢弃 (父目录已更新，X 是旧文件)
    else:
        → 添加到内存树
        → 加入 Blind-spot List (盲区新增)
        → 如果 (Now - X.mtime) < 10min: 加入 Suspect List
```

#### 场景 2: Audit 报告"目录 D 缺少文件 B" (Blind Spot Deletion)

```
if Parent D is Skipped in Audit (audit_skipped=True):
    → 维持现状 (认为是 NFS 缓存导致的未扫描，保留之前的 Deletion 记录)

elif Parent D was Not Scanned:
    → 忽略

else (Full Scan on D):
    if B in Memory Tree:
        → 将 B 加入 Blind Spot Deletion List (记录发现时间)
        → (注意：此时不立即从内存树删除，仅标记。直到收到明确的 Delete 事件或 Session Reset)

    # 列表清理
    # 仅在检测到文件恢复(Audit found) 或 实时事件(Realtime Delete/Update) 或 新 Session 时清理。
```

---

## 6. 混合时钟策略 (Hybrid Clock)

为解决分布式环境中 Agent 与 Fusion 服务器之间的时钟漂移 (Clock Drift) 问题，系统引入了混合时钟机制。

### 6.1 问题背景

在 NFS 环境中，文件 `mtime` 由 NFS Server 生成，而 Fusion 的当前时间 `now` 由 Fusion Server 的物理时钟决定。这可能导致：
- `now < mtime`: 新文件被误判为未来产生，导致 `staleness` 计算错误。
- `now - mtime` 误差：导致热文件检测 (Hot File Detection) 不准确。

### 6.2 逻辑时钟 (Logical Clock)

Fusion 维护一个单调递增的逻辑时钟 `L`。
- **更新规则**：每次收到文件事件（包含 `mtime`）时，更新 `L = max(L, mtime)`。
- **持久化**：内存维护，重启重置。

### 6.3 混合时间 (Hybrid Now)

定义混合当前时间 `H` 为物理时钟与逻辑时钟的最大值：

```python
hybrid_now = max(time.time(), logical_clock.value)
```

### 6.4 应用场景

| 场景 | 使用时间源 | 逻辑 |
|------|------------|------|
| **热文件检测** | `Hybrid Now` | `if (hybrid_now - mtime) < threshold: mark_suspect()` |
| **时效性指标** | `Hybrid Now` | `staleness = hybrid_now - oldest_unprocessed_mtime` |
| **Session 超时** | `Physical Time` | `time.time() - last_heartbeat > timeout` |
| **Suspect TTL** | `Physical Time` | `expiry = time.time() + ttl` |

> **设计原则**：涉及与 `mtime` 进行**绝对时间点比较**的逻辑使用 `Hybrid Now`；涉及**时间段 (Duration)** 测量的逻辑使用 `Physical Time`。

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
| 全局级 | Blind-spot List 非空 | `agent_missing: true` |
| 文件级 | 文件在 Suspect List 中 | `integrity_suspect: true` |

---

## 10. 扩展性要求

所有 Driver 和 Parser 必须支持 `message_source` 字段：

- **Driver**: 能够生成 `realtime`, `snapshot`, `audit` 类型的事件
- **Parser**: 在 `process_event` 中根据 `message_source` 执行不同的处理逻辑
