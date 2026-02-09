# Fustor 分层时钟与时间一致性设计 (Hierarchical Clock Design)

## 1. 背景与挑战

在分布式 NFS 监控环境中，Fustor 面临以下时间同步挑战：

| 挑战 | 描述 | 影响 |
| :--- | :--- | :--- |
| **Clock Skew** | 不同 Agent 服务器与 NFS Server 之间存在时钟偏差 | 基于 mtime 的 Age 计算错误 |
| **Clock Drift** | 物理时钟随温度、负载产生微小漂移 | 静态 Offset 校验失效 |
| **Future Timestamp** | 用户 `touch` 或程序写入未来时间的文件 | **灾难性**：传统 `Max(mtime)` 逻辑时钟会被瞬间"撑大"，导致所有正常时间的新文件被误判为"旧数据" |
| **Stagnation** | 系统无写入（静默）时，被动时钟停止更新 | Suspect 文件永远无法过期 |

---

## 2. 核心架构：双轨时间系统 (Dual-Track Time System)

Fustor 严格区分"数据一致性时间"与"系统运行时间"，并在架构上实现了分层：

| 层面 | 组件 | 核心逻辑 | 目的 |
| :--- | :--- | :--- | :--- |
| **Agent 层** | **Source FS Clock** | **影子参考系 (Shadow Reference Frame)** | 保护本地 LRU 和扫描频率，防止受 NFS 时间跳变干扰 |
| **Fusion 层** | **View FS Clock** | **统计逻辑水位线 (Watermark)** | 驱动全局一致性判定 (Suspect/Tombstone)，提供真实 Age |

---

## 3. Agent 侧：Source FS 时钟 (本地保护)

Agent 负责监控 NFS 变化。NFS 服务器的时钟跳变（例如被 `ntpdate` 强制同步）不应干扰 Agent 本身的扫描任务调度和内存淘汰逻辑。

### 3.1 影子参考系 (Shadow Reference Frame)

Agent 内部计算一个虚拟的物理参考系。

**实现** (`source-fs/__init__.py`)：

```python
# 1. P99 漂移采样
mtimes = sorted(dir_mtime_map.values())
p99_idx = max(0, len(mtimes) - 1 - (len(mtimes) // 1000))  # 99.9th percentile
latest_mtime_stable = mtimes[p99_idx]

# 2. 计算漂移
self.drift_from_nfs = latest_mtime_stable - time.time()

# 3. 归一化调度 (LRU 使用归一化时间)
lru_timestamp = server_mtime - drift_from_nfs
self.watch_manager.schedule(path, lru_timestamp)
```

**效果**：即使 NFS 上某个孤岛文件跳变到 2050 年，该文件在 Agent 内部也只会自保（计算出的归一化年龄依然是正常的），而不会导致全树的归一化年龄被拉大而导致其他文件被提前踢出缓存。

### 3.2 职责限制

Agent 侧的时钟逻辑**仅用于本地资源管理**，不驱动全局逻辑。Agent 发送的消息始终携带：
- 原始 `mtime`
- 采集时的物理观察时刻（`index` 字段，毫秒级物理时间戳）

---

## 4. Fusion 侧：View FS 时钟 (全局裁决)

Fusion 侧的逻辑时钟（Watermark）是系统的"真相之钟"，它决定了如何合并来自不同 Agent 的消息。

### 4.1 简化逻辑时钟算法 (Simplified Logical Clock)

Fusion 采用 **纯物理时间驱动的水位线**，完全免疫 mtime 异常。

**公式**：

```python
Watermark = Fusion_Physical_Time - Mode_Skew
```

**实现** (`fustor_core/clock/logical_clock.py`)：

#### A. Skew 采样

对于每个 Realtime 事件，计算 Fusion 本地时间与 mtime 的差异：

```python
if mtime and can_sample_skew:
    reference_time = time.time()  # Fusion Local Time
    diff = int(reference_time - mtime)
    self._global_buffer.append(diff)
    self._global_histogram[diff] += 1
```

- **免疫力**: 使用 Fusion Local Time 作为参考系，免疫 Agent 时钟偏差

#### B. Mode Skew 选举

```python
def _get_global_skew_locked(self) -> float:
    mode_key = self._global_histogram.most_common(1)[0][0]
    return float(mode_key)
```

- 选取出现频率最高 (Mode) 的差异值作为权威偏差

#### C. Watermark 计算

```python
def get_watermark(self) -> float:
    skew = self._get_global_skew_locked() or 0.0
    return time.time() - skew
```

- **简洁性**: 纯函数，无状态依赖
- **免疫性**: 恶意 mtime（如 `touch -d 2050`）完全无法推进 Watermark
- **一致性**: Watermark 始终与物理时间同步推进，不受写入流量影响

> [!NOTE]
> **移除的旧逻辑**: 
> - ~~Trust Window~~ (±1s 信任窗口)
> - ~~Fast Path~~ (mtime 直接推进水位线)
> 
> 这些已被移除以简化实现并增强对时间异常的免疫力。


### 4.2 核心 API

```python
class LogicalClock:
    def update(self, mtime: Optional[float], can_sample_skew: bool = True) -> float:
        """更新时钟状态，返回当前水位线
        
        注：始终使用 Fusion Local Time 作为物理参考，免疫 Agent 时钟偏差
        """
    
    def get_watermark(self) -> float:
        """获取当前逻辑水位线"""
    
    def now(self) -> float:
        """获取当前水位线（get_watermark 的别名）"""
    
    def reset(self, initial_ts: float):
        """重置时钟状态"""
```

---

## 5. 已知局限性

### 5.1 冷启动问题 (Cold Start Issue)

**场景描述**：
当 Fusion 刚启动或 View 刚创建时，若第一批事件为 Snapshot/Audit（而非 Realtime），逻辑时钟可能出现以下行为：

1. **Skew 采样不足**：`_global_histogram` 为空，`_compute_mode_skew()` 返回 0
2. **Watermark 回退到物理时间**：`baseline = time.time() - 0 = time.time()`
3. **Age 计算偏差**：若文件 mtime 较旧，`age = watermark - mtime` 可能**异常地大**

**影响**：
- 本应被标记为 Suspect 的热文件可能被误判为"已冷却"
- Snapshot 阶段的文件可能无法正确进入 Suspect List

**缓解措施**（当前实现）：
```python
def get_watermark(self) -> float:
    baseline = time.time() - self._compute_mode_skew()
    return max(self._watermark, baseline)
```
- 即使 `_watermark` 未被有效推进，也会回退到当前物理时间作为保底
- 这确保了 Age 不会为负值，但可能导致冷启动时的 Suspect 判定偏宽松

**建议处理方式**：
- 在生产环境中，Agent 应优先建立 Realtime 连接，使 Fusion 有机会在 Snapshot 前校准 Skew
- 对于关键场景，可在配置中增加 `initial_suspect_window` 参数，冷启动期间采用更宽松的 Suspect 阈值

---

## 6. 处理策略对照表

| 场景 | Agent (Source FS) 行为 | Fusion (View FS) 行为 |
| :--- | :--- | :--- |
| **正常流逝** | 顺延 P99 校准，LRU 保持稳定 | Watermark 随 BaseLine 稳健推进 |
| **mtime 跳向未来** | 计算出的 Drift 增大，归一化时间保持稳定 | **拒绝推进水位线**（超出 Trust Window）；标记该文件为 `integrity_suspect` |
| **mtime 退回过去** | 归一化 Age 变大，作为旧文件处理 | 视为旧数据注入，不影响当前水位线；通过墓碑策略裁决 |
| **长期无写入** | 停止采样，Drift 维持现状 | BaseLine 随物理时间流逝，Watermark 自动推进，促使 Suspect 过期 |
| **冷启动** | 正常发送物理时间戳 | Skew 采样不足时回退到物理时间，可能导致 Suspect 判定偏宽松 |

---

## 7. 时间使用场景总结

| 判定需求 | 时间源 | 字段/方法 | 说明 |
| :--- | :--- | :--- | :--- |
| **热文件判定 (Suspect Age)** | Logical Time | `logical_clock.get_watermark() - mtime` | 判断文件是否为近期活跃 |
| **墓碑转世判定** | Logical Time | `mtime > tombstone[0]` (逻辑时间戳) | 判断新文件是否覆盖已删除文件 |
| **墓碑 TTL 清理** | Physical Time | `time.time() - tombstone[1]` (物理时间戳) | 清理超过 1 小时的墓碑 |
| **陈旧证据保护** | Physical Time | `node.last_updated_at` vs `audit_start` | 保护审计后有实时更新的节点 |
| **Suspect TTL 过期** | Monotonic Time | `time.monotonic()` vs `suspect_heap[0][0]` | 稳定的过期判定，不受系统时钟调整影响 |
| **事件索引 (Index)** | Physical Time | `int(time.time() * 1000)` | Agent 捕获事件的物理时刻 |

---

## 8. 优势总结

1.  **层级隔离**: Agent 保护本地资源，Fusion 保护全局一致性
2.  **免疫单点故障**: 即使某个 Agent 时钟彻底错乱，Fusion 通过全局 Mode 选举将其样本识别为异常并剔除
3.  **高精度与强健性**: 兼顾了实时快进的高精度和基准线流逝的强健性
4.  **自愈能力**: 长期无写入时，Watermark 随物理时间自动推进，避免 Suspect 永久滞留
5.  **可观测性**: 通过 `logical_clock.hybrid_now()` 可获取当前混合时间，便于监控和调试
