# Fustor 逻辑时钟与时间一致性设计 (Logical Clock Design)

## 1. 背景与挑战

在分布式 NFS 监控环境中，Fustor 面临以下时间同步挑战：

| 挑战 | 描述 | 影响 |
| :--- | :--- | :--- |
| **Clock Skew** | 不同 Agent 服务器与 NFS Server 之间存在时钟偏差。 | 基于 mtime 的 Age 计算错误。 |
| **Clock Drift** | 物理时钟随温度、负载产生微小漂移。 | 静态 Offset 校验失效。 |
| **Future Timestamp** | 用户 `touch` 或程序写入未来时间的文件。 | **灾难性**：传统 `Max(mtime)` 逻辑时钟会被瞬间“撑大”，导致所有正常时间的新文件被误判为“旧数据”（Stable），从而绕过一致性保护机制。 |
| **Stagnation** | 系统无写入（静默）时，被动时钟停止更新。 | Suspect 文件永远无法过期。 |

## 2. 核心架构：双轨时间系统 (Dual-Track Time System)

Fustor 严格区分“数据一致性时间”与“系统运行时间”。

| 轨道 | 定义 | 来源 | 作用 |
| :--- | :--- | :--- | :--- |
| **Physical Time** | Fusion Server 的系统时间 (`time.time()`) | Fusion 本地时钟 | 1. `last_updated_at` (新鲜度保护)<br>2. `last_audit_start` (审计周期)<br>3. `suspect_ttl` (状态过期)<br>4. **保底水位线** (Watchdog) |
| **Logical Clock (Watermark)** | **NFS Server 的估算物理时间** | 算法合成 | **数据一致性核心基准**。<br>用于计算 Data Age，判定 Suspect 状态。 |

## 3. 稳健逻辑时钟算法 (Robust Logical Clock Algorithm)

为了解决上述挑战，Fustor 放弃被动的 `Max(mtime)` 机制，采用 **"统计学校准的主动时钟"** 方案。

### 3.1 算法逻辑

1.  **优先计算 (Primary)**: 尝试基于 `BaseLine` (AgentTime - Skew) 和 `FastPath` (信任窗口) 计算水位线。
2.  **兜底 (Fallback)**: **仅当** 无法计算上述值时（例如系统冷启动且无实时事件样本），使用 `FusionLocalTime` 临时顶替水位线，以防止系统死锁。

**注意**: 不应直接取 `max(..., FusionLocalTime)`，因为 Fusion 本地时钟可能与 NFS 存在固有偏差，强制取 Max 会破坏 Skew 校准结果。

### 3.2 组件详解

#### A. 基准线 (BaseLine)
利用 Agent 物理时间流逝来驱动时钟，通过统计学方法剔除异常值。

1.  **Skew 采样**: 对于每个 Realtime 事件，计算 `Diff = AgentTime - mtime`。
2.  **Global Skew 选举**:
    *   维护所有活跃 Session 的样本（滑动窗口，最大 1000 个）。
    *   构建全局差异直方图 (Histogram)。
    *   **即使只有 1 个样本，也立即开始计算**。无需等待窗口填满。
    *   选取 **出现频率最高 (Mode)** 的 Diff 作为全局权威偏差 `G_Skew`。
    *   *Tie-breaker*: 若频率相同，取最小的 Diff（保守策略）。
3.  **计算**:
    $$ BaseLine = CurrentAgentTime - G\_Skew $$

#### B. 信任窗口 (Trust Window)与快进 (FastPath)
为消除 BaseLine 的计算滞后（统计平滑带来的副作用），允许在安全范围内直接采信 mtime。

*   设定窗口 $W = 1.0s$。
*   **规则**: 若事件 `mtime` 满足 `BaseLine < mtime <= BaseLine + W`，则：
    *   该 mtime 是合法的“最新前沿”。
    *   `FastPath = mtime` (水位线立即推进到此)。
    *   通过此机制，系统的响应延迟理论上降为 0。

#### C. 兜底机制 (Fallback Strategy)
*   **FusionLocalTime**: 当系统冷启动导致样本不足无法计算 Skew 时，使用 Fusion 本地时间作为**临时水位线**。
*   一旦累积了足够的样本并算出 `G_Skew`，立即切换回 `BaseLine` 模式，不再参考 Fusion 本地时间（除非再次发生所有 Session 断开的情况）。

## 4. 事件处理策略表 (Arbitration Policies)

根据 Data mtime 与当前 Watermark 的关系，执行差异化处理：

| 场景 | 条件 | Tree 更新 | Clock 推进 | Suspect 标记 | 说明 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **过期/历史数据** | `mtime <= Watermark` | ✅ 更新 | ⛔ 不推进 | 根据 Age 判定 | 正常的乱序到达或旧数据同步。 |
| **当前/实时数据** | `Watermark < mtime <= Watermark + 1s` | ✅ 更新 | ✅ **推进至 mtime** | ✅ 标记 (Hot) | 处于信任窗口内，视为合法推进。 |
| **未来/异常数据** | `mtime > Watermark + 1s` | ✅ 更新 | ⛔ **拒绝推进** | ✅ **强制 Suspect** | 可能是 `touch` 或时钟错误。收录数据但隔离影响，直到时钟追上。 |

## 5. 优势总结

1.  **免疫跳钟攻击**: 用户 `touch -d "2050年"` 不会撑大水位线，不会导致全系统误判正常数据为过期。
2.  **静默自动推进**: 即使无写入，水位线也会随 `FusionLocalTime` 自动增长，保证 Suspect 状态能正常过期。
3.  **自适应漂移**: 滑动窗口自动适应不同 Agent 的网络延迟和时钟热漂移，无需人工校准 NTP。
4.  **高精度**: 信任窗口机制保留了 `Max(mtime)` 的高实时性优点，同时屏蔽了其不稳定性。
