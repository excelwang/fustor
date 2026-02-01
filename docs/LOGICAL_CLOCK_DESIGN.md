# Fustor 分层时钟与时间一致性设计 (Hierarchical Clock Design)

## 1. 背景与挑战

在分布式 NFS 监控环境中，Fustor 面临以下时间同步挑战：

| 挑战 | 描述 | 影响 |
| :--- | :--- | :--- |
| **Clock Skew** | 不同 Agent 服务器与 NFS Server 之间存在时钟偏差。 | 基于 mtime 的 Age 计算错误。 |
| **Clock Drift** | 物理时钟随温度、负载产生微小漂移。 | 静态 Offset 校验失效。 |
| **Future Timestamp** | 用户 `touch` 或程序写入未来时间的文件。 | **灾难性**：传统 `Max(mtime)` 逻辑时钟会被瞬间“撑大”，导致所有正常时间的新文件被误判为“旧数据”。 |
| **Stagnation** | 系统无写入（静默）时，被动时钟停止更新。 | Suspect 文件永远无法过期。 |

---

## 2. 核心架构：双轨时间系统 (Dual-Track Time System)

Fustor 严格区分“数据一致性时间”与“系统运行时间”，并在架构上实现了分层：

| 层面 | 组件 | 核心逻辑 | 目的 |
| :--- | :--- | :--- | :--- |
| **Agent 层** | **Source FS Clock** | **影子参考系 (Shadow Reference Frame)** | 保护本地 LRU 和扫描频率，防止受 NFS 时间跳变干扰。 |
| **Fusion 层** | **View FS Clock** | **统计逻辑水位线 (Watermark)** | 驱动全局一致性判定 (Suspect/Tombstone)，提供真实 Age。 |

---

## 3. Agent 侧：Source FS 时钟 (本地保护)

Agent 负责监控 NFS 变化。NFS 服务器的时钟跳变（例如被 `ntpdate` 强制同步）不应干扰 Agent 本身的扫描任务调度和内存淘汰逻辑。

### 3.1 影子参考系 (Shadow Reference Frame)
Agent 内部计算一个虚拟的物理参考系。

1.  **P99 漂移采样**: Agent 在扫描时计算所有目录 `mtime` 的第 99.9 百分位点 $M_{p99}$，并计算相对于 Agent 本地时钟的位移：$Drift = M_{p99} - T_{local\_phys}$。
2.  **归一化调度 (Normalized Age)**: 
    *   LRU 排序和 30 天保护窗口使用 **归一化时间**：$T_{norm} = M_{dir} - Drift$。
    *   **效果**: 这使得即使 NFS 上某个孤岛文件跳变到 2050 年，该文件在 Agent 内部也只会自保（计算出的归一化年龄依然是正常的），而不会导致全树的归一化年龄被拉大而导致其他文件被提前踢出缓存。

### 3.2 职责限制
Agent 侧的时钟逻辑**仅用于本地资源管理**，不驱动全局逻辑。Agent 发送的消息始终携带原始 `mtime` 和采集时的物理观察时刻（物理锚点）。

---

## 4. Fusion 侧：View FS 时钟 (全局裁决)

Fusion 侧的逻辑时钟（Watermark）是系统的“真相之钟”，它决定了如何合并来自不同 Agent 的消息。

### 4.1 稳健逻辑时钟算法 (Robust Logical Clock)
Fusion 放弃被动的 `Max(mtime)` 机制，采用 **"统计学校准的主动时钟"**。

#### A. 基准线 (BaseLine) 驱动
1.  **Skew 采样**: 对于每个 Realtime 事件，计算 `Diff = ReferenceTime - mtime`。ReferenceTime 优先使用 Agent 事件携带的物理时刻，保底使用 Fusion 本地时间。
2.  **Global Skew 选举 (Mode)**:
    *   Fusion 维护一个全局滑动窗口直方图（如 10,000 个样本）。
    *   选取出现频率最高 (Mode) 的差异值作为权威偏差 `G_Skew`。
    *   **De-sessionization**: 时钟独立于会话，所有 Agent 的样本进入全局池共同校准全局 Skew。
3.  **推进**: $BaseLine = CurrentTime - G\_Skew$。这确保了即使无写入，时钟也会随物理时间自然流逝推进。

#### B. 信任窗口 (Trust Window) 与快进
为保留实时性，允许在安全范围内直接采信 mtime。
*   设定窗口 $W = 1.0s$。
*   **规则**: 若事件 `mtime` 满足 `BaseLine < mtime <= BaseLine + W`，则 $Watermark$ 立即快进到该 `mtime`。

#### C. 删除事件的“物理引导”
由于 `DELETE` 消息无 `mtime`，Fusion 利用该消息到达的物理时刻（物理观察时刻）和 `G_Skew` 计算对应的逻辑时刻，从而驱动墓碑 (Tombstone) 的演进。

---

## 5. 处理策略对照表

| 场景 | Agent (Source FS) 行为 | Fusion (View FS) 行为 |
| :--- | :--- | :--- |
| **正常流逝** | 顺延 P99 校准，LRU 保持稳定。 | Watermark 随 BaseLine 稳健推进。 |
| **mtime 跳向未来** | 计算出的 Drift 增大，归一化时间保持稳定。 | **拒绝推进水位线**；标记该文件为 `integrity_suspect`。 |
| **mtime 退回过去** | 归一化 Age 变大，作为旧文件处理。 | 视为旧数据注入，不影响当前水位线；通过墓碑策略裁决。 |
| **长期无写入** | 停止采样，Drift 维持现状。 | 基准线随物理时间流逝，水位线自动推进，促使 Suspect 过期。 |

---

## 6. 优势总结

1.  **层级隔离**: Agent 保护本地资源，Fusion 保护全局一致性。
2.  **免疫单点故障**: 即使某个 Agent 时钟彻底错乱，Fusion 也会通过全局 Mode 选举将其样本识别为异常并剔除。
3.  **高精度与强健性**: 兼顾了实时快进的高精度和基准线流逝的强健性。
