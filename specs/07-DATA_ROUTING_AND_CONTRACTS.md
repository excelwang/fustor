# Fustor 数据路由与契约规范

> 版本: 1.0.0  
> 日期: 2026-02-11  
> 状态: 已确认

本文档记录了 Fustor 在多租户、多节点环境下关于数据契约（Schema）定义及事件路由（Routing）的核心设计决策。

---

## 1. 语义化 Schema 契约 (Semantic Schema Contract)

### 1.1 背景与问题
在早期实现中，`Event.event_schema` 字段的使用存在歧义，有时被设置为物理路径（如 `/data/share`），有时被设置为数据源 ID（如 `nfs-source-a`）。这导致 Fusion 端无法在异构环境（如不同节点挂载路径不同）下正确识别并合并同一类数据。

### 1.2 决策
`event_schema` 必须代表 **“数据契约/格式”**（Data Format），而非数据源的物理属性。

*   **规范值**：对于文件系统同步，`event_schema` 统一固定为 **`"fs"`**。
*   **约束**：
    *   **Source 驱动**：产生的所有事件（实时、快照、审计）必须将 `event_schema` 设置为驱动声明的逻辑 Schema 名称。
    *   **View 驱动**：必须声明其支持的 `target_schema`（如 `fs`），用于接收路由。

### 1.3 核心价值
*   **路径归一化**：允许不同节点的 Agent 以不同的物理路径监控同一个存储，Fusion 能够通过统一的 `"fs"` 契约将它们自动合并。
*   **驱动解耦**：View 驱动只需关注数据格式，无需关心数据是从哪个具体路径或哪个 Agent 实例产生的。

---

## 2. 两级路由分发机制 (Two-Tier Event Routing)

### 2.1 背景
为了支持一个管道（Pipe）同时更新多个视图（View），且保证不同类型的数据（如 FS 和 DB）在同一管道中不发生误分发，需要建立严格的路由过滤机制。

### 2.2 决策
Fustor 采用 **“Pipe 层粗粒度过滤 + Manager 层细粒度路由”** 的两级分发架构。

#### 第一级：FusionPipe 路由 (Coarse-grained)
*   **职责**：负责基于 `ViewHandler` 的能力声明进行路由。
*   **逻辑**：
    *   如果 `Handler.schema_name` 与 `Event.event_schema` 匹配，则分发。
    *   如果 `Handler.schema_name == "view-manager"`，视为通配符（Aggregator），允许通过。
    *   否则，直接丢弃该事件，不进入 Handler。

#### 第二级：ViewManager 路由 (Fine-grained)
*   **职责**：在聚合器内部，负责将事件精准投递给具体的驱动实例。
*   **逻辑**：
    *   遍历内部所有 `ViewDriver` 实例。
    *   检查 `Driver.target_schema`：仅当其与 `Event.event_schema` 完全一致时，才调用驱动的 `process_event` 方法。

### 2.3 架构权衡 (Trade-offs)
*   **性能**：避免了将所有事件广播给所有 Handler 导致的无效 CPU 消耗和 Pydantic 反序列化开销。
*   **安全性**：防止了非法或不兼容的 Schema 数据污染视图（例如：防止将 DB 事件错误地传给 FS 内存树）。
*   **灵活性**：保留了 `view-manager` 作为聚合器的设计，允许一个 Pipe 通过插件化方式扩展多种视图。

---

## 3. 字段映射与投影语义 (Field Mapping & Projection)

### 3.1 背景
Agent 的 `fields_mapping` 配置允许用户在数据离开 Agent 前对事件字段进行重命名、类型转换和裁剪。这在异构数据源接入同一 Fusion View 时尤为重要。

### 3.2 决策：投影语义 (Projection Semantic)

`fields_mapping` 采用 **"投影"** 模式：

*   **已配置 `fields_mapping`（列表非空）**：输出中 **仅包含映射规则显式声明的字段**。未在规则中出现的源字段将被静默丢弃。
*   **未配置 `fields_mapping`（列表为空或缺省）**：Mapper 为透明直通，所有字段原样传输，不做任何变换。

> **关键不变量**：  
> `len(fields_mapping) == 0  ⟹  event_out ≡ event_in`  
> `len(fields_mapping) > 0   ⟹  keys(event_out) ⊆ { m.to | m ∈ fields_mapping }`

### 3.3 配置格式

```yaml
pipes:
  my-pipe:
    source: shared-fs
    sender: fusion-main
    fields_mapping:
      - to: "path"                    # 目标字段名
        source: ["path:string"]       # 源字段名[:类型转换]
      - to: "modified_time"
        source: ["modified_time:number"]
      - to: "custom_size"
        source: ["size:integer"]      # size → custom_size (重命名)
      - to: "label"
        hardcoded_value: "production" # 硬编码常量
```

### 3.4 设计权衡

*   **数据安全**：投影语义确保只有用户明确声明的字段被传输，防止敏感字段意外泄漏。
*   **使用注意**：配置 `fields_mapping` 时，用户**必须**列出所有需要传输的字段。遗漏任何字段（如 `size`、`is_directory`）将导致该字段不被传输，Fusion 端对应值为默认值（0 / None / False）。
