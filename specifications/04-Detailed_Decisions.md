---
# Deverloper Newbie: 架构重构待确认细节事项

## 1. Schema 相关

| # | 问题 | 选项/说明 |
|---|------|---------|
| 1.1 | `fustor-schema-*` 包应该包含什么？ | A) 只有 Pydantic 模型<br>B) 模型 + 序列化/反序列化工具<br>C) 模型 + 验证器 + 工具 |
| 1.2 | Schema 版本不兼容时，Fusion 应该如何处理？ | A) 拒绝并返回错误<br>B) 尝试降级处理<br>C) 记录警告但继续 |
| 1.3 | 第三方定义新 Schema 的流程是什么？ | 需要注册到某处？还是只需遵循格式约定？ |

---

## 2. Pipeline 相关

| # | 问题 | 选项/说明 |
|---|------|---------|
| 2.1 | 一个 **Fusion Pipeline** 是否可以绑定多个 **View**？ | A) 1:1（每个 Pipeline 固定一个 View）<br>B) 1:N（一个 Pipeline 分发到多个 View） |
| 2.2 | 一个 **View** 是否可以接收多个 **Fusion Pipeline** 的事件？ | A) 1:1<br>B) N:1（多个 Pipeline 聚合到一个 View） |
| 2.3 | **Receiver** 与 **Fusion Pipeline** 的关系？ | A) 1:1（每个 Receiver 独占一个 Pipeline）<br>B) 1:N（一个 Receiver 可服务多个 Pipeline）<br>C) N:1（多个 Receiver 可指向同一 Pipeline） |

---

## 3. 并发与一致性

| # | 问题 | 选项/说明 |
|---|------|---------|
| 3.1 | 多个 **Session** 同时写入同一 **View**，如何处理冲突？ | A) LogicalClock 仲裁（谁的 mtime 更新谁生效）<br>B) 最后写入者胜出<br>C) 其他机制？ |
| 3.2 | **Leader/Follower** 机制是否保留？ | A) 保留（Snapshot 只接受 Leader）<br>B) 废弃（所有 Session 平等） |
| 3.3 | **LogicalClock** 是哪个层级的组件？ | A) View 级别（每个 View 一个）<br>B) Pipeline 级别<br>C) Session 级别（每个 Session 独立） |
| 3.4 | **审计周期**（audit start/end）是哪个层级的？ | A) Session 级别（每个 Session 独立审计）<br>B) View 级别（所有 Session 共享一个审计状态）<br>C) Pipeline 级别 |

---

## 4. Session 生命周期

| # | 问题 | 选项/说明 |
|---|------|---------|
| 4.1 | Session 关闭（正常或超时）后，**View 状态**如何处理？ | A) 保留内存树，等待新 Session<br>B) 清空内存树<br>C) 取决于配置 |
| 4.2 | **所有 Session 都关闭**后，View 应该做什么？ | A) 保持待命状态<br>B) 重置盲区列表但保留树<br>C) 完全重置 |
| 4.3 | Session 超时设置在哪里配置？ | A) Pipeline 配置<br>B) Receiver 配置<br>C) 全局配置 |

---

## 5. API Key 与路由

| # | 问题 | 选项/说明 |
|---|------|---------|
| 5.1 | 一个 **API Key** 可以访问多个 **Fusion Pipeline** 吗？ | A) 1:1（每个 Key 固定一个 Pipeline）<br>B) 1:N（一个 Key 可访问多个 Pipeline） |
| 5.2 | 如果 5.1 是 1:N，如何在请求中指定目标 Pipeline？ | A) URL 路径中<br>B) 请求头中<br>C) 请求体中 |
| 5.3 | 废弃 Datastore 后，原来的 `datastore_id` 概念如何映射？ | A) 用 pipeline_id 替代<br>B) 用 view_id 替代<br>C) 用 receiver_id 替代 |

---

## 6. 现有功能保留

| # | 问题 | 当前实现 | 新架构中是否保留？ |
|---|------|---------|------------------|
| 6.1 | **Sentinel 机制**（定期检查 suspect 列表） | 有 | ？ |
| 6.2 | **Blind-spot 列表** | View 级别 | ？ |
| 6.3 | **Suspect 列表** | View 级别 | ？ |
| 6.4 | **Tombstone 列表** | View 级别 | ？ |
| 6.5 | **审计跳过优化**（parent mtime 未变则跳过） | Source 级别 | ？ |
| 6.6 | **热文件检测**（hot_file_threshold） | View 级别 | ？ |
| 6.7 | **断点续传**（从 latest_index 恢复） | Datastore 级别 | ？移到 Session？ |

---

## 7. 包职责边界

| # | 问题 | 建议位置 | 需确认？ |
|---|------|---------|---------|
| 7.1 | **LogicalClock** | fustor-core/clock/ | ✅ 通用，不应依赖 FS |
| 7.2 | **Handler 抽象** | fustor-core/pipeline/ | ？ |
| 7.3 | **Sender/Receiver 抽象** | fustor-core/transport/ | ？ |
| 7.4 | **仲裁器 (Arbitrator)** | fustor-view-fs (FS 特有) | ？还是 core？ |
| 7.5 | **一致性状态 (Suspect, Blind-spot)** | fustor-view-fs (FS 特有) | ？还是 core？ |

---

## 8. 配置文件

| # | 问题 | 选项/说明 |
|---|------|---------|
| 8.1 | Agent 的 `syncs-config/` 重命名为 `pipelines-config/`？ | 确认 |
| 8.2 | Agent 的 [pushers-config.yaml](cci:7://file:///home/huajin/fustor_monorepo/examples/yaml-config/pushers-config.yaml:0:0-0:0) 重命名为 `senders-config.yaml`？ | 确认 |
| 8.3 | Fusion 废弃 [datastores-config.yaml](cci:7://file:///home/huajin/fustor_monorepo/examples/yaml-config/datastores-config.yaml:0:0-0:0)？ | 确认 |
| 8.4 | Fusion 新增 `receivers-config.yaml`？ | 确认 |
| 8.5 | Fusion 新增 `pipelines-config/`？ | 确认 |

---

## 9. API 变更

| # | 问题 | 当前路径 | 新路径？ |
|---|------|---------|---------|
| 9.1 | Session 创建 | `/api/v1/ingest/sessions/` | 保持？ |
| 9.2 | 事件推送 | `/api/v1/ingest/events/` | 保持？ |
| 9.3 | 心跳 | `/api/v1/ingest/sessions/heartbeat` | 保持？ |
| 9.4 | 审计信号 | `/api/v1/ingest/consistency/audit/*` | 保持？ |
| 9.5 | View 查询 | `/api/v1/views/{view_id}/*` | 保持？ |

---

## 10. 其他

| # | 问题 | 说明 |
|---|------|------|
| 10.1 | 重构的优先级和阶段划分？ | 先重构哪些模块？ |
| 10.2 | 是否需要保持向后兼容？ | 您说不考虑兼容性，但 API 呢？ |
| 10.3 | 重构是否需要同步更新 CI/CD 和集成测试？ | 现有测试如何迁移？ |

---

## 优先需要您确认的关键问题

1. **2.1 / 2.2 / 2.3**: Receiver → Pipeline → View 的关系基数
2. **3.2**: Leader/Follower 是否保留
3. **3.3 / 3.4**: LogicalClock 和审计周期的层级
4. **5.3**: datastore_id 的替代策略
5. **6.x**: 现有一致性功能是否全部保留
6. **7.4 / 7.5**: 仲裁器和一致性状态是 FS 特有还是通用

---

请逐一确认或补充！

# Deverloper Senior: 确认如下

1.1 A
1.2  B
1.3  发布一个符合约束的python 包。
2.1 B
2.2 B
2.3 B
3.1 要遵循一致性方案
3.2 Leader/Follower只是fs-source与fs-view之间的特殊设计
3.3 A，服务于一致性方案
3.4 B，audit只是fs-source与fs-view之间的特殊设计，服务于一致性方案（由一致性方案决定那个具体的session来审计）
4.1 提供接口让各视图自行决定。
4.2 如何视图是live类型的则选C，否者选A。
4.3 A。由fusion一侧决定后通过agent创建session请求的响应消息告知agent。agent发送心跳的间隔为超时值的一半。
5.1 api key是transport级别的，不是pipeline级别的吧，因此是B
5.2 pipeline是上层的业务逻辑，身份认证是下层的传输机制，sender/receiver与pipeline的对应关系在配置文件里，而不需要在消息体或路径上配置。
5.3 用view_id替代。
6.1 保留在view-fs和source-fs。
6.2、6.3、6.4、6.6 保留在view-fs
6.5 保留在source-fs、view-fs
6.7 pipeline级别的。
7 采纳全部建议。
8.1 agent-pipes-config/
8.2、8.3、8.4 OK
8.5 fusion-pipes-config/
9 /api/v1/ingest-> /api/v1/pipe，9.4、9.5 OK
10  先重构抽象的基础的模块，然后是核心业务逻辑，然后是驱动，完全不考虑兼容性。最后更新测试。

### Planner Response

感谢您的详细确认！让我整理最终设计并创建正式文档。

---

# Deverloper Senior:  最终设计决策总结

## 确认的设计决策

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           核心关系确认                                                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   Receiver : Fusion Pipeline = 1 : N                                                 │
│   (一个 Receiver 可服务多个 Pipeline)                                                 │
│                                                                                      │
│   Fusion Pipeline : View = 1 : N                                                     │
│   (一个 Pipeline 可分发到多个 View)                                                   │
│                                                                                      │
│   View : Fusion Pipeline = N : 1                                                     │
│   (一个 View 可接收多个 Pipeline 的事件)                                               │
│                                                                                      │
│   ───────────────────────────────────────────────────────────────────────────────── │
│                                                                                      │
│   API Key 是 Transport 级别 (Receiver 持有)                                           │
│   Pipeline 与 Receiver 的对应关系在配置文件中                                          │
│   用 view_id 替代 datastore_id                                                       │
│                                                                                      │
│   ───────────────────────────────────────────────────────────────────────────────── │
│                                                                                      │
│   一致性组件层级:                                                                     │
│   • LogicalClock: View 级别                                                          │
│   • Leader/Follower: fs 特有 (view-fs)                                               │
│   • 审计周期: View 级别, fs 特有                                                       │
│   • 断点续传: Pipeline 级别                                                           │
│                                                                                      │
│   ───────────────────────────────────────────────────────────────────────────────── │
│                                                                                      │
│   Session 生命周期:                                                                   │
│   • 超时配置在 Pipeline, 通过创建响应告知 Agent                                        │
│   • 关闭后由 View 接口自行决定状态处理                                                  │
│   • 所有 Session 关闭: live 类型清空, 否则保留                                         │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘




# ✅ 设计决策确认表

## Schema

| # | 问题 | 确认答案 |
|---|------|---------|
| 1.1 | Schema 包内容 | **A) 只有 Pydantic 模型** |
| 1.2 | 版本不兼容处理 | **B) 尝试降级处理** |
| 1.3 | 第三方扩展方式 | **发布符合约束的 Python 包** |

## Pipeline 关系

| # | 问题 | 确认答案 |
|---|------|---------|
| 2.1 | Fusion Pipeline : View | **B) 1:N（一个 Pipeline 分发到多个 View）** |
| 2.2 | View : Fusion Pipeline | **B) N:1（一个 View 可接收多个 Pipeline）** |
| 2.3 | Receiver : Fusion Pipeline | **B) 1:N（一个 Receiver 可服务多个 Pipeline）** |

## 并发与一致性

| # | 问题 | 确认答案 |
|---|------|---------|
| 3.1 | 多 Session 冲突处理 | **遵循一致性方案（LogicalClock 仲裁）** |
| 3.2 | Leader/Follower | **fs 特有设计，保留在 view-fs** |
| 3.3 | LogicalClock 层级 | **A) View 级别** |
| 3.4 | 审计周期层级 | **B) View 级别，fs 特有** |

## Session 生命周期

| # | 问题 | 确认答案 |
|---|------|---------|
| 4.1 | Session 关闭后 View 状态 | **提供接口让 View 自行决定** |
| 4.2 | 所有 Session 关闭后 | **live 类型清空，否则保留** |
| 4.3 | Session 超时配置位置 | **Pipeline 配置，通过响应告知 Agent** |

## API Key 与路由

| # | 问题 | 确认答案 |
|---|------|---------|
| 5.1 | API Key 访问范围 | **B) Transport 级别，1:N** |
| 5.2 | Pipeline 路由方式 | **配置文件中定义，不在消息中** |
| 5.3 | datastore_id 替代 | **用 view_id 替代** |

## 现有功能保留

| # | 功能 | 保留位置 |
|---|------|---------|
| 6.1 | Sentinel 机制 | view-fs, source-fs |
| 6.2-6.4, 6.6 | Blind-spot, Suspect, Tombstone, 热文件 | view-fs |
| 6.5 | 审计跳过优化 | source-fs, view-fs |
| 6.7 | 断点续传 | **Pipeline 级别** |

## 配置目录

| 组件 | Agent | Fusion |
|------|-------|--------|
| Sender/Receiver | `senders-config.yaml` | `receivers-config.yaml` |
| Pipeline | `agent-pipes-config/` | `fusion-pipes-config/` |
| datastores-config | - | **(废弃)** |

## API 路径

| 改动 | Before | After |
|------|--------|-------|
| 摄取基路径 | `/api/v1/ingest` | `/api/v1/pipe` |