commit cee83167236889931affa6d02ecb1c012490c48e
Author: Your Name <you@example.com>
Date:   Sun May 3 08:09:12 2026 +0800

    fs-meta: merge demo assets with env-driven setup

diff --git a/fs-meta/docs/DEMO_ENV_STATUS_AND_GAP.md b/fs-meta/docs/DEMO_ENV_STATUS_AND_GAP.md
new file mode 100644
index 0000000..f165c62
--- /dev/null
+++ b/fs-meta/docs/DEMO_ENV_STATUS_AND_GAP.md
@@ -0,0 +1,187 @@
+# fs-meta Demo 环境现状与缺口说明
+
+## 1. 目的
+
+这份文档用于交接当前 `fs-meta` demo 环境的真实状态，说明：
+
+- 已经打通了哪些环节
+- 当前还缺什么
+- 缺口是实现问题、环境问题，还是约束冲突
+- 后续推进需要放开什么前提
+
+本文档不替代 `fs-meta` 正式手册，只描述当前这次 5 节点 demo 实操结果。
+
+## 2. 当前环境边界
+
+当前讨论中的 demo 运行节点不写死在仓库里，由现场本地环境变量注入：
+
+- `FSMETA_DEMO_HOST_1`
+- `FSMETA_DEMO_HOST_2`
+- `FSMETA_DEMO_HOST_3`
+- `FSMETA_DEMO_HOST_4`
+- `FSMETA_DEMO_HOST_5`
+
+当前约束明确为：
+
+- `fs-meta` 仍按单一产品边界部署
+- worker 运行模式按 `embedded`
+- 不改现有集群挂载结构
+
+控制机或仓库机不计入运行面。
+
+## 3. 已完成项
+
+### 3.1 capanix 5 节点底座
+
+已在 `FSMETA_DEMO_HOST_1..5` 对应节点上准备并运行兼容目标系统环境的 `capanix` 二进制。
+
+当前结果：
+
+- 5 节点 `cluster status` 为 `cluster_ready`
+- 5 节点都 `reachable=true`
+
+这说明：
+
+- 底座节点组网正常
+- 管理控制面正常
+- 基本 runtime 机制正常
+
+### 3.2 fs-meta 可执行产物
+
+已在 `FSMETA_DEMO_ENTRY_HOST` 对应入口节点本机编译出兼容当前环境的产物：
+
+- `capanixd`
+- `cnxctl`
+- `fsmeta`
+- `libfs_meta_runtime.so`
+
+并已把这套兼容版产物下发到各运行节点。
+
+### 3.3 fs-meta deploy
+
+`fsmeta deploy` 已经成功执行，返回：
+
+- `status = ok`
+- `app_id = fs-meta`
+- `api_facade_resource_id = fs-meta-tcp-listener`
+- bootstrap 管理员账号 `admin`
+- 自动生成的初始密码
+
+这说明：
+
+- deploy 编译路径已经可用
+- relation target 已提交进 runtime
+- `fs-meta` app 已被 runtime 接受
+
+### 3.4 facade API
+
+`FSMETA_DEMO_API_BASE` 对应的 `fs-meta` API 已经可以对外提供标准 HTTP 服务。
+
+已验证：
+
+- `POST /api/fs-meta/v1/session/login` 正常
+- `GET /api/fs-meta/v1/status` 正常
+- `POST /api/fs-meta/v1/query-api-keys` 正常
+
+这说明：
+
+- facade 绑定成功
+- auth 物化成功
+- 产品管理 API 可用
+
+## 4. 当前未完成项
+
+### 4.1 runtime grants 仍不完整
+
+虽然 5 节点底座正常，且多个节点本地 `resource announce` 已成功，但当前 `FSMETA_DEMO_ENTRY_HOST` 上运行中的 `fs-meta` 所看到的 `/runtime/grants` 仍然只有本机资源：
+
+- `panda145::panda145-...` 对应 `/mnt/fustor-peers/nfs146`
+- `panda145::fs-meta-tcp-listener`
+
+当前缺失的目标资源包括：
+
+- `panda144` 的 NFS 资源
+- `panda146` 的 NFS 资源
+- `panda147` 的 NFS 资源
+- `panda148` 的 NFS 资源
+
+### 4.2 monitoring roots 无法按 5 组写入
+
+因为 `/runtime/grants` 不完整，当前 5-group roots 只能 `preview` 出 unmatched，不能成功 `apply`。
+
+结果是：
+
+- `roots_count = 0`
+- `sink.groups = []`
+- 无法进入 5-group 聚合统计
+
+### 4.3 10 亿文件 demo 未闭环
+
+当前不能展示：
+
+- 5 个 logical groups 同时接入
+- 5 组总量汇总
+- 10 亿文件规模说明
+
+因为前提 `roots apply` 尚未完成。
+
+## 5. 已排除的问题
+
+下面这些问题都已经被确认不是当前主阻塞：
+
+- 二进制 `glibc` 不兼容
+- `route_plans` 配置不兼容
+- `CAPANIX_ADMIN_SK_B64` 缺失
+- `runtime.exec.source` state carrier 未 opt-in
+- `fs-meta-tcp-listener` 资源缺失
+- `weed` 占用 `18080` 导致 facade 无法接管
+- `fsmeta` / `cnxctl` / `libfs_meta_runtime.so` 路径不一致
+- `fs-meta` API 本身不可访问
+
+也就是说，当前阻塞已经不是底座、端口、编译、签名或基本 deploy 问题。
+
+## 6. 当前根因判断
+
+当前根因是：
+
+- 在“worker 用 embedded”且“不改现有挂载结构”的前提下
+- 当前 active facade 所在节点是 `FSMETA_DEMO_ENTRY_HOST`
+- `fs-meta` 当前稳定消费到的是本机可见资源
+- 跨节点资源没有在当前这套运行链中完整变成 active app 可见的 `/runtime/grants`
+
+因此当前环境状态是：
+
+- 单机产品面正常
+- 分布式 5 组产品面未闭环
+
+## 7. 现阶段最准确的结论
+
+不能把当前状态表述为：
+
+- “整个 demo 测试环境正常运行”
+
+更准确的说法是：
+
+- `capanix` 5 节点底座正常
+- `fs-meta` 单机产品面正常
+- 5 节点 / 5 组 / 10 亿文件 的完整 demo 还未打通
+
+## 8. 若要真正闭环，需要放开的前提
+
+当前只剩两条方向，必须至少放开其中一条：
+
+### 方向 A：允许调整运行模型
+
+让 `source/sink` 以真正分布式承载方式运行，而不是把当前 active app 限在单节点可见资源上。
+
+### 方向 B：允许调整资源可见性结构
+
+让 active facade 所在节点本机可见全部 5 个 NFS 数据源，从而在不改应用运行模型的前提下完成 5-group 接入。
+
+如果这两个前提都不放开，则当前这套 5 组 demo 无法在现有边界内彻底闭环。
+
+## 9. 当前可对外表述
+
+如果需要对内同步当前进展，可以使用下面这段话：
+
+> 当前 `capanix` 5 节点底座和 `fs-meta` 产品管理/API 能力已经打通，`deploy`、登录、状态查询和 query key 管理均可用；但 5 组文件资源尚未完整进入 active app 的 `/runtime/grants`，因此 5-group roots 与 10 亿文件规模演示尚未闭环。当前剩余问题已收敛到资源可见性/运行模型层，不再是端口、编译、签名或 deploy 基础问题。
