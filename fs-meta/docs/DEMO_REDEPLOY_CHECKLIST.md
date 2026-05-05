commit cee83167236889931affa6d02ecb1c012490c48e
Author: Your Name <you@example.com>
Date:   Sun May 3 08:09:12 2026 +0800

    fs-meta: merge demo assets with env-driven setup

diff --git a/fs-meta/docs/DEMO_REDEPLOY_CHECKLIST.md b/fs-meta/docs/DEMO_REDEPLOY_CHECKLIST.md
new file mode 100644
index 0000000..06a3165
--- /dev/null
+++ b/fs-meta/docs/DEMO_REDEPLOY_CHECKLIST.md
@@ -0,0 +1,218 @@
+# fs-meta Demo 重部署清单
+
+适用场景：
+
+- 现场 demo 环境入口由本地环境变量 `FSMETA_DEMO_ENTRY_HOST` 指定
+- 目标节点系统版本可能与仓库机不同，不能默认直接使用仓库机编译产物
+- 需要使用入口节点本机编译出的 `fsmeta` 工具重新提交 deploy
+- 目标是验证这次 deploy/compiler 修正是否能恢复分布式 demo 主链
+
+不覆盖：
+
+- release-upgrade 演示
+- external worker 路径
+- 非 demo real-NFS 长尾场景
+
+## 1. 前提
+
+先准备本地环境变量。推荐复制并填写本地未提交文件：
+
+```bash
+cp fs-meta/docs/examples/demo-env.example.sh /tmp/fsmeta-demo.env
+$EDITOR /tmp/fsmeta-demo.env
+source /tmp/fsmeta-demo.env
+```
+
+登录入口节点：
+
+```bash
+ssh "$FSMETA_DEMO_ENTRY_SSH"
+```
+
+进入现场源码树：
+
+```bash
+cd "$FSMETA_DEMO_BUILD_ROOT"
+```
+
+加载现场控制面环境变量：
+
+```bash
+source "$FSMETA_DEMO_ENTRY_NODE_ENV"
+```
+
+确认本次要使用的二进制已经在 `145` 上构建完成：
+
+```bash
+./target/release/fsmeta --help
+./target/release/fsmeta-locald --help
+```
+
+当前关键路径由环境变量提供：
+
+- deploy config: `FSMETA_DEMO_DEPLOY_CONFIG`
+- control socket: `FSMETA_DEMO_CONTROL_SOCKET`
+- node env: `FSMETA_DEMO_ENTRY_NODE_ENV`
+
+## 2. 记录重部署前状态
+
+先把现场当前状态保存下来，便于回比：
+
+```bash
+mkdir -p /tmp/fsmeta-redeploy-audit
+
+./target/release/fsmeta --output json deploy --help >/tmp/fsmeta-redeploy-audit/deploy-help.txt 2>&1 || true
+
+"$FSMETA_DEMO_RUN_ROOT/bin/cnxctl" cluster status \
+  > /tmp/fsmeta-redeploy-audit/cluster-status-before.json
+
+"$FSMETA_DEMO_RUN_ROOT/bin/cnxctl" process list \
+  > /tmp/fsmeta-redeploy-audit/process-list-before.json
+
+curl -sS "$FSMETA_DEMO_API_BASE/api/fs-meta/v1/status" \
+  > /tmp/fsmeta-redeploy-audit/status-before.json || true
+```
+
+如果手上已有当前 admin 密码，也一并记录 deploy 前 grants：
+
+```bash
+TOKEN="$(curl -sS -X POST "$FSMETA_DEMO_API_BASE/api/fs-meta/v1/session/login" \
+  -H 'content-type: application/json' \
+  -d '{"username":"admin","password":"<CURRENT_PASSWORD>"}' | jq -r '.token')"
+
+curl -sS "$FSMETA_DEMO_API_BASE/api/fs-meta/v1/runtime/grants" \
+  -H "authorization: Bearer $TOKEN" \
+  > /tmp/fsmeta-redeploy-audit/runtime-grants-before.json
+```
+
+## 3. 用 145 本机编译的 fsmeta 重新 deploy
+
+建议把 deploy 输出完整保存：
+
+```bash
+DEPLOY_OUT=/tmp/fsmeta-redeploy-audit/deploy-$(date +%Y%m%d-%H%M%S).json
+
+./target/release/fsmeta --output json deploy \
+  --socket "$FSMETA_DEMO_CONTROL_SOCKET" \
+  --config "$FSMETA_DEMO_DEPLOY_CONFIG" \
+  | tee "$DEPLOY_OUT"
+```
+
+重点关注 deploy 输出里的：
+
+- `status`
+- `app_id`
+- `api_facade_resource_id`
+- `bootstrap_management.username`
+- `bootstrap_management.password`
+
+如果这次 deploy 生成了新的 bootstrap 管理员密码，后续步骤统一使用新密码。
+
+## 4. 重部署后立即核验 runtime / realization
+
+先看 runtime 是否重新挂起：
+
+```bash
+"$FSMETA_DEMO_RUN_ROOT/bin/cnxctl" process list
+```
+
+尤其关注：
+
+- 入口节点是否有 `fs-meta` active process
+- peer 节点是否从 `spawned=0 / registry empty` 变成真正有 active realization
+
+同时看 peer 节点本机：
+
+```bash
+ssh "$FSMETA_DEMO_PEER_SSH" "
+  source \"$FSMETA_DEMO_PEER_NODE_ENV\"
+  \"$FSMETA_DEMO_PEER_RUN_ROOT/bin/cnxctl\" process list
+  cat \"$FSMETA_DEMO_PEER_REGISTRY\"
+"
+```
+
+## 5. 登录 fs-meta 管理面并检查 runtime grants
+
+```bash
+TOKEN="$(curl -sS -X POST "$FSMETA_DEMO_API_BASE/api/fs-meta/v1/session/login" \
+  -H 'content-type: application/json' \
+  -d '{"username":"admin","password":"<DEPLOY_OUTPUT_PASSWORD>"}' | jq -r '.token')"
+
+curl -sS "$FSMETA_DEMO_API_BASE/api/fs-meta/v1/runtime/grants" \
+  -H "authorization: Bearer $TOKEN" | tee /tmp/fsmeta-redeploy-audit/runtime-grants-after.json
+```
+
+这一步的目标不是“接口能通”，而是确认 grants 不再只有本机 2 条，而是能看到 5 组 demo 资源。
+
+## 6. preview / apply 5-group monitoring roots
+
+如果现场已有 5-group roots JSON，就直接使用；否则先用本地环境变量渲染模板：
+
+```bash
+fs-meta/docs/examples/render-monitoring-roots-5group.sh \
+  fs-meta/docs/examples/monitoring-roots-5group.template.json \
+  "$FSMETA_DEMO_ROOTS_FILE"
+```
+
+preview：
+
+```bash
+curl -sS -X POST "$FSMETA_DEMO_API_BASE/api/fs-meta/v1/monitoring/roots/preview" \
+  -H "authorization: Bearer $TOKEN" \
+  -H 'content-type: application/json' \
+  --data @"$FSMETA_DEMO_ROOTS_FILE" \
+  | tee /tmp/fsmeta-redeploy-audit/roots-preview-after.json
+```
+
+apply：
+
+```bash
+curl -sS -X PUT "$FSMETA_DEMO_API_BASE/api/fs-meta/v1/monitoring/roots" \
+  -H "authorization: Bearer $TOKEN" \
+  -H 'content-type: application/json' \
+  --data @"$FSMETA_DEMO_ROOTS_FILE" \
+  | tee /tmp/fsmeta-redeploy-audit/roots-apply-after.json
+```
+
+预期：
+
+- preview 不再出现 demo 5 组资源的 `unmatched_roots`
+- apply 成功
+
+## 7. 必要时触发 rescan
+
+```bash
+curl -sS -X POST "$FSMETA_DEMO_API_BASE/api/fs-meta/v1/index/rescan" \
+  -H "authorization: Bearer $TOKEN" \
+  | tee /tmp/fsmeta-redeploy-audit/rescan-after.json
+```
+
+## 8. 最终验收
+
+至少确认：
+
+1. 入口节点的 `/runtime/grants` 不再只有本机 2 条
+2. peer 节点本地不再是 `registry empty + spawned=0`
+3. 5-group roots 可以 preview / apply
+4. `/status` 正常
+5. `/tree`、`/stats`、`/on-demand-force-find` 可继续走 demo 主链
+
+## 9. 如果结果仍异常，优先看哪里
+
+先看 `capanix` runtime / realization：
+
+- `cnxctl cluster status`
+- `cnxctl process list`
+- `run/nodes/<node>/home/registry.json`
+- `run/nodes/<node>/daemon.log`
+
+不要先怀疑 roots API 用法。按 specs，正确流程本来就是：
+
+1. `deploy` with `roots=[]`
+2. `GET /runtime/grants`
+3. `preview/apply roots`
+
+如果这条链还失败，优先看：
+
+- active app 的 `host_object_grants` 是否仍然只有本机资源
+- peer 节点是否根本没有完成 app realization
