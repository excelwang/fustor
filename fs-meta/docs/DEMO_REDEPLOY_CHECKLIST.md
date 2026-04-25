# fs-meta Demo 重部署清单

适用场景：

- 现场 demo 环境使用 `10.0.82.145` 作为操作入口
- `10.0.82.14x` 为 CentOS 7，不能直接使用本机编译产物
- 需要使用 `145` 本机编译出的 `fsmeta` 工具重新提交 deploy
- 目标是验证这次 deploy/compiler 修正是否能恢复分布式 demo 主链

不覆盖：

- release-upgrade 演示
- external worker 路径
- 非 demo real-NFS 长尾场景

## 1. 前提

登录 `145`：

```bash
ssh wanghuajin@10.0.82.145
```

进入现场源码树：

```bash
cd /tmp/fsmeta-build-145/fustor
```

加载现场控制面环境变量：

```bash
source /tmp/fsmeta-run2/run/nodes/panda145/node.env
```

确认本次要使用的二进制已经在 `145` 上构建完成：

```bash
./target/release/fsmeta --help
./target/release/fsmeta-locald --help
```

当前默认关键路径：

- deploy config: `/tmp/fsmeta-run2/run/fs-meta-embedded.yaml`
- control socket: `/tmp/fsmeta-run2/run/nodes/panda145/home/core.sock`
- node env: `/tmp/fsmeta-run2/run/nodes/panda145/node.env`

## 2. 记录重部署前状态

先把现场当前状态保存下来，便于回比：

```bash
mkdir -p /tmp/fsmeta-redeploy-audit

./target/release/fsmeta --output json deploy --help >/tmp/fsmeta-redeploy-audit/deploy-help.txt 2>&1 || true

/tmp/fsmeta-run2/run/bin/cnxctl cluster status \
  > /tmp/fsmeta-redeploy-audit/cluster-status-before.json

/tmp/fsmeta-run2/run/bin/cnxctl process list \
  > /tmp/fsmeta-redeploy-audit/process-list-before.json

curl -sS http://10.0.82.145:18080/api/fs-meta/v1/status \
  > /tmp/fsmeta-redeploy-audit/status-before.json || true
```

如果手上已有当前 admin 密码，也一并记录 deploy 前 grants：

```bash
TOKEN="$(curl -sS -X POST http://10.0.82.145:18080/api/fs-meta/v1/session/login \
  -H 'content-type: application/json' \
  -d '{"username":"admin","password":"<CURRENT_PASSWORD>"}' | jq -r '.token')"

curl -sS http://10.0.82.145:18080/api/fs-meta/v1/runtime/grants \
  -H "authorization: Bearer $TOKEN" \
  > /tmp/fsmeta-redeploy-audit/runtime-grants-before.json
```

## 3. 用 145 本机编译的 fsmeta 重新 deploy

建议把 deploy 输出完整保存：

```bash
DEPLOY_OUT=/tmp/fsmeta-redeploy-audit/deploy-$(date +%Y%m%d-%H%M%S).json

./target/release/fsmeta --output json deploy \
  --socket /tmp/fsmeta-run2/run/nodes/panda145/home/core.sock \
  --config /tmp/fsmeta-run2/run/fs-meta-embedded.yaml \
  | tee "$DEPLOY_OUT"
```

重点关注 deploy 输出里的：

- `status`
- `app_id`
- `api_facade_resource_id`
- `bootstrap_management.username`
- `bootstrap_management.password`

如果这次 deploy 生成了新的 bootstrap 管理员密码，后续步骤统一使用新密码。

## 4. 重部署后立即核验 runtime / realization

先看 runtime 是否重新挂起：

```bash
/tmp/fsmeta-run2/run/bin/cnxctl process list
```

尤其关注：

- `panda145` 是否有 `fs-meta` active process
- `panda146` 是否从 `spawned=0 / registry empty` 变成真正有 active realization

同时看 `146` 本机：

```bash
ssh wanghuajin@10.0.82.146 '
  source /data/huajin/fsmeta-run2/run/nodes/panda146/node.env
  /data/huajin/fsmeta-run2/run/bin/cnxctl process list
  cat /data/huajin/fsmeta-run2/run/nodes/panda146/home/registry.json
'
```

## 5. 登录 fs-meta 管理面并检查 runtime grants

```bash
TOKEN="$(curl -sS -X POST http://10.0.82.145:18080/api/fs-meta/v1/session/login \
  -H 'content-type: application/json' \
  -d '{"username":"admin","password":"<DEPLOY_OUTPUT_PASSWORD>"}' | jq -r '.token')"

curl -sS http://10.0.82.145:18080/api/fs-meta/v1/runtime/grants \
  -H "authorization: Bearer $TOKEN" | tee /tmp/fsmeta-redeploy-audit/runtime-grants-after.json
```

这一步的目标不是“接口能通”，而是确认 grants 不再只有本机 2 条，而是能看到 5 组 demo 资源。

## 6. preview / apply 5-group monitoring roots

如果现场已有 5-group roots JSON，就直接使用；否则使用约定好的 5-group payload 文件。

preview：

```bash
curl -sS -X POST http://10.0.82.145:18080/api/fs-meta/v1/monitoring/roots/preview \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  --data @/path/to/monitoring-roots-5group.json \
  | tee /tmp/fsmeta-redeploy-audit/roots-preview-after.json
```

apply：

```bash
curl -sS -X PUT http://10.0.82.145:18080/api/fs-meta/v1/monitoring/roots \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  --data @/path/to/monitoring-roots-5group.json \
  | tee /tmp/fsmeta-redeploy-audit/roots-apply-after.json
```

预期：

- preview 不再出现 demo 5 组资源的 `unmatched_roots`
- apply 成功

## 7. 必要时触发 rescan

```bash
curl -sS -X POST http://10.0.82.145:18080/api/fs-meta/v1/index/rescan \
  -H "authorization: Bearer $TOKEN" \
  | tee /tmp/fsmeta-redeploy-audit/rescan-after.json
```

## 8. 最终验收

至少确认：

1. `145` 的 `/runtime/grants` 不再只有本机 2 条
2. `146` 本地不再是 `registry empty + spawned=0`
3. 5-group roots 可以 preview / apply
4. `/status` 正常
5. `/tree`、`/stats`、`/on-demand-force-find` 可继续走 demo 主链

## 9. 如果结果仍异常，优先看哪里

先看 `capanix` runtime / realization：

- `cnxctl cluster status`
- `cnxctl process list`
- `run/nodes/<node>/home/registry.json`
- `run/nodes/<node>/daemon.log`

不要先怀疑 roots API 用法。按 specs，正确流程本来就是：

1. `deploy` with `roots=[]`
2. `GET /runtime/grants`
3. `preview/apply roots`

如果这条链还失败，优先看：

- active app 的 `host_object_grants` 是否仍然只有本机资源
- `146/148` 这类节点是否根本没有完成 app realization
