# fs-meta Upstream Spec Alignment

本文件记录 `fs-meta` formal specs 与上游 `capanix` specs 的约束对齐结果。

默认优先级：

1. 上游 `capanix` root specs 与直接依赖模块 specs 是 owner 约束源。
2. `fs-meta` 只能消费这些 owner 语义，不能在本地 specs 中重写它们。
3. 若发现上游描述不合理，本轮只记录 follow-up，不反向修改上游仓库。

| Upstream owner | Constrained concept | fs-meta impact | Hard-cut action | Status |
| --- | --- | --- | --- | --- |
| `capanix/specs` root | `Authoritative Truth` vs `Observation`, `STATEFUL_APP_OBSERVATION_PLANE_OPT_IN`, `UNCERTAIN_STATE_MUST_NOT_PROMOTE`, `POST_CUTOVER_STALE_OWNER_FENCING` | cutover、truth/observation、trusted exposure 相关 clauses | 保留 fs-meta 的 app-owned `observation_eligible`，禁止把 truth/exposure owner 写成 fs-meta 自造平台语义 | aligned |
| `crates/app-sdk/specs` + `crates/runtime-api/specs` | ordinary app-facing boundary chain | app scope、runtime glue、typed boundary wording | 统一为 `app-sdk -> runtime-api -> kernel-api -> kernel`；fs-meta 只消费该链路，不重定义 authoring authority | aligned |
| `crates/runtime/specs` | runtime-owned bind/run、route、state carrier、trusted exposure gating | group bind/run、host_object_grants、state carrier、cutover workflows | fs-meta 只消费 runtime-owned carriers 和 proofs；不提升 `host_object_grants`、state carrier 或 trusted exposure 为 domain-owned platform semantics | aligned |
| `crates/host-adapter-sdk/specs` | host-local adaptation boundary | host programming、adapter workflows | fs-meta 只描述 bound-host local programming；不定义 locality resolution 或 distributed host-operation forwarding contract | aligned |
| `crates/host-adapter-fs/specs` | generic bound-host host-fs facade ownership | host-fs facade、watch/session、post-bind dispatch wording | fs-meta 通过 `HostFsFacade` 消费本地 facade；不把 dispatch tags、watch descriptors、session ids 提升为 runtime/platform capability vocabulary | aligned |
| `crates/host-fs-types/specs` | generic host-fs DTO ownership | passthrough carriers、Unix metadata continuity | fs-meta 继续拥有产品 HTTP envelope、record/control/query vocabulary，而 generic host-fs DTO 保持在 `host-fs-types` | aligned |
| `crates/route-proto/specs` | typed coordination carriers only | exec control、grants-changed、exposure-confirmed | fs-meta 只消费 typed carriers 与 stateless validation；route semantics 仍由 runtime owner 定义 | aligned |
| `crates/config/specs` | shared config path precedence, manifest discovery, intent compilation | thin deploy config、deploy docs、product config split | fs-meta 只声明 product bootstrap input，不重写 shared config / manifest / intent semantics | aligned |
| `crates/daemon/specs` + `crates/cli/specs` | deploy/ingress client non-authority boundary | tooling docs、local-dev launcher、CLI scope | fs-meta tooling 保持 request/deploy client 角色，不回收 daemon/runtime/kernel authority | aligned |

## Review Notes

1. `fs-meta/specs/L0-*` 只保留 fs-meta 域的 why/term，避免把上游 owner vocabulary 重新写成本地平台 authority。
2. `fs-meta/specs/L2-ARCHITECTURE.md` 只保留产品/runtime architecture；repo topology、crate ownership 和 dependency rules 已移到 `ENGINEERING_GOVERNANCE.md`。
3. `fs-meta/specs/L3-RUNTIME/*` 中涉及 state carrier、worker bootstrap、adapter bridge、deploy config 的条目均按 upstream owner 语义重写为 “consume, not redefine”。
