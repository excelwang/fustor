# fs-meta Engineering Governance

本文件记录 `fs-meta` 的工程治理规则。它不是 formal L0-L3 规格树的一部分。

## ENGINEERING_GOVERNANCE.REPOSITORY_TOPOLOGY

1. `fs-meta/specs/` 是唯一的 formal fs-meta specification tree；`specs/app/` 和 `specs/cli/` 不再作为并列 authority roots。
2. Formal specs 仅限 `L0-GLOSSARY`、`L0-VISION`、`L1-CONTRACTS`、`L2-ARCHITECTURE` 和 `L3-RUNTIME/*`。
3. 产品与操作文档位于 `fs-meta/docs/`；部署示例位于 `fs-meta/docs/examples/`。
4. contract-test fixtures、release 示例和 regression 材料位于 `fs-meta/testdata/specs/`；它们不是 formal specs。
5. Runnable fixture apps、fixture manifests 和 runtime artifacts 位于 `fs-meta/fixtures/`；它们是测试与示例材料，不是 formal specs。

## ENGINEERING_GOVERNANCE.CRATE_OWNERSHIP

1. `fs-meta/` 是产品容器目录，不是 Cargo package，不拥有代码级业务 authority。
2. `fs-meta/lib/` 是唯一的开发者-facing fs-meta authoring/domain package；它保持 `publish = false`，暴露 bounded reusable product config/types、query/shared contracts 与 product compile-time declarations，而不要求开发者直接依赖 runtime/deploy seams。
3. `fs-meta/app/` 是内部 `fs-meta-runtime` package，也是 canonical deployable runtime artifact package；它拥有 ordinary `create_unit` entry、shared external worker module entry `capanix_run_worker_module(...)`、runtime-entry/bootstrap glue、worker bootstrap glue、fixture binary `fs_meta_api_fixture`，以及最终装配后的 source/sink/query/api 产品实现。
4. `fs-meta/deploy/` 是内部 `fs-meta-deploy` package；它拥有 release/deploy 文档生成与产品拓扑编译，不拥有 deploy-time runtime artifact implementation authority。
5. `fs-meta/lib/` 只解析 product-owned manifest config；runtime-injected `__cnx_runtime` overlay（如 `host_object_grants`、`announced_resources`）只在 `fs-meta/app/` 组装成 app-owned runtime config。
6. Helper-only upstream worker bootstrap/transport support remains owned beneath `capanix-runtime-entry-sdk`; fs-meta packages consume that support through bounded runtime-entry helper surfaces rather than importing the low-level helper crate directly.
7. `fs-meta/tooling/` 拥有 operator CLI binaries 和 optional local-dev daemon composition；它依赖 bounded `fs-meta` / `fs-meta-deploy` surfaces，但不拥有 worker bootstrap 或 runtime planning semantics。

## ENGINEERING_GOVERNANCE.DEPENDENCY_RULES

1. `fs-meta/lib/` 不直接依赖 `capanix-kernel-api`、`capanix-runtime-api`、`capanix-config`、`capanix-unit-entry-macros` 或 low-level external-worker bridge crates；`fs-meta/app/` 和 `fs-meta/deploy/` 也遵守同样的限制，这些能力必须通过 `capanix-service-sdk` / `capanix-runtime-entry-sdk` / internal deploy adapter shim `capanix-deploy-sdk` 这样的高层 surfaces 进入 fs-meta。
2. `fs-meta/app/` 作为 `fs-meta-runtime` package 通过 `capanix-runtime-entry-sdk` 提供的 entry、runtime bootstrap、typed control/grant、worker-hosting helper 来导出 product artifact entries；若确实需要更低层 boundary/state bridge，只能通过显式 `advanced::*` escape hatch 消费，同时通过 `capanix-service-sdk` 保持 primitive-first authoring，而不得回收产品 deploy authority。
3. `fs-meta/deploy/` 通过 internal deploy adapter shim `capanix-deploy-sdk` 消费 release/deploy 编译能力，而不是直接依赖 `capanix-config` 和 `capanix-runtime-api`。
4. `fs-meta/tooling/` 可以依赖 bounded `fs-meta` / `fs-meta-deploy` types 和 optional daemon/bootstrap seams，但不得依赖 worker runtime internals 或重写 worker bootstrap。
5. Helper-only upstream worker bootstrap support 仍负责 canonical `Timeout` / `TransportClosed` 分类、wall-clock timeout clipping、bootstrap/retry/lifecycle supervision；fs-meta 本地 crate 通过 `capanix-runtime-entry-sdk` 等更高层 helper 消费它，而不重定义这些 transport/bootstrap 语义。

## ENGINEERING_GOVERNANCE.IMPLEMENTATION_REALIZATION_NOTES

1. Exact upstream crate names, helper-layer topology, and package naming remain engineering constraints rather than formal L1-L3 authority, as long as the published ownership and behavior seams stay stable.
2. Exact repo/package topology and source-file locations for worker servers, runtime glue, and release-compiler wiring are implementation material; current runtime worker entry files live under `fs-meta/app/src/workers/` and are governed here rather than by formal architecture clauses.
3. Runtime realization currently uses compiled worker bindings and lower startup transport fields beneath the formal worker/mode model; names such as `__cnx_runtime.workers` and `workers.<role>.startup.path/socket_dir` are implementation detail, not product contract vocabulary.
4. Current wrapper/helper entrypoints and platform-owned bootstrap control envelope names, including `RuntimeLoadedServiceApp::from_runtime_config(...)`, `AppBuilder`, `Init`, `Start`, `Ping`, and `Close`, remain implementation material beneath the formal readiness, failure-classification, and shutdown contracts.
5. Package-local implementation tuning knobs such as `FS_META_SOURCE_SCAN_WORKERS`, `FS_META_SOURCE_AUDIT_INTERVAL_MS`, `FS_META_SOURCE_THROTTLE_INTERVAL_MS`, `FS_META_SINK_TOMBSTONE_TTL_MS`, `FS_META_SINK_TOMBSTONE_TOLERANCE_US`, and `FS_META_SOURCE_AUDIT_DEEP_INTERVAL_ROUNDS` are bounded implementation controls and MUST NOT redefine truth, cutover, or public query contracts.

## ENGINEERING_GOVERNANCE.VALIDATION_WORKFLOW

1. `fs-meta/scripts/validate_specs.sh` 是 formal specs tree 的标准校验入口。
2. 校验流程先运行 repo-local precheck，再运行 upstream vibespec validator。
3. repo-local precheck 负责：
   - formal spec front matter 统一性
   - `L0-GLOSSARY` 不出现规范性 RFC2119 语句
   - `L0-VISION` 不重新定义实现细节和 package/runtime vocabulary
4. upstream 对齐要求直接体现在主 specs 与 contract tests 中；不保留独立的上游对齐备忘录。
