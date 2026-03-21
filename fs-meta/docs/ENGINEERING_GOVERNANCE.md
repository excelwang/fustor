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
2. `fs-meta/lib/` 是唯一的开发者-facing fs-meta authoring package；它保持 `publish = false`，暴露 product-specific config/types、API/query/source/sink 语义与 bounded authoring surface，而不要求开发者直接依赖 runtime/deploy seams。
3. `fs-meta/app/` 是内部 `fs-meta-runtime` package；它拥有 ordinary `create_unit` entry、shared external worker module entry `capanix_run_worker_module(...)`、runtime host glue、worker bootstrap glue 和 fixture binary `fs_meta_api_fixture`。
4. `fs-meta/deploy/` 是内部 `fs-meta-deploy` package；它拥有 release/deploy 文档生成与产品拓扑编译，不拥有业务 handler 语义。
5. Helper-only upstream worker bootstrap/transport support remains owned beneath `capanix-runtime-host-sdk`; fs-meta packages consume that support through bounded runtime-host helper surfaces rather than importing the low-level helper crate directly.
8. `fs-meta/tooling/` 拥有 operator CLI binaries 和 optional local-dev daemon composition；它依赖 bounded `fs-meta` / `fs-meta-deploy` surfaces，但不拥有 worker bootstrap 或 runtime planning semantics。

## ENGINEERING_GOVERNANCE.DEPENDENCY_RULES

1. `fs-meta/lib/` 不直接依赖 `capanix-kernel-api`、`capanix-runtime-api`、`capanix-config`、`capanix-unit-entry-macros` 或 low-level external-worker bridge crates；`fs-meta/app/` 和 `fs-meta/deploy/` 也遵守同样的限制，这些能力必须通过 `capanix-service-sdk` / `capanix-runtime-host-sdk` / `capanix-deploy-sdk` 这样的高层 surfaces 进入 fs-meta。
2. `fs-meta/app/` 作为 `fs-meta-runtime` package 通过 `capanix-runtime-host-sdk` 提供的 entry、runtime control/grant、route/state/channel、worker-hosting helper 来导出 product artifact entries，同时通过 `capanix-service-sdk` 保持 primitive-first authoring，而不得回收产品 deploy authority。
3. `fs-meta/deploy/` 通过 `capanix-deploy-sdk` 消费 release/deploy 编译能力，而不是直接依赖 `capanix-config` 和 `capanix-runtime-api`。
4. `fs-meta/tooling/` 可以依赖 bounded `fs-meta` / `fs-meta-deploy` types 和 optional daemon/bootstrap seams，但不得依赖 worker runtime internals 或重写 worker bootstrap。
5. Helper-only upstream worker bootstrap support仍负责 canonical `Timeout` / `TransportClosed` 分类与 wall-clock timeout clipping；fs-meta 本地 crate 通过 `capanix-runtime-host-sdk` 等更高层 helper 消费它，而不重定义这些 transport/bootstrap 语义。

## ENGINEERING_GOVERNANCE.VALIDATION_WORKFLOW

1. `fs-meta/scripts/validate_specs.sh` 是 formal specs tree 的标准校验入口。
2. 校验流程先运行 repo-local precheck，再运行 upstream vibespec validator。
3. repo-local precheck 负责：
   - formal spec front matter 统一性
   - `L0-GLOSSARY` 不出现规范性 RFC2119 语句
   - `L0-VISION` 不重新定义实现细节和 package/runtime vocabulary
4. upstream 对齐审计记录在 `fs-meta/docs/UPSTREAM_SPEC_ALIGNMENT.md`；该文档用于人工 review，而不是自动校验输入。
