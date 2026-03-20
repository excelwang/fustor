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
2. `fs-meta/app/` 是唯一的产品 app package；它保持 `publish = false`，拥有 package-local config/types 以及 API、query、orchestration 和 business composition。
3. `fs-meta/runtime-support/` is the only crate that owns worker child-process bootstrap, control/data socket/log path materialization, direct control-plane startup/management, retry clipping, and low-level external-worker transport supervision.
4. `fs-meta/worker-facade/` 拥有 embedded `facade-worker` artifact entry 和 fixture binary surface；它不拥有 business/query semantics。
5. `fs-meta/worker-source/` 拥有 `source-worker` external artifact/runtime entry 和 `run_source_worker_server(...)` bootstrap。
6. `fs-meta/worker-sink/` 拥有 `sink-worker` external artifact/runtime entry 和 `run_sink_worker_server(...)` bootstrap。
7. `fs-meta/worker-scan/` owns the `scan-worker` executable artifact identity and `run_scan_worker_server(...)` entry; it may currently reuse lower-level source-worker runtime helpers internally.
8. `fs-meta/tooling/` 拥有 operator CLI binaries 和 optional local-dev daemon composition；它不拥有 worker bootstrap 或 runtime planning semantics。

## ENGINEERING_GOVERNANCE.DEPENDENCY_RULES

1. `fs-meta/app/` does not depend on `capanix-kernel-api`, worker artifact crates, low-level external-worker bridge crate or embedded-entry macro crate; it may consume only the bounded typed transport/client surface exposed by `fs-meta/runtime-support/`.
2. `fs-meta/tooling/` 可以依赖 bounded `product` types 和 optional daemon/bootstrap seams，但不得依赖 worker runtime internals 或重写 worker bootstrap。
3. `fs-meta/runtime-support/` 可以依赖 low-level external-worker bridge crate 作为 bridge runner；它负责 fs-meta worker bootstrap semantics，并保留 canonical `Timeout` / `TransportClosed` 分类与 wall-clock timeout clipping。
4. `fs-meta/worker-facade/` 可以依赖 `fs-meta/app` 和 embedded-entry macro crate 来做 embedded artifact realization，但 realization wiring 保持 artifact-local。
5. `fs-meta/worker-source/` 与 `fs-meta/worker-sink/` 可以依赖 `fs-meta/app` 和 low-level external-worker bridge crate 承载 external worker servers；app business modules 不得反向依赖这些 artifact crates。
6. `fs-meta/worker-scan/` 可以依赖 `worker-source` 的 lower-level runtime helpers；若 scan runtime semantics 发生分歧，应先更新 formal specs，再改依赖关系。

## ENGINEERING_GOVERNANCE.VALIDATION_WORKFLOW

1. `fs-meta/scripts/validate_specs.sh` 是 formal specs tree 的标准校验入口。
2. 校验流程先运行 repo-local precheck，再运行 upstream vibespec validator。
3. repo-local precheck 负责：
   - formal spec front matter 统一性
   - `L0-GLOSSARY` 不出现规范性 RFC2119 语句
   - `L0-VISION` 不重新定义实现细节和 package/runtime vocabulary
4. upstream 对齐审计记录在 `fs-meta/docs/UPSTREAM_SPEC_ALIGNMENT.md`；该文档用于人工 review，而不是自动校验输入。
