# fs-meta Formal Specs

本目录是 `fs-meta` 的**唯一正式规格树**。

## 正式规格入口

1. [L0-GLOSSARY.md](./L0-GLOSSARY.md)：术语边界。
2. [L0-VISION.md](./L0-VISION.md)：产品目标、app/cli 边界与演化方向。
3. [L1-CONTRACTS.md](./L1-CONTRACTS.md)：黑盒契约、app package 合约、cli/tooling 合约。
4. [L2-ARCHITECTURE.md](./L2-ARCHITECTURE.md)：域架构、app package 分层、cli/tooling 边界、repo/specs 布局规则。
5. [L3-RUNTIME/API_HTTP.md](./L3-RUNTIME/API_HTTP.md)：当前 fs-meta 管理 API。
6. [L3-RUNTIME/WORKFLOWS.md](./L3-RUNTIME/WORKFLOWS.md)：部署、登录、grants、roots、rescan 工作流。
7. [L3-RUNTIME/OBSERVATION_CUTOVER.md](./L3-RUNTIME/OBSERVATION_CUTOVER.md)：cutover / observation eligibility / stale fencing 运行时流程。
8. [L3-RUNTIME/WORKER_RUNTIME_SUPPORT.md](./L3-RUNTIME/WORKER_RUNTIME_SUPPORT.md)：worker bootstrap / transport supervision / source-side scan unit realization。

## 非规格材料位置

1. 产品部署文档：`../docs/PRODUCT_DEPLOYMENT.md`
2. 用户示例配置：`../docs/examples/`
3. 工程治理与规格校验规则：`../docs/ENGINEERING_GOVERNANCE.md`
4. 上游 capanix 规格对齐审计：`../docs/UPSTREAM_SPEC_ALIGNMENT.md`
5. contract-test / regression yaml 与 release 示例：`../testdata/specs/`
6. 可运行 fixture apps / manifests / runtime artifacts：`../fixtures/`
7. 规格校验脚本：`../scripts/validate_specs.sh`

## 作用域

1. 本目录只记录正式 L0-L3 规格，不再承载并列 `app/` 或 `cli/` 子规格树。
2. 本目录不再放工程治理、产品部署手册、contract-test 配置或 release 示例。
3. 产品行为、域边界与运行时 ownership 评审以本目录中的主规格为唯一 formal authority。
