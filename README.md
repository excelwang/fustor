# fustor

Standalone `fustor` workspace extracted from `datanix`.

`fustor` is the top-level project name. `fs-meta` remains the product/domain
package under `fs-meta/`, while the repository root hosts the workspace root
package and repository-level tests.

## Layout

- `fs-meta`: product container directory holding the `fs-meta`, `fs-meta-runtime`,
  `fs-meta-deploy`, and `fs-meta-tooling` packages plus fixtures and specs
- `fs-meta/fixtures`: module fixtures, manifests, and test apps
- `fs-meta/specs`: product and contract specs
- `fs-meta/tests`: domain-level specs and end-to-end tests

## Common Commands

```bash
cargo check
cargo test -p fs-meta --test app_specs -- --nocapture
cargo test -p fs-meta-tooling -- --nocapture
```

## Dependency Overrides

Workspace-internal crates inherit `capanix-*` dependencies from the root
[`Cargo.toml`](/home/huajin/fustor/Cargo.toml) via `workspace = true`.

This repository does not default unresolved `capanix-*` dependencies to any
local checkout. When unpublished upstream crates need to come from a local
checkout such as `../capanix`, override them only at the workspace root with
`[patch.crates-io]`. Do not add `../capanix` path dependencies inside member
crates.

```toml
[patch.crates-io]
capanix-kernel-api = { path = "../capanix/crates/kernel-api" }
capanix-runtime-api = { path = "../capanix/crates/runtime-api" }
```
