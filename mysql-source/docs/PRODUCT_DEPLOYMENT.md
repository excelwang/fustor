# mysql-source Deployment Contract

`mysql-source` is an independent Fustor/Capanix app for MySQL table reads. It follows the `fs-meta` source-style architecture: Capanix owns route/channel/runtime evidence, while `mysql-source` owns typed request/response payloads, MySQL endpoint auth resolution, snapshot session semantics, and error payloads.

## App Manifest

The app artifact manifest is [mysql-source.yaml](../fixtures/manifests/mysql-source.yaml). It declares internal request/reply ports only:

- `mysql-source.internal` / `fetch` routes to `mysql-source.fetch:v1` for `MysqlSourceRequest` -> `MysqlSourcePayload`.
- `mysql-source.internal` / `status` is reserved for diagnostics/status. The v1 runtime currently serves the fetch route.

Runtime instances scope the concrete bound route by node id, matching the `fs-meta` pattern. A node `mysql-source-node-a` serves `mysql-source.fetch.mysql_source_node_a:v1.req`.

## Product Config

The product config shape is shown in [mysql-source.local.yaml](examples/mysql-source.local.yaml). Required endpoint fields:

- `object_ref`: stable app-level endpoint identity used by requests and grants.
- `endpoint_uri`: MySQL endpoint URI.
- `credential_ref`: optional local credential binding.
- `schema_scopes`: allowed schema patterns.
- `table_scopes`: allowed table patterns.
- `primary_key`: optional stable ordering key for concrete driver implementations.

Credential config contains references only. Supported auth references are `basic_env`, `api_key_env`, `bearer_env`, and `none`. MySQL deployments normally use `basic_env`.

## Runtime Grants

`__cnx_runtime.resource_grants[]` is the current transitional carrier for MySQL endpoint visibility. It must contain only endpoint identity, scope, state, and grant evidence:

- `resource_kind`: `mysql` or `mysql_endpoint`.
- `object_ref`: must match an endpoint config row.
- `grant_state`: `active` to allow use.
- `grant_epoch`: optional evidence copied into response payloads.
- `schema_scopes` and `table_scopes`: optional grant-level narrowing of product scopes.
- `credential_ref`: optional opaque reference override.

Capanix must not carry MySQL password bytes. Secret material is resolved locally from env refs such as `MYSQL_PROD_READER_PASSWORD`; missing local secret bindings fail closed.

## Internal Protocol

Callers send MessagePack-encoded `MysqlSourceRequest` to the internal fetch route. Responses are MessagePack-encoded `MysqlSourcePayload` variants:

- `Rows`: row batches from a transaction snapshot session plus `next_cursor`.
- `Fields`: discovered table columns.
- `Probe`: connection probe result.
- `Error`: typed app error with retryability.

V1 intentionally implements snapshot-session protocol only. A concrete MySQL driver should open a transaction session using `START TRANSACTION WITH CONSISTENT SNAPSHOT`, keep it alive for the returned cursor TTL, and close it on terminal page or explicit `SnapshotClose`. Binlog CDC is intentionally out of scope for this first Rust app boundary.
