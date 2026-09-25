# pg-source Deployment Contract

`pg-source` is a first-class Fustor/Capanix source driver for PostgreSQL table reads. It follows the same source boundary style as `mysql-source`: Capanix owns route/channel/runtime evidence; `pg-source` owns endpoint config, local credential resolution, typed payloads, PostgreSQL access, snapshot paging, and typed errors.

## App Manifest

The app artifact manifest is [pg-source.yaml](../fixtures/manifests/pg-source.yaml):

- `pg-source.internal` / `fetch` routes to `pg-source.fetch:v1` for `PgSourceRequest` -> `PgSourcePayload`.
- `pg-source.internal` / `status` returns local readiness diagnostics and does not call the native driver.

Runtime instances scope concrete bound routes by node id, for example `pg-source.fetch.pg_source_node_a:v1.req`.

## Product Config

The product config shape is shown in [pg-source.local.yaml](examples/pg-source.local.yaml). Endpoint fields:

- `object_ref`: stable endpoint identity.
- `endpoint_uri`: `pg://`, `postgres://`, or `postgresql://` URI. Default port is 5432.
- `credential_ref`: optional local credential binding.
- `schema_scopes`: allowed schema patterns.
- `table_scopes`: allowed table patterns.
- `primary_key`: optional stable ordering key. If omitted, the native driver must discover a single-column primary key.

Credential config contains references only. PostgreSQL deployments normally use `basic_env`; secret bytes must stay in local environment variables. The runtime artifact expects the `psql` client binary to be installed on the host.

## Runtime Grants

`__cnx_runtime.resource_grants[]` carries endpoint visibility:

- `resource_kind`: `pg` or `pg_endpoint`.
- `object_ref`: must match an endpoint config row.
- `grant_state`: `active` to allow use.
- `grant_epoch`: optional evidence copied into payloads.
- `schema_scopes` and `table_scopes`: optional grant-level narrowing.
- `credential_ref`: optional opaque reference override.

Grant scopes may narrow but must not widen product scopes.

## Internal Protocol

`PgSourcePayload` variants:

- `Rows`: row batches from a read-only repeatable-read transaction snapshot plus `next_cursor`.
- `Fields`: discovered table columns.
- `Probe`: connection probe result.
- `Status`: local runtime readiness.
- `Error`: typed app error with retryability.

V1 pages by keyset cursor using the configured or discovered primary key. The wire protocol keeps the same `SnapshotOpen` / `SnapshotPage` / `SnapshotClose` shape as `mysql-source`, but the current implementation runs bounded `psql` commands per page instead of holding a long-lived PostgreSQL client connection. CDC/logical replication is out of scope.

## Native Integration Tests

The native PostgreSQL integration test is ignored by default:

```bash
FUSTOR_PG_SOURCE_ENDPOINT_URI=pg://127.0.0.1:5432/postgres \
FUSTOR_PG_SOURCE_USERNAME=reader \
FUSTOR_PG_SOURCE_PASSWORD=secret \
cargo test -p pg-source-runtime --test native_pg_integration -- --ignored
```
