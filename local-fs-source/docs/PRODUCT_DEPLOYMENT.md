# local-fs-source Deployment Contract

`local-fs-source` is a first-class Fustor/Capanix source driver for bounded local filesystem metadata reads. Capanix owns route/channel/runtime evidence; this app owns path-scoped config, typed request/response payloads, native filesystem access, paging, and typed errors.

## App Manifest

The app artifact manifest is [local-fs-source.yaml](../fixtures/manifests/local-fs-source.yaml). It declares internal request/reply ports only:

- `local-fs-source.internal` / `fetch` routes to `local-fs-source.fetch:v1` for `LocalFsSourceRequest` -> `LocalFsSourcePayload`.
- `local-fs-source.internal` / `status` returns local readiness diagnostics from manifest endpoints and active runtime grants.

Runtime instances scope concrete bound routes by node id, for example `local-fs-source.fetch.local_fs_source_node_a:v1.req`.

## Product Config

The product config shape is shown in [local-fs-source.local.yaml](examples/local-fs-source.local.yaml). Required endpoint fields:

- `object_ref`: stable endpoint identity used by requests and grants.
- `root_path`: absolute filesystem root. Relative roots fail closed.
- `path_scopes`: allowed relative path patterns under `root_path`.

The native driver rejects absolute request paths, `..`, platform root/prefix components, and canonicalized paths that escape `root_path`.

## Runtime Grants

`__cnx_runtime.resource_grants[]` carries endpoint visibility:

- `resource_kind`: `local-fs`, `local_fs`, or `local_fs_endpoint`.
- `object_ref`: must match an endpoint config row.
- `grant_state`: `active` to allow use.
- `grant_epoch`: optional evidence copied into payloads.
- `path_scopes`: optional grant-level narrowing of product scopes.

Grant scopes may narrow but must not widen product scopes.

## Internal Protocol

Fetch accepts MessagePack-encoded `LocalFsSourceRequest` and returns `LocalFsSourcePayload`:

- `Entries`: one page of file/directory metadata.
- `Fields`: static metadata field catalog.
- `Probe`: root reachability.
- `Status`: local runtime readiness.
- `Error`: typed app error with retryability.

V1 does not recursively crawl a tree. A directory snapshot returns one sorted page of direct children; callers continue with `next_cursor`.
