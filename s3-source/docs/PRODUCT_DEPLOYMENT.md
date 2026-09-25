# s3-source Deployment Contract

`s3-source` is an independent Fustor/Capanix app for S3-compatible object metadata reads. It follows the `fs-meta` source-style architecture: Capanix owns route/channel/runtime evidence, while `s3-source` owns typed request/response payloads, S3 endpoint auth resolution, snapshot/poll cursor semantics, and error payloads.

## App Manifest

The app artifact manifest is [s3-source.yaml](../fixtures/manifests/s3-source.yaml). It declares internal request/reply ports only:

- `s3-source.internal` / `fetch` routes to `s3-source.fetch:v1` for `S3SourceRequest` -> `S3SourcePayload`.
- `s3-source.internal` / `status` returns local readiness diagnostics from manifest endpoints and active runtime grants. It is a request/reply route and does not call the native S3 driver.

Runtime instances scope the concrete bound route by node id, matching the `fs-meta` pattern. A node `s3-source-node-a` serves `s3-source.fetch.s3_source_node_a:v1.req`.

## Product Config

The product config shape is shown in [s3-source.local.yaml](examples/s3-source.local.yaml). Required endpoint fields:

- `object_ref`: stable app-level endpoint identity used by requests and grants.
- `endpoint_uri`: S3-compatible endpoint URI.
- `bucket`: default bucket.
- `region`: optional region.
- `prefix`: default prefix.
- `credential_ref`: optional local credential binding.
- `bucket_scopes` and `prefix_scopes`: allowed bucket/prefix patterns.

Credential config contains references only. Supported auth references are `basic_env`, `api_key_env`, `bearer_env`, and `none`. S3-compatible deployments normally use `basic_env` for access key id and secret access key env refs.

## Runtime Grants

`__cnx_runtime.resource_grants[]` is the current transitional carrier for S3 endpoint visibility. It must contain only endpoint identity, scope, state, and grant evidence:

- `resource_kind`: `s3` or `s3_endpoint`.
- `object_ref`: must match an endpoint config row.
- `grant_state`: `active` to allow use.
- `grant_epoch`: optional evidence copied into response payloads.
- `bucket_scopes` and `prefix_scopes`: optional grant-level narrowing of product scopes.
- `credential_ref`: optional opaque reference override.

Capanix must not carry S3 secret bytes. Secret material is resolved locally from env refs such as `S3_PROD_SECRET_ACCESS_KEY`; missing local secret bindings fail closed.

## Internal Protocol

Callers send MessagePack-encoded `S3SourceRequest` to the internal fetch route. Responses are MessagePack-encoded `S3SourcePayload` variants:

- `Objects`: object metadata batches plus `next_cursor`.
- `Fields`: the object metadata field catalog.
- `Probe`: connection probe result.
- `Status`: local runtime readiness from configured endpoints and active grants.
- `Error`: typed app error with retryability.

V1 supports snapshot listing and polling. Snapshot maps to paged S3 `ListObjectsV2` style traversal with an opaque continuation token. Polling is checkpointed by `(last_modified_unix_ms, last_key)` plus an optional `continuation_token`; old poll cursors without the token still decode. Polling is intentionally bounded to a fixed number of S3 list pages per request and may return `has_more=true` before the full prefix has been scanned, so downstream callers must persist and continue from `next_cursor` instead of assuming one call drains the prefix.

## Native Integration Tests

The native S3 tests are ignored by default. To run them against a disposable S3-compatible bucket:

```bash
FUSTOR_S3_SOURCE_ENDPOINT_URI=http://127.0.0.1:9000 \
FUSTOR_S3_SOURCE_ACCESS_KEY=minioadmin \
FUSTOR_S3_SOURCE_SECRET_KEY=minioadmin \
FUSTOR_S3_SOURCE_BUCKET=fustor-source-it \
cargo test -p s3-source-runtime --test native_s3_integration -- --ignored
```

`FUSTOR_S3_SOURCE_REGION` is optional and defaults to `us-east-1`.
