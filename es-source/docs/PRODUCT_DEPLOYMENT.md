# es-source Deployment Contract

`es-source` is an independent Fustor/Capanix app for Elasticsearch raw document reads. It follows the `fs-meta` app pattern: Capanix owns route/channel/runtime evidence, while `es-source` owns request/response payloads, ES auth resolution, cursor semantics, and error payloads.

## App Manifest

The app artifact manifest is [es-source.yaml](../fixtures/manifests/es-source.yaml). It declares internal request/reply ports only:

- `es-source.internal` / `fetch` routes to `es-source.fetch:v1` for `EsSourceRequest` -> `EsSourcePayload`.
- `es-source.internal` / `status` is reserved for diagnostics/status. The v1 runtime currently serves the fetch route.

Runtime instances scope the concrete bound route by node id, matching the `fs-meta` pattern. A node `es-source-node-a` serves `es-source.fetch.es_source_node_a:v1.req`.

## Product Config

The product config shape is shown in [es-source.local.yaml](examples/es-source.local.yaml). Required endpoint fields:

- `object_ref`: stable app-level endpoint identity used by requests and grants.
- `endpoint_uri`: ES base URL.
- `credential_ref`: optional local credential binding.
- `index_scopes`: allowed index patterns such as `logs-*`.
- `timestamp_field`: defaults to `@timestamp`.
- `tie_breaker_field`: defaults to `_id`.

Credential config contains references only. Supported auth references are `basic_env`, `api_key_env`, `bearer_env`, and `none`.

## Runtime Grants

`__cnx_runtime.resource_grants[]` is the current transitional carrier for ES endpoint visibility. It must contain only endpoint identity, scope, state, and grant evidence:

- `resource_kind`: `elasticsearch` or `es_endpoint`.
- `object_ref`: must match an endpoint config row.
- `grant_state`: `active` to allow use.
- `grant_epoch`: optional evidence copied into response payloads.
- `index_scopes`: optional grant-level narrowing of product scopes.
- `credential_ref`: optional opaque reference override.

Capanix must not carry ES password/API key/token bytes. Secret material is resolved locally from env refs such as `ES_PROD_READER_API_KEY`; missing local secret bindings fail closed.

## Internal Protocol

Callers send MessagePack-encoded `EsSourceRequest` to the internal fetch route. Responses are MessagePack-encoded `EsSourcePayload` variants:

- `Batch`: raw ES document envelopes plus `next_cursor`.
- `Fields`: flattened mapping fields.
- `Probe`: connection probe result.
- `Error`: typed app error with retryability.

Downstream owns cursor persistence. Snapshot uses PIT + `search_after`; polling uses timestamp plus stable sort cursor. `es-source` does not persist a global checkpoint.
