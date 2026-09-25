# jsonl-source Deployment Contract

`jsonl-source` is a first-class Fustor/Capanix source driver for newline-delimited JSON files under a local root. It owns typed JSONL requests, record pages, bounded field sampling, native parsing, and fail-closed path validation.

## App Manifest

The app artifact manifest is [jsonl-source.yaml](../fixtures/manifests/jsonl-source.yaml):

- `jsonl-source.internal` / `fetch` routes to `jsonl-source.fetch:v1` for `JsonlSourceRequest` -> `JsonlSourcePayload`.
- `jsonl-source.internal` / `status` returns local readiness diagnostics.

## Product Config

The product config shape is shown in [jsonl-source.local.yaml](examples/jsonl-source.local.yaml). Endpoint fields:

- `object_ref`: stable endpoint identity.
- `root_path`: absolute filesystem root.
- `path_scopes`: allowed relative JSONL path patterns.

Top-level `field_sample_limit` bounds field discovery and defaults to 1000 parsed non-empty lines. Request `path` is required for `SnapshotPage` and `DiscoverFields`. Paths must remain under `root_path` after canonicalization.

## Runtime Grants

`resource_kind` must be `jsonl` or `jsonl_endpoint`; grants may narrow `path_scopes` but must not widen product scopes. Secret material is not accepted in product config or runtime grants.

## Internal Protocol

`JsonlSourcePayload` variants:

- `Records`: paged JSON records with `line_number` and `next_cursor`.
- `Fields`: object-key catalog sampled from bounded non-empty JSON lines.
- `Probe`: root reachability.
- `Status`: local readiness.
- `Error`: typed app error.

V1 snapshot cursors carry byte offset plus line number, so continuation does not have to rescan from the beginning of the file.
