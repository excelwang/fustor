# csv-source Deployment Contract

`csv-source` is a first-class Fustor/Capanix source driver for path-scoped CSV files under a local root. It owns typed CSV requests, record pages, field discovery, native parsing, and fail-closed path validation.

## App Manifest

The app artifact manifest is [csv-source.yaml](../fixtures/manifests/csv-source.yaml):

- `csv-source.internal` / `fetch` routes to `csv-source.fetch:v1` for `CsvSourceRequest` -> `CsvSourcePayload`.
- `csv-source.internal` / `status` returns local readiness diagnostics.

## Product Config

The product config shape is shown in [csv-source.local.yaml](examples/csv-source.local.yaml). Endpoint fields:

- `object_ref`: stable endpoint identity.
- `root_path`: absolute filesystem root.
- `path_scopes`: allowed relative CSV path patterns.
- `has_headers`: defaults to `true`.
- `delimiter`: optional single-byte delimiter, defaults to `,`.

Request `path` is required for `SnapshotPage` and `DiscoverFields`. Paths must remain under `root_path` after canonicalization.

## Runtime Grants

`resource_kind` must be `csv` or `csv_endpoint`; grants may narrow `path_scopes` but must not widen product scopes. Secret material is not accepted in product config or runtime grants.

## Internal Protocol

`CsvSourcePayload` variants:

- `Records`: paged CSV records with `row_number`, string-valued JSON object, fields, and `next_cursor`.
- `Fields`: field catalog from headers or generated `field_N` names.
- `Probe`: root reachability.
- `Status`: local readiness.
- `Error`: typed app error.

V1 paging is by record offset. It does not infer numeric types; CSV cells are emitted as strings.
