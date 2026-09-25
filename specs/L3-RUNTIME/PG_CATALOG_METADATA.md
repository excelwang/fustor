---
version: 0.1.0
---

# L3: PostgreSQL catalog inspection

## [interface] PgCatalogMetadata

**Rationale**: SourceProvider implements CATALOG_METADATA without adding business
inventory, transformations, scheduling or arbitrary SQL to the storage engine.

```text
PgSourceRequest {object_ref, operation: catalog-metadata, limit}
PgSourcePayload::Catalog {
  object_ref, database, database_oid, system_identifier?, read_only: true,
  complete: true, grant_epoch?,
  objects[{schema, name, oid, kind, estimated_rows?, row_count_method: estimate,
           view_definition_md5?}]
}
SourceDeclaration.objectScope = {catalogMetadata: true}
```

Existing table-scope declarations and operations retain their format and meaning.
Catalog selection uses only the endpoint's installed product/grant scopes; it
does not replace them with wildcards. No request SQL, schema/table selector,
cursor or field filter is accepted by this operation. Results include only
native-visible user relations in those scopes. Cluster identity is null when the
native principal cannot read it; callers requiring a pinned identity must reject
that missing evidence, not synthesize it.

## [algorithm] ReadPgCatalog

**Rationale**: Bounded metadata cannot silently masquerade as a full data scan or
the source's business revision.

```text
read_catalog(endpoint, native_grant, envelope_limit) -> PgCatalogMetadata | SourceError
```

1. Validate envelope, grant epoch, operation shape and configured limit.
2. Reserve the native call and open READ ONLY REPEATABLE READ with deadline and
   lock timeouts. Read native database identity and visible catalog objects using
   fixed parameterized SQL; do not execute view definitions or source payload SQL.
3. Read at most limit+1 permitted objects; overflow returns a typed exhaustion
   error. Estimates apply only to ordinary/materialized relations with known
   nonnegative statistics, never normal views, foreign tables or partitions.
4. Roll back the read transaction and close it on success or failure. Emit the
   normal native source receipt; its observation revision is not a whole-database
   data revision. No graph or DataView publication is performed.
