---
version: 0.1.0
---

# L3: Multi-scope native source configuration

## [interface] NamedObjectScopes

**Rationale**: NativeSourceConfiguration exposes multiple bounded operations over
one authority without creating a connection per application data product.

```text
SourceDeclaration: existing fields + exactly one of
  objectScope: existing-kind-scope
  objectScopes: [{id, label?, objectScope: existing-kind-scope}] (1..16)
```

Each scope passes the existing native validate-scope gate. All scopes must yield
identical connection/grant/configuration bytes; incompatible native settings need
separate declarations rather than a silently widened endpoint. Named operations
use scope-id + ':' + native-operation-id and preserve scopeId/objectScope metadata.
The old single-scope operation ids and declaration format remain unchanged.

## [interface] OwnerPinnedStatusAssociation

**Rationale**: A provider's revision/health endpoint must follow an explicit owner
association, not an old template endpoint or a caller-supplied URL.

```text
source-policy policy entry (graph only):
  statusBindings?: [{endpointUri, statusUrl, statusCredentialRef?}]
```

Both endpoints are checked against installed targets. Endpoint identities are
unique; credentials reference installed native credentials. Registration selects
only an exact endpointUri match. No match removes stale status URL/credential
fields. Validation performs no network access; unknown caller fields are rejected.
