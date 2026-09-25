---
version: 0.1.1
---

# Native source configuration tooling

## [interface] NativeSourceConfiguration

**Rationale**: Source configuration belongs to its owner, not the console.

```text
describe(scope) -> revision, source declarations
apply(scope, {declaration, expectedRevision}) -> saved revision, operation receipt
resolve(scope) -> native source configuration references
```

Implements Components.NativeSourceConfiguration.

The optional source-config tool provides `describe`, `apply` and `resolve`
over one explicitly configured source-owner scope. The scope carries an id,
native configuration references, a private registry directory and installed
source capability policy. It is not a solution or Agent instance.

`apply` accepts a source declaration and expected registry revision. Native
validation and expected-revision preview are internal; the caller need not
perform a separate preview round trip. The registry uses the existing native
source declaration/immutable-file format so prior configuration references and
digests remain readable.

## [algorithm] NativeConfigurationCommit

**Rationale**: Preserve immutable configuration and explicit commit uncertainty.

```text
declaration -> validate -> lock/revalidate -> immutable version -> registry head
```

1. Validate declaration and installed target/root/credential-reference scope.
2. Invoke only native configuration and scope validation, never a provider read.
3. Under a process-bound file lock, revalidate expected revisions and policy.
4. Persist a request receipt and immutable native configuration before atomically
   replacing the registry head; preserve prior files. A receipt acknowledgement
   follows file sync and directory-entry sync, including newly created parents.
5. Return the native revision, operation id and next-read activation. A failed
   confirmation is distinct from an uncommitted configuration.

Authentication is performed by the calling owner transport. Audit contains
configuration operation references, not caller reasoning or task state.

## [algorithm] PreserveNativeTransport

**Rationale**: Replacing an endpoint address must not silently disable the
installation's authenticated transport or move trust selection to the caller.

For MySQL, retain the native endpoint's optional `tls` configuration and the
legacy `ssl=true` URI requirement when rebuilding host, port and database from
a declaration. A declaration cannot supply CA paths, TLS bypass switches or
additional native transport settings. Native product configuration, not runtime
endpoint overrides, owns the TLS requirement. Validation performs no certificate
file reads or provider connections; actual source reads establish peer trust.
