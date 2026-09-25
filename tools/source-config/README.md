# Native source configuration

Optional peripheral tooling for Fustor's own source declarations. It is not a
workflow service, instance registry, provider provisioner or grant authority.
Node.js runs the file transaction; the installed native Fustor source inspector
performs validation. No provider payload is read during registration.

The private owner configuration is JSON:

```json
{"id":"sources","directory":"/private/fustor/sources","policyFile":"/private/fustor/source-policy.json"}
```

The deployment policy contains the native inspector executable, allowed targets,
named filesystem roots and per-kind native templates, object/grant references and
bounded read capabilities. It never accepts executable paths, policy, grants or
literal secrets from a source declaration. Existing kinds include jsonl, csv,
local-fs, s3, pg, mysql, es and graph; graph is not privileged over other sources.

One declaration can use either its original `objectScope`, or up to sixteen
named `objectScopes: [{id, label?, objectScope}]`. Every scope is independently
validated by the native owner. They must resolve to identical native connection,
grant and configuration bytes; incompatible grants or endpoint configurations
require separate declarations instead of implicit widening. Operations are
identified by `scope-id:operation-id`; old single-scope operation IDs are unchanged.

A graph kind policy may install `statusBindings` entries containing exact
`endpointUri`, `statusUrl` and optional `statusCredentialRef`. Both endpoints
must be in the installed target allowlist; credential references must exist in
the native template. Declarations cannot supply status URLs or credentials.
Rebinding to a different endpoint without a matching status binding removes the
previous status association. Validation never calls either endpoint.

Use `node cli.mjs describe --config /private/fustor/owner.json` to discover the
revision and allowed declaration fields. `apply --config ...` takes one stdin
JSON object `{declaration, expectedRevision}`; preview/validation is internal.
`resolve --config ...` is an owner-internal native reference projection, not a
browser response. New immutable native bytes precede the atomic active reference;
conflicts and uncertain audit confirmation remain explicit. Read commands do not
create a source or invent provider readiness. Configuration is consumed on the
next owner read; it does not restart providers.

Test with `FUSTOR_SOURCE_INSPECT=/installed/fustor-source-inspect npm test`.
The contract test uses the real native validator and a nonexistent source file;
it proves persistence/conflict behavior without claiming provider access.
