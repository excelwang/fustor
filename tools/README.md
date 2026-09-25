# Fustor boundary conformance executable

`fustor_boundary.py` is a small configuration-driven acceptance executable for
two generic Fustor contracts that did not yet have a deployable surface:

- a graph is observed as a peer, bounded, read-only source by configured query
  id; callers cannot submit native query text or graph administration actions;
- a caller-produced external result is admitted by a generic evidence-gated
  manifest, and DataView stores the manifest plus normalized facts without
  fetching the referenced product bytes.

The executable contains no workflow topology, ETL, graph schema, BigSearch
mapping, endpoint, or credential. Those remain in caller/application deployment
configuration. It is a conformance surface for real-source acceptance, not a
replacement for Capanix routing or for the existing source runtimes.

The driver exercised by the current real-run profile is a Neo4j-compatible
PuppyGraph adapter, including its provider-specific readiness/revision probe.
That driver is an implementation behind the provider-neutral graph-source
contract; adding a production driver registry remains separate engineering
work and must not change the public request, receipt, or admission contracts.

Its file-backed DataView records are disposable, per-run acceptance evidence.
They exercise the generic admission invariants but do not replace or fork the
production `data-view`, normalizer registry, revision store, or service API.

Run its bounded contract tests with:

```bash
python3 -m unittest discover -s tools/tests -p 'test_*.py'
```
