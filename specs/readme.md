# Platform Specs

This directory contains platform-level specs for cross-app architecture. The
formal fs-meta specification tree remains under `fs-meta/specs/`.

## Platform-Level Specs

1. [L2-SOURCE-UNION-GRAPH.md](./L2-SOURCE-UNION-GRAPH.md): source metadata
   skeleton, union graph view, source pushdown/cache policy, and graph-to-native
   provenance.

## fs-meta Gate Discovery Shim

The gate discovery entrypoints below delegate to the canonical repo-owned
validator and contract workflow for the fs-meta spec tree.

./scripts/validate.py specs/
./scripts/test-workflow.sh contracts
