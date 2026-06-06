---
version: 0.1.0
---

# L2: Source Union Graph Architecture

> Focus: define the minimal source metadata skeleton needed to construct a
> `union graph view` across FS/NFS, local files, S3, ES, SQL, and future heterogeneous
> sources without duplicating native data or turning every layer into a
> materialized view.

## SOURCE_UNION_GRAPH.INTENT

1. **Primary Goal**:
   1. Build a unified graph view over heterogeneous source metadata.
   2. Preserve direct lineage from every graph fact back to native source data,
      source-specific cache, or explicit derivation rule.
   3. Keep source-native engines authoritative for their own details and query
      execution.

2. **Non-Goals**:
   1. Do not introduce an independent `metadata view` layer as a mandatory
      materialized state.
   2. Do not copy full ES documents, SQL rows, S3 object metadata, or fs-meta
      tree state into the union graph by default.
   3. Do not require every source to implement an fs-meta-like materialized sink
      tree.

3. **Core Principle**:
   1. Source views are virtual graph-skeleton adapters, not full metadata
      replicas.
   2. `union graph view` owns global identity, graph facts, and provenance.
   3. Native sources, or source-specific accelerators such as fs-meta sink tree,
      remain the authority for source-specific detail hydration.

## SOURCE_UNION_GRAPH.LAYERS

1. **Native Source Or Source-Specific Accelerator**:
   1. FS/NFS source may use `fs-meta` sink tree as a source-specific cache to
      accelerate large directory metadata loading and exact path lookup.
   2. ES source uses native index, mapping, PIT/search-after, and search APIs.
   3. SQL sources use native catalog tables, information schema, and SQL
      pushdown.
   4. Local file sources such as CSV, JSON, and XLSX use file-native parsing,
      schema/header inspection, and bounded row sampling only when needed.
   5. S3 source uses native bucket/list/head/inventory APIs and may introduce a
      source-specific cache only when required by scale or SLA.

2. **Source Graph Adapter**:
   1. Exposes each source as minimal graph skeleton nodes and edges.
   2. Converts source-native observations into `SourceGraphNode` and
      `SourceGraphEdge` records.
   3. Carries native pointers and provenance anchors instead of full source
      payload copies.

3. **Union Identity Spine**:
   1. Maps source-local identities to stable global graph identities.
   2. Stores only identity, source reference, native pointer, fingerprint, and
      watermarks needed to detect change and hydrate details.
   3. Does not own source-native truth.

4. **Union Graph View**:
   1. Provides the unified node/edge/property graph.
   2. Stores graph facts plus provenance references.
   3. Supports asset, lineage, semantic, and relationship queries as graph
      projections.
   4. Treats metadata-style queries such as asset list, asset detail, and
      source search as projections over the graph plus source hydration, not as
      a separate materialized metadata layer.

## SOURCE_UNION_GRAPH.SOURCE_ADAPTER_CONTRACT

1. **SourceGraphNode**:

   ```text
   SourceGraphNode {
     source_kind          # fs | local-fs | s3 | es | pg | mysql | ...
     source_instance      # nfs group | bucket endpoint | es cluster | db instance
     local_id             # stable id inside this source instance
     node_kind            # root | namespace | collection | asset | field | entity
     native_ref           # fs://... | s3://... | es://... | pg://...
     display_name         # short user-facing label
     native_pointer       # adapter-owned pointer for source hydration
     fingerprint          # mtime+size+inode | etag | seq_no+term | schema_hash | ...
     observed_at          # observation timestamp
     grant_epoch          # runtime/source grant epoch when available
   }
   ```

2. **SourceGraphEdge**:

   ```text
   SourceGraphEdge {
     source_kind
     source_instance
     from_local_id
     to_local_id
     edge_kind            # contains | has_field | indexes | derived_from | same_as_candidate
     evidence_ref         # observation/snapshot/poll/query evidence
     confidence           # observed | declared | inferred
     observed_at
     grant_epoch
   }
   ```

3. **CrossSourceGraphEdge**:

   ```text
   CrossSourceGraphEdge {
     from                  # source_kind + source_instance + local_id
     to                    # source_kind + source_instance + local_id
     edge_kind             # parsed_to | loaded_into | projected_as | declares_schema | has_runtime_status
     evidence_ref
     confidence
     observed_at
     grant_epoch
   }
   ```

4. **Adapter Constraints**:
   1. `local_id` must be stable enough for repeat observations within one
      source instance.
   2. `native_ref` must identify the native object in a human-readable form.
   3. `native_pointer` must be sufficient for the adapter to hydrate details
      from native source or source-specific cache.
   4. `fingerprint` must change when the source-native object version relevant
      to graph facts changes.
   5. Adapters must not emit full row/document/content payloads as graph
      defaults.

## SOURCE_UNION_GRAPH.PROVENANCE

1. **NativeAnchor**:

   ```text
   NativeAnchor {
     source_kind
     source_instance
     native_ref
     native_pointer
     native_version       # etag | inode+size+mtime | seq_no+term | schema_hash | txid
     cache_pointer        # optional, for example fs-meta:<group>:<path>
     grant_epoch
   }
   ```

2. **EvidenceRecord**:

   ```text
   EvidenceRecord {
     evidence_id
     anchor: NativeAnchor
     observed_by          # fs-meta | es-source | s3-source | pg-source | mysql-source
     observation_kind     # snapshot | poll | query | manual | inferred
     observed_at
     watermark            # cursor | PIT | txid | scan epoch | inventory version
     adapter_version
     confidence           # observed | declared | inferred
   }
   ```

3. **GraphFact**:

   ```text
   GraphFact {
     fact_id
     fact_kind            # node | edge | property
     subject_gid
     predicate            # required for edge/property facts
     object_gid_or_value  # required for edge/property facts
     evidence_refs        # one or more EvidenceRecord ids
     derived_by           # optional rule or mapping id
     valid_from
     invalidated_at
   }
   ```

4. **Lineage Requirements**:
   1. Every node, edge, and property in `union graph view` must reference one or
      more evidence records or an explicit derivation rule.
   2. FS graph facts derived through `fs-meta` must still anchor to the native
      NFS object; `fs-meta` cache pointer is cache provenance, not native truth.
   3. Cross-source edges must preserve all contributing source evidence, for
      example file observation, table observation, and pipeline/job evidence.
   4. Inferred facts must be labeled as inferred and must not overwrite observed
      native facts without conflict evidence.

## SOURCE_UNION_GRAPH.SOURCE_POLICIES

1. **FS/NFS**:
   1. `fs-meta` sink tree is an FS adapter accelerator for directory tree,
      file-stat, and exact-path access.
   2. It is optimized for large NFS metadata loading performance.
   3. It is not a platform-wide mandatory materialized view pattern.
   4. Union graph stores only global identity, graph facts, fingerprints, and
      provenance pointers for FS nodes.

2. **S3**:
   1. The default source path is native bucket/object API pushdown.
   2. Bucket, prefix, and object may become graph nodes.
   3. Full object metadata, tags, and object content are hydrated from S3 or
      source-specific cache only when explicitly needed.

3. **Local Files**:
   1. Local CSV, JSON, XML, and XLSX files may become graph assets when they are
      inputs to a parser, loader, or graph projection workflow.
   2. Headers, file fingerprints, and row-family identifiers may become graph
      facts when required to explain downstream lineage.
   3. Full file content and full row payloads are not copied into the graph by
      default.

4. **ES**:
   1. The default source path is native ES pushdown.
   2. Cluster, index, field, and selected document identities may become graph
      nodes.
   3. Full document source is not copied into the graph by default.
   4. PIT/search-after and mapping APIs remain ES adapter implementation
      details.

5. **PG/MySQL**:
   1. The default source path is native catalog and SQL pushdown.
   2. Database, schema, table, column, and selected row identities may become
      graph nodes.
   3. Full rows are not copied into the graph by default.
   4. Row-level graph promotion requires explicit business schema, lineage, or
      semantic rule.

## SOURCE_UNION_GRAPH.MATERIALIZATION_POLICY

1. **Persist By Default**:
   1. Global identity spine.
   2. Cross-source graph facts and semantic/lineage edges.
   3. Evidence records and native anchors.
   4. Watermarks and fingerprints needed for change detection.

2. **Derived And Rebuildable**:
   1. Search indexes.
   2. Relationship indexes.
   3. Freshness and coverage indexes.
   4. Query acceleration structures.

3. **Do Not Persist By Default**:
   1. ES document source payloads.
   2. SQL row payloads.
   3. S3 full object metadata and tags.
   4. fs-meta materialized tree or full file-stat cache.
   5. Native source content.

4. **Promotion Rule**:
   1. Source-native details may be promoted into graph facts only when they are
      required for graph identity, graph relation, governance decision, search
      SLA, or explicit business semantic modeling.
   2. Promoted facts must carry provenance and a refresh/invalidated lifecycle.

## SOURCE_UNION_GRAPH.QUERY_MODEL

1. **Directory Query**:
   1. FS directory reads should use `fs-meta` cache through the FS adapter.
   2. Union graph may provide identity and relationship context around the
      returned files but must not duplicate the directory tree.

2. **Native Detail Hydration**:
   1. Asset detail queries resolve `gid -> native_pointer`.
   2. The responsible source adapter hydrates source-native details from native
      engine or source-specific cache.
   3. Hydration must return provenance that identifies whether data came from
      native engine, cache, or derived fact.

3. **Graph Relationship Query**:
   1. Relationship and lineage queries read graph facts and evidence records.
   2. Node labels and lightweight properties may come from union graph.
   3. Heavy source-native detail is hydrated only when requested.

4. **Metadata-Style Projection**:
   1. Asset inventory, search, and detail pages are projections over union
      graph plus source hydration.
   2. They do not define a separate `metadata view` state layer.

5. **Bio Pipeline Demonstration**:
   1. The demo path may ingest the existing bio pipeline state as source graph
      skeletons without rerunning the bio pipeline.
   2. NFS raw files are represented as `fs` nodes with `fs-meta` cache pointers.
   3. CNCB/OMIX CSV and XLSX inputs are represented as `local-fs` nodes.
   4. PPG schema and status JSON are represented as `es`-style JSON document
      nodes.
   5. PostgreSQL catalogs, tables, and PPG labels are connected by
      `loaded-into`, `projected-as`, `declares-schema`, and
      `has-runtime-status` edges.

## SOURCE_UNION_GRAPH.OWNERSHIP

1. Source adapters own source-specific connection, query pushdown, cache usage,
   and source-native hydration.
2. Union graph owns global identity, graph facts, evidence references, and
   cross-source relationship semantics.
3. Source-specific accelerators own cache lifecycle and cache correctness for
   their source only.
4. Platform query planning may choose graph index, native pushdown, source cache,
   or mixed hydration paths, but must preserve graph-to-native provenance in the
   response path.
