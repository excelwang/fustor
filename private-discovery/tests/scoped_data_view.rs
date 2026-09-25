use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use data_view::{
    ActionResultSubmission, BoundedSourceAction, CanonicalIdentity, ContinuationPolicy,
    CoverageState, DATA_VIEW_WIRE_VERSION, DataViewCommitStatus, DataViewContractError,
    DataViewEngine, DerivationEvidence, DiscoveryOperation, FailureMode, FreshnessState,
    MemoryDataViewPersistence, NativeAnchor, NormalizedDataViewCommit, ObservationPath,
    PayloadEncoding, ProducerKind, ProjectionMode, RelationshipAssertion, RelationshipConfidence,
    ResultNormalizer, RetentionPolicy, SchemaVersionRef, SideEffectLevel, SingleResultNormalizer,
    SourceAuthorityRef, StreamingRefreshEntry, SystemIdGenerator, TrackedResultRef,
};
use private_discovery::{
    ActionNode, DataViewRevisionSelector, DisclosureBudget, DiscoveryBudget, DiscoveryFrontier,
    DiscoveryFrontierStrategy, DiscoveryIntent, DiscoveryScope, PolicyContext,
    PrivateDataDiscoveryRequest, PrivateDiscoveryOrchestrator, RankedSourceAction,
    ScopedDataViewProjectionOutcome, ScopedDataViewProjectionRequest, ScopedDataViewProjector,
    ScopedDisclosureBudget, ScopedDiscoveryGraph, ScopedProjectionAccess, SourceInstanceKey,
};
use union_graph::SourceKind;

fn schema() -> SchemaVersionRef {
    SchemaVersionRef {
        schema_ref: "fustor.test.scoped".into(),
        schema_version: "1".into(),
    }
}

struct RelationshipNormalizer {
    schema: SchemaVersionRef,
}

impl ResultNormalizer for RelationshipNormalizer {
    fn schema(&self) -> &SchemaVersionRef {
        &self.schema
    }

    fn normalize_action(
        &self,
        submission: &ActionResultSubmission,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        let anchor = submission
            .native_anchors
            .first()
            .ok_or(DataViewContractError::InvalidContract("native_anchors"))?;
        let result_ref = if anchor.native_ref == "a.json" {
            TrackedResultRef("result:a".into())
        } else {
            TrackedResultRef("result:b".into())
        };
        let relationship_assertions = (result_ref.0 == "result:b")
            .then(|| RelationshipAssertion {
                relationship_id: "relationship:a-to-b".into(),
                source_result_ref: TrackedResultRef("result:a".into()),
                target_result_ref: result_ref.clone(),
                relationship_kind: "depends-on".into(),
                authority_refs: vec![
                    SourceAuthorityRef("source:a".into()),
                    SourceAuthorityRef("source:b".into()),
                ],
                evidence_refs: vec!["evidence:relationship:a-to-b".into()],
                derivation_rule: Some("rule:a-to-b".into()),
                confidence: RelationshipConfidence::Declared,
                observed_revision: submission.source_revision.clone(),
                freshness: FreshnessState::Fresh,
            })
            .into_iter()
            .collect();
        Ok(NormalizedDataViewCommit {
            complete_payload_attachment: submission.payload.clone(),
            canonical_identities: vec![CanonicalIdentity {
                canonical_id: result_ref.0.clone(),
                result_ref,
                entity_kind: "object".into(),
                source_authority_ref: submission.source_authority_ref.clone(),
                native_anchors: submission.native_anchors.clone(),
                properties: BTreeMap::new(),
            }],
            relationship_assertions,
            dependency_refs: vec![],
            normalization_evidence: vec!["normalizer:relationship-test".into()],
            source_revision_order_key: submission.source_revision.as_bytes().to_vec(),
        })
    }

    fn normalize_stream_refresh(
        &self,
        _source_authority_ref: &SourceAuthorityRef,
        _tracked_result_ref: &TrackedResultRef,
        _entry: &StreamingRefreshEntry,
    ) -> Result<NormalizedDataViewCommit, DataViewContractError> {
        Err(DataViewContractError::InvalidContract(
            "relationship test stream refresh",
        ))
    }
}

fn action(action_id: &str, authority: &str) -> BoundedSourceAction {
    BoundedSourceAction {
        wire_version: DATA_VIEW_WIRE_VERSION,
        action_id: action_id.into(),
        source_authority_ref: SourceAuthorityRef(authority.into()),
        observation_path: ObservationPath::Direct,
        operation: DiscoveryOperation::ObjectHead,
        permission_scope: "scoped-test".into(),
        freshness_precondition: FreshnessState::Fresh,
        max_rows: 1,
        max_bytes: 1024,
        max_calls: 1,
        max_fanout: 1,
        max_latency_ms: 1000,
        max_cost_units: 2,
        side_effect_level: SideEffectLevel::ReadOnly,
        continuation_policy: ContinuationPolicy::None,
        failure_modes: vec![FailureMode::NotDisclosed],
        expansion_rules: vec![],
        expected_information_gain: 50,
        explanation: "materialize visible object".into(),
    }
}

fn materialize(
    engine: &mut DataViewEngine,
    action_id: &str,
    authority: &str,
    native_ref: &str,
    policy_labels: &[&str],
) {
    engine
        .register_action(action(action_id, authority))
        .expect("register action");
    let context = engine
        .issue_action_context(
            action_id,
            engine.head_ref(),
            ProjectionMode::CurrentActionable,
            "policy-a",
            4,
            100,
            200,
        )
        .expect("context");
    let payload = format!(r#"{{"native_ref":"{native_ref}"}}"#).into_bytes();
    let submission = ActionResultSubmission {
        wire_version: DATA_VIEW_WIRE_VERSION,
        submission_id: format!("submission:{action_id}"),
        execution: context,
        source_authority_ref: SourceAuthorityRef(authority.into()),
        authority_object_ref: authority.into(),
        producer_id: authority.into(),
        producer_kind: ProducerKind::Source,
        projection_evidence: None,
        schema: schema(),
        encoding: PayloadEncoding::Json,
        actual_bytes: payload.len() as u64,
        payload,
        native_anchors: vec![NativeAnchor {
            anchor_kind: "object".into(),
            native_ref: native_ref.into(),
            native_pointer: None,
            native_version: Some("rev-1".into()),
        }],
        source_revision: "rev-1".into(),
        evidence_refs: vec![format!("evidence:{native_ref}")],
        observed_at_ms: 120,
        policy_labels: policy_labels
            .iter()
            .map(|label| (*label).to_string())
            .collect(),
        grant_epoch: 4,
        freshness: FreshnessState::Fresh,
        derivation: DerivationEvidence {
            derivation_kind: "observed".into(),
            rule_ref: None,
            dependency_refs: vec![],
        },
        continuation: None,
        actual_rows: 1,
        actual_calls: 1,
        actual_fanout: 1,
        actual_latency_ms: 10,
        actual_cost_units: 1,
    };
    assert_eq!(
        engine
            .submit_action_result(submission, 120, "policy-a")
            .status,
        DataViewCommitStatus::Accepted
    );
}

fn empty_graph() -> ScopedDiscoveryGraph {
    ScopedDiscoveryGraph {
        resource_nodes: vec![],
        action_nodes: vec![],
        visibility_edges: vec![],
        executable_edges: vec![],
        expansion_edges: vec![],
        source_authority_refs: vec![],
        policy_constraints: vec![],
        freshness_constraints: vec![],
        expires_at_ms: 1000,
    }
}

fn empty_frontier() -> DiscoveryFrontier {
    DiscoveryFrontier {
        strategy: DiscoveryFrontierStrategy::BalancedBreadthThenDepth,
        ranked_actions: vec![],
        skipped_branches: vec![],
        explanation: "test".into(),
    }
}

fn access(authorities: &[&str], labels: &[&str]) -> ScopedProjectionAccess {
    ScopedProjectionAccess {
        subject_scope_id: "tenant-a:user-a".into(),
        tenant_id: "tenant-a".into(),
        principal_id: "user-a".into(),
        purpose: "research".into(),
        permitted_source_authority_refs: authorities
            .iter()
            .map(|value| SourceAuthorityRef((*value).into()))
            .collect(),
        visible_policy_labels: labels.iter().map(|value| (*value).into()).collect(),
        current_grant_epoch: 4,
        allow_stale_but_plannable: false,
        discovery_budget: DiscoveryBudget::for_time_budget(30 * 60 * 1000),
        disclosure_budget: ScopedDisclosureBudget {
            max_results: 10,
            max_payload_bytes: 4096,
            max_relationships: 10,
            include_payloads: true,
            include_evidence_refs: true,
        },
        coverage_evidence: CoverageState::CompleteForScope,
    }
}

fn request(
    selector: DataViewRevisionSelector,
    access: ScopedProjectionAccess,
) -> ScopedDataViewProjectionRequest {
    ScopedDataViewProjectionRequest {
        revision_selector: selector,
        access,
        scoped_discovery_graph: empty_graph(),
        discovery_frontier: empty_frontier(),
    }
}

fn projected(outcome: ScopedDataViewProjectionOutcome) -> private_discovery::ScopedDataView {
    match outcome {
        ScopedDataViewProjectionOutcome::Projected(view) => *view,
        ScopedDataViewProjectionOutcome::RevisionNotAvailable => {
            panic!("expected projected view")
        }
    }
}

#[test]
fn hidden_only_commits_do_not_change_the_callers_public_revision_ref() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "object")))
        .expect("normalizer");
    materialize(
        &mut engine,
        "action:visible",
        "source:visible",
        "visible.json",
        &["tenant-a"],
    );
    let mut projector = ScopedDataViewProjector;
    let first = projected(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Latest,
                    access(&["source:visible"], &["tenant-a"]),
                ),
            )
            .expect("first projection"),
    );
    assert_eq!(first.visible_results.len(), 1);

    materialize(
        &mut engine,
        "action:hidden",
        "source:hidden",
        "hidden.json",
        &["tenant-b"],
    );
    let second = projected(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Latest,
                    access(&["source:visible"], &["tenant-a"]),
                ),
            )
            .expect("second projection"),
    );
    assert_eq!(second.visible_results.len(), 1);
    assert_eq!(second.data_view, first.data_view);

    let historical = projected(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Exact(first.data_view.clone()),
                    access(&["source:visible"], &["tenant-a"]),
                ),
            )
            .expect("historical projection"),
    );
    assert_eq!(historical.projection_mode, ProjectionMode::HistoricalReplay);
    assert_eq!(historical.visible_results.len(), 1);
}

#[test]
fn scoped_revision_aliases_survive_engine_and_projector_restart() {
    let persistence = Arc::new(MemoryDataViewPersistence::default());
    let mut engine = DataViewEngine::with_persistence(
        "global",
        Arc::new(SystemIdGenerator),
        persistence.clone(),
    )
    .expect("engine");
    let first = {
        let mut projector = ScopedDataViewProjector;
        projected(
            projector
                .project(
                    &mut engine,
                    request(
                        DataViewRevisionSelector::Latest,
                        access(&["source:visible"], &["tenant-a"]),
                    ),
                )
                .expect("initial projection"),
        )
    };
    drop(engine);

    let mut reopened =
        DataViewEngine::with_persistence("global", Arc::new(SystemIdGenerator), persistence)
            .expect("reopen engine");
    let mut projector = ScopedDataViewProjector;
    let latest = projected(
        projector
            .project(
                &mut reopened,
                request(
                    DataViewRevisionSelector::Latest,
                    access(&["source:visible"], &["tenant-a"]),
                ),
            )
            .expect("latest after restart"),
    );
    assert_eq!(latest.data_view, first.data_view);
    let exact = projected(
        projector
            .project(
                &mut reopened,
                request(
                    DataViewRevisionSelector::Exact(first.data_view.clone()),
                    access(&["source:visible"], &["tenant-a"]),
                ),
            )
            .expect("exact after restart"),
    );
    assert_eq!(exact.data_view, first.data_view);
    assert_eq!(exact.projection_mode, ProjectionMode::HistoricalReplay);
}

#[test]
fn exact_revision_reapplies_current_policy_and_unknown_refs_are_indistinguishable() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "object")))
        .expect("normalizer");
    materialize(
        &mut engine,
        "action:visible",
        "source:visible",
        "visible.json",
        &["tenant-a"],
    );
    let mut projector = ScopedDataViewProjector;
    let current = projected(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Latest,
                    access(&["source:visible"], &["tenant-a"]),
                ),
            )
            .expect("current projection"),
    );

    assert_eq!(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Exact(current.data_view.clone()),
                    access(&["source:visible"], &[]),
                ),
            )
            .expect("revoked projection"),
        ScopedDataViewProjectionOutcome::RevisionNotAvailable
    );

    let unknown = data_view::DataViewRef {
        data_view_id: "global".into(),
        revision_id: "revision:unknown".into(),
    };
    assert_eq!(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Exact(unknown.clone()),
                    access(&["source:visible"], &["tenant-a"]),
                ),
            )
            .expect("unknown outcome"),
        ScopedDataViewProjectionOutcome::RevisionNotAvailable
    );
    let base_access = access(&["source:visible"], &["tenant-a"]);
    let mut changed_scopes = Vec::new();
    let mut other_subject = base_access.clone();
    other_subject.subject_scope_id = "tenant-a:user-b".into();
    changed_scopes.push(other_subject);
    let mut other_tenant = base_access.clone();
    other_tenant.tenant_id = "tenant-b".into();
    changed_scopes.push(other_tenant);
    let mut other_principal = base_access.clone();
    other_principal.principal_id = "user-b".into();
    changed_scopes.push(other_principal);
    let mut other_purpose = base_access.clone();
    other_purpose.purpose = "audit".into();
    changed_scopes.push(other_purpose);
    let mut other_grant = base_access;
    other_grant.current_grant_epoch = 5;
    changed_scopes.push(other_grant);
    for changed_scope in changed_scopes {
        assert_eq!(
            projector
                .project(
                    &mut engine,
                    request(
                        DataViewRevisionSelector::Exact(current.data_view.clone()),
                        changed_scope,
                    ),
                )
                .expect("unauthorized outcome"),
            ScopedDataViewProjectionOutcome::RevisionNotAvailable
        );
    }
}

// @verify_spec("CONTRACTS.PRIVATE_DISCOVERY.SCOPED_DATA_VIEW", mode="system")
#[test]
fn latest_projection_applies_tenant_principal_and_purpose_policy_labels() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "object")))
        .expect("normalizer");
    materialize(
        &mut engine,
        "action:scoped",
        "source:visible",
        "scoped.json",
        &["tenant:tenant-a", "principal:user-a", "purpose:research"],
    );
    let mut projector = ScopedDataViewProjector;
    let mut allowed = access(
        &["source:visible"],
        &["tenant:tenant-a", "principal:user-a", "purpose:research"],
    );
    assert_eq!(
        projected(
            projector
                .project(
                    &mut engine,
                    request(DataViewRevisionSelector::Latest, allowed.clone()),
                )
                .expect("allowed projection"),
        )
        .visible_results
        .len(),
        1
    );

    for denied_label in ["tenant:tenant-a", "principal:user-a", "purpose:research"] {
        allowed.visible_policy_labels.remove(denied_label);
        let denied = projected(
            projector
                .project(
                    &mut engine,
                    request(DataViewRevisionSelector::Latest, allowed.clone()),
                )
                .expect("denied projection"),
        );
        assert!(denied.visible_results.is_empty());
        allowed.visible_policy_labels.insert(denied_label.into());
    }
}

// @verify_spec("CONTRACTS.DATA_VIEW.RELATIONSHIP_EVIDENCE_PRESERVATION", mode="system")
#[test]
fn scoped_relationship_requires_both_visible_endpoints_and_redacts_hidden_lineage() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    engine
        .register_normalizer(Arc::new(RelationshipNormalizer { schema: schema() }))
        .expect("normalizer");
    materialize(&mut engine, "action:a", "source:a", "a.json", &["tenant-a"]);
    materialize(&mut engine, "action:b", "source:b", "b.json", &["tenant-a"]);
    let mut projector = ScopedDataViewProjector;
    let fully_visible = projected(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Latest,
                    access(&["source:a", "source:b"], &["tenant-a"]),
                ),
            )
            .expect("full projection"),
    );
    assert_eq!(fully_visible.visible_results.len(), 2);
    assert_eq!(fully_visible.visible_relationships.len(), 1);
    assert_eq!(
        fully_visible.visible_relationships[0].evidence_refs,
        vec!["evidence:relationship:a-to-b"]
    );

    let one_endpoint = projected(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Latest,
                    access(&["source:b"], &["tenant-a"]),
                ),
            )
            .expect("one-endpoint projection"),
    );
    assert_eq!(one_endpoint.visible_results.len(), 1);
    assert!(one_endpoint.visible_relationships.is_empty());

    let mut redacted_access = access(&["source:a", "source:b"], &["tenant-a"]);
    redacted_access.disclosure_budget.include_evidence_refs = false;
    let redacted = projected(
        projector
            .project(
                &mut engine,
                request(DataViewRevisionSelector::Latest, redacted_access),
            )
            .expect("redacted projection"),
    );
    assert_eq!(redacted.visible_relationships.len(), 1);
    assert!(redacted.visible_relationships[0].evidence_refs.is_empty());
    assert!(redacted.visible_relationships[0].lineage_redacted);
}

#[test]
fn disclosure_budget_downgrades_coverage_without_exposing_hidden_counts() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "object")))
        .expect("normalizer");
    materialize(
        &mut engine,
        "action:a",
        "source:visible",
        "a.json",
        &["tenant-a"],
    );
    materialize(
        &mut engine,
        "action:b",
        "source:visible",
        "b.json",
        &["tenant-a"],
    );
    let mut limited_access = access(&["source:visible"], &["tenant-a"]);
    limited_access.disclosure_budget.max_results = 1;
    let mut projector = ScopedDataViewProjector;
    let view = projected(
        projector
            .project(
                &mut engine,
                request(DataViewRevisionSelector::Latest, limited_access),
            )
            .expect("projection"),
    );
    assert_eq!(view.visible_results.len(), 1);
    assert_eq!(view.coverage, CoverageState::Partial);
    assert_eq!(view.limiting_reasons, vec!["disclosure-budget-exhausted"]);
    assert_eq!(
        view.visible_results
            .iter()
            .map(|result| result.result_ref.clone())
            .collect::<HashSet<_>>()
            .len(),
        1
    );
}

#[test]
fn frontier_and_continuations_are_limited_to_visible_budgeted_actions() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    let mut visible_action = action("action:visible", "source:visible");
    visible_action.continuation_policy = ContinuationPolicy::CursorBounded;
    let hidden_action = action("action:hidden", "source:hidden");
    engine
        .register_action(visible_action.clone())
        .expect("register visible action");
    let graph = ScopedDiscoveryGraph {
        resource_nodes: vec![],
        action_nodes: vec![
            ActionNode {
                action_id: visible_action.action_id.clone(),
                operation: visible_action.operation.clone(),
                source_authority_ref: "source:visible".into(),
            },
            ActionNode {
                action_id: hidden_action.action_id.clone(),
                operation: hidden_action.operation.clone(),
                source_authority_ref: "source:hidden".into(),
            },
        ],
        visibility_edges: vec![],
        executable_edges: vec![],
        expansion_edges: vec![],
        source_authority_refs: vec!["source:visible".into(), "source:hidden".into()],
        policy_constraints: vec![],
        freshness_constraints: vec![],
        expires_at_ms: 1000,
    };
    let frontier = DiscoveryFrontier {
        strategy: DiscoveryFrontierStrategy::BalancedBreadthThenDepth,
        ranked_actions: vec![
            RankedSourceAction {
                rank: 1,
                action: visible_action,
            },
            RankedSourceAction {
                rank: 2,
                action: hidden_action,
            },
        ],
        skipped_branches: vec!["source:hidden".into()],
        explanation: "contains hidden branch details".into(),
    };
    let mut projector = ScopedDataViewProjector;
    let visible_access = access(&["source:visible"], &["tenant-a"]);
    let view = projected(
        projector
            .project(
                &mut engine,
                ScopedDataViewProjectionRequest {
                    revision_selector: DataViewRevisionSelector::Latest,
                    access: visible_access.clone(),
                    scoped_discovery_graph: graph.clone(),
                    discovery_frontier: frontier.clone(),
                },
            )
            .expect("projection"),
    );
    assert_eq!(view.discovery_frontier.ranked_actions.len(), 1);
    assert!(view.discovery_frontier.skipped_branches.is_empty());
    assert_eq!(view.visible_continuation_action_ids, vec!["action:visible"]);
    assert!(
        projector
            .issue_action_context(
                &mut engine,
                &view,
                &visible_access,
                "action:visible",
                "policy-a",
                4,
                100,
                200,
            )
            .is_ok()
    );

    let historical = projected(
        projector
            .project(
                &mut engine,
                ScopedDataViewProjectionRequest {
                    revision_selector: DataViewRevisionSelector::Exact(view.data_view.clone()),
                    access: visible_access.clone(),
                    scoped_discovery_graph: graph,
                    discovery_frontier: frontier,
                },
            )
            .expect("historical projection"),
    );
    assert_eq!(
        projector.issue_action_context(
            &mut engine,
            &historical,
            &visible_access,
            "action:visible",
            "policy-a",
            4,
            100,
            200,
        ),
        Err(DataViewContractError::HistoricalReplayIsReadOnly)
    );
}

#[test]
fn short_broad_scan_keeps_metadata_actions_without_admitting_data_probes() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    let metadata_action = action("action:head", "source:visible");
    let mut data_probe = action("action:sample", "source:visible");
    data_probe.operation = DiscoveryOperation::LimitedSample;
    let graph = ScopedDiscoveryGraph {
        resource_nodes: vec![],
        action_nodes: vec![
            ActionNode {
                action_id: metadata_action.action_id.clone(),
                operation: metadata_action.operation.clone(),
                source_authority_ref: "source:visible".into(),
            },
            ActionNode {
                action_id: data_probe.action_id.clone(),
                operation: data_probe.operation.clone(),
                source_authority_ref: "source:visible".into(),
            },
        ],
        visibility_edges: vec![],
        executable_edges: vec![],
        expansion_edges: vec![],
        source_authority_refs: vec!["source:visible".into()],
        policy_constraints: vec![],
        freshness_constraints: vec![],
        expires_at_ms: 1000,
    };
    let frontier = DiscoveryFrontier {
        strategy: DiscoveryFrontierStrategy::ShortBroadScan,
        ranked_actions: vec![
            RankedSourceAction {
                rank: 1,
                action: metadata_action,
            },
            RankedSourceAction {
                rank: 2,
                action: data_probe,
            },
        ],
        skipped_branches: vec![],
        explanation: "short scan".into(),
    };
    let mut short_access = access(&["source:visible"], &["tenant-a"]);
    short_access.discovery_budget = DiscoveryBudget::for_time_budget(5 * 60 * 1000);
    let mut projector = ScopedDataViewProjector;
    let view = projected(
        projector
            .project(
                &mut engine,
                ScopedDataViewProjectionRequest {
                    revision_selector: DataViewRevisionSelector::Latest,
                    access: short_access,
                    scoped_discovery_graph: graph,
                    discovery_frontier: frontier,
                },
            )
            .expect("projection"),
    );

    assert_eq!(view.discovery_frontier.ranked_actions.len(), 1);
    assert_eq!(
        view.discovery_frontier.ranked_actions[0].action.action_id,
        "action:head"
    );
}

#[test]
fn private_discovery_consumes_revisioned_scoped_data_view_evidence() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "s3-object")))
        .expect("normalizer");
    materialize(
        &mut engine,
        "action:s3",
        "source:s3:corp-bucket",
        "reports/q1.csv",
        &["tenant-a"],
    );
    let mut projector = ScopedDataViewProjector;
    let scoped = projected(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Latest,
                    access(&["source:s3:corp-bucket"], &["tenant-a"]),
                ),
            )
            .expect("projection"),
    );
    let discovery = PrivateDiscoveryOrchestrator::new().discover(
        PrivateDataDiscoveryRequest {
            discovery_intent: DiscoveryIntent {
                task: "find report evidence".into(),
                priorities: vec![],
            },
            policy_context: PolicyContext {
                user_id: "user-a".into(),
                tenant_id: "tenant-a".into(),
                task_purpose: "research".into(),
                allowed_source_instances: BTreeSet::from([SourceInstanceKey::new(
                    SourceKind::S3,
                    "corp-bucket",
                )]),
                grant_epoch: Some(4),
                request_context: Default::default(),
            },
            discovery_scope: DiscoveryScope {
                source_instances: BTreeSet::new(),
                max_fragments: 10,
            },
            time_budget_ms: 5 * 60 * 1000,
            disclosure_budget: DisclosureBudget {
                max_fragments: 10,
                max_names: 10,
                max_relation_hints: 10,
            },
            candidate_fragments: vec![],
            scoped_data_view: Some(scoped),
            source_capabilities: vec![],
        },
        130,
    );
    assert_eq!(discovery.visible_fragments.len(), 1);
    assert_eq!(
        discovery.visible_fragments[0].source_authority_ref,
        "source:s3:corp-bucket"
    );
    assert!(!discovery.discovery_frontier.ranked_actions.is_empty());
}

#[test]
fn compliance_redaction_makes_old_scoped_revision_generically_unavailable() {
    let mut engine = DataViewEngine::new("global").expect("engine");
    engine
        .register_normalizer(Arc::new(SingleResultNormalizer::new(schema(), "object")))
        .expect("normalizer");
    materialize(
        &mut engine,
        "action:visible",
        "source:visible",
        "visible.json",
        &["tenant-a"],
    );
    let result_ref = engine
        .store()
        .tracked_result_refs()
        .into_iter()
        .next()
        .expect("tracked result");
    let mut projector = ScopedDataViewProjector;
    let before = projected(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Latest,
                    access(&["source:visible"], &["tenant-a"]),
                ),
            )
            .expect("projection before deletion"),
    );
    engine
        .apply_retention(&RetentionPolicy {
            now_ms: 130,
            actionable_ttl_ms: 10_000,
            history_ttl_ms: 10_000,
            max_payload_versions: 100,
            max_payload_bytes: 1024 * 1024,
            source_actionable_ttl_ms: BTreeMap::new(),
            tenant_policy_labels_to_delete: BTreeSet::new(),
            compliance_delete_result_refs: BTreeSet::from([result_ref]),
            retain_minimum_tombstones: true,
        })
        .expect("compliance deletion");

    assert_eq!(
        projector
            .project(
                &mut engine,
                request(
                    DataViewRevisionSelector::Exact(before.data_view),
                    access(&["source:visible"], &["tenant-a"]),
                ),
            )
            .expect("old exact projection"),
        ScopedDataViewProjectionOutcome::RevisionNotAvailable
    );
}
