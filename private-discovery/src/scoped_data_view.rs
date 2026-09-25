use std::collections::{BTreeMap, BTreeSet};

use data_view::{
    ActionExecutionContext, ContinuationPolicy, CoverageState, DataViewContractError,
    DataViewEngine, DataViewRef, FreshnessState, PayloadEncoding, ProjectionMode,
    RelationshipAssertion, TrackedResultRef,
};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{DiscoveryBudget, DiscoveryFrontier, FreshnessGuard, ScopedDiscoveryGraph};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "kebab-case")]
pub enum DataViewRevisionSelector {
    Latest,
    Exact(DataViewRef),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedDisclosureBudget {
    pub max_results: usize,
    pub max_payload_bytes: usize,
    pub max_relationships: usize,
    pub include_payloads: bool,
    pub include_evidence_refs: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedProjectionAccess {
    pub subject_scope_id: String,
    pub tenant_id: String,
    pub principal_id: String,
    pub purpose: String,
    #[serde(default)]
    pub permitted_source_authority_refs: BTreeSet<data_view::SourceAuthorityRef>,
    #[serde(default)]
    pub visible_policy_labels: BTreeSet<String>,
    pub current_grant_epoch: u64,
    pub allow_stale_but_plannable: bool,
    pub discovery_budget: DiscoveryBudget,
    pub disclosure_budget: ScopedDisclosureBudget,
    pub coverage_evidence: CoverageState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedDataViewProjectionRequest {
    pub revision_selector: DataViewRevisionSelector,
    pub access: ScopedProjectionAccess,
    pub scoped_discovery_graph: ScopedDiscoveryGraph,
    pub discovery_frontier: DiscoveryFrontier,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedVisibleResult {
    pub result_ref: TrackedResultRef,
    pub source_authority_ref: data_view::SourceAuthorityRef,
    pub entity_kind: String,
    #[serde(default)]
    pub native_anchors: Vec<data_view::NativeAnchor>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub properties: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_encoding: Option<PayloadEncoding>,
    pub observed_at_ms: u64,
    pub freshness: FreshnessState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedRelationship {
    pub relationship_id: String,
    pub source_result_ref: TrackedResultRef,
    pub target_result_ref: TrackedResultRef,
    pub relationship_kind: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derivation_rule: Option<String>,
    pub freshness: FreshnessState,
    pub lineage_redacted: bool,
}

impl ScopedRelationship {
    fn from_assertion(assertion: &RelationshipAssertion, include_evidence_refs: bool) -> Self {
        Self {
            relationship_id: assertion.relationship_id.clone(),
            source_result_ref: assertion.source_result_ref.clone(),
            target_result_ref: assertion.target_result_ref.clone(),
            relationship_kind: assertion.relationship_kind.clone(),
            evidence_refs: if include_evidence_refs {
                assertion.evidence_refs.clone()
            } else {
                Vec::new()
            },
            derivation_rule: assertion.derivation_rule.clone(),
            freshness: assertion.freshness.clone(),
            lineage_redacted: !include_evidence_refs && !assertion.evidence_refs.is_empty(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedDataView {
    pub data_view: DataViewRef,
    pub projection_mode: ProjectionMode,
    #[serde(default)]
    pub visible_results: Vec<ScopedVisibleResult>,
    #[serde(default)]
    pub visible_relationships: Vec<ScopedRelationship>,
    pub scoped_discovery_graph: ScopedDiscoveryGraph,
    pub discovery_frontier: DiscoveryFrontier,
    pub coverage: CoverageState,
    #[serde(default)]
    pub limiting_reasons: Vec<String>,
    #[serde(default)]
    pub visible_continuation_action_ids: Vec<String>,
    #[serde(default)]
    pub freshness_constraints: Vec<FreshnessGuard>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScopedDataViewProjectionOutcome {
    Projected(Box<ScopedDataView>),
    RevisionNotAvailable,
}

#[derive(Debug, Default, Clone)]
pub struct ScopedDataViewProjector;

impl ScopedDataViewProjector {
    pub fn project(
        &mut self,
        engine: &mut DataViewEngine,
        request: ScopedDataViewProjectionRequest,
    ) -> Result<ScopedDataViewProjectionOutcome, DataViewContractError> {
        validate_projection_access(&request.access)?;
        let scope_binding = projection_scope_binding(&request.access);
        let (internal_ref, projection_mode, requested_public_ref) = match &request.revision_selector
        {
            DataViewRevisionSelector::Latest => {
                (engine.head_ref(), ProjectionMode::CurrentActionable, None)
            }
            DataViewRevisionSelector::Exact(public_ref) => {
                let Some(internal_ref) =
                    engine.resolve_scoped_revision_alias(public_ref, &scope_binding)
                else {
                    return Ok(ScopedDataViewProjectionOutcome::RevisionNotAvailable);
                };
                (
                    internal_ref,
                    ProjectionMode::HistoricalReplay,
                    Some(public_ref.clone()),
                )
            }
        };
        let store = engine.store();
        let Some(snapshot) = store.snapshot(&internal_ref) else {
            return Ok(ScopedDataViewProjectionOutcome::RevisionNotAvailable);
        };

        let mut visible_results = Vec::new();
        let mut visible_result_refs = BTreeSet::new();
        let mut visible_version_keys = Vec::new();
        let mut used_payload_bytes = 0_usize;
        let mut budget_limited = false;
        for (result_ref, version_ref) in &snapshot.selected_payload_versions {
            let Some(version) = store.payload_version(version_ref) else {
                return Ok(ScopedDataViewProjectionOutcome::RevisionNotAvailable);
            };
            if !result_is_visible(version, &request.access) {
                continue;
            }
            let Some(attachment) = store.attachment(&version.attachment_ref) else {
                return Ok(ScopedDataViewProjectionOutcome::RevisionNotAvailable);
            };
            let payload_bytes = if request.access.disclosure_budget.include_payloads {
                attachment.payload.len()
            } else {
                0
            };
            if visible_results.len() >= request.access.disclosure_budget.max_results
                || used_payload_bytes.saturating_add(payload_bytes)
                    > request.access.disclosure_budget.max_payload_bytes
            {
                budget_limited = true;
                continue;
            }
            used_payload_bytes += payload_bytes;
            visible_result_refs.insert(result_ref.clone());
            visible_version_keys.push(format!("{}:{}", result_ref.0, version.selection_key));
            visible_results.push(ScopedVisibleResult {
                result_ref: result_ref.clone(),
                source_authority_ref: version.source_authority_ref.clone(),
                entity_kind: version.canonical_identity.entity_kind.clone(),
                native_anchors: version.native_anchors.clone(),
                evidence_refs: if request.access.disclosure_budget.include_evidence_refs {
                    version.evidence_refs.clone()
                } else {
                    Vec::new()
                },
                properties: version.canonical_identity.properties.clone(),
                payload: request
                    .access
                    .disclosure_budget
                    .include_payloads
                    .then(|| attachment.payload.clone()),
                payload_encoding: request
                    .access
                    .disclosure_budget
                    .include_payloads
                    .then(|| attachment.encoding.clone()),
                observed_at_ms: version.observed_at_ms,
                freshness: version.freshness.clone(),
            });
        }

        let mut visible_relationships = Vec::new();
        for version_ref in snapshot.selected_relationship_versions.values() {
            if visible_relationships.len() >= request.access.disclosure_budget.max_relationships {
                budget_limited = true;
                break;
            }
            let Some(version) = store.relationship_version(version_ref) else {
                return Ok(ScopedDataViewProjectionOutcome::RevisionNotAvailable);
            };
            if visible_result_refs.contains(&version.assertion.source_result_ref)
                && visible_result_refs.contains(&version.assertion.target_result_ref)
            {
                visible_version_keys.push(format!(
                    "relationship:{}:{}",
                    version.assertion.relationship_id, version.version_ref
                ));
                visible_relationships.push(ScopedRelationship::from_assertion(
                    &version.assertion,
                    request.access.disclosure_budget.include_evidence_refs,
                ));
            }
        }

        let scoped_graph = filter_graph(
            request.scoped_discovery_graph,
            &request.access.permitted_source_authority_refs,
        );
        let visible_action_ids = scoped_graph
            .action_nodes
            .iter()
            .map(|node| node.action_id.as_str())
            .collect::<BTreeSet<_>>();
        let frontier = filter_frontier(
            request.discovery_frontier,
            &visible_action_ids,
            &request.access.discovery_budget,
        );
        let visible_continuation_action_ids = frontier
            .ranked_actions
            .iter()
            .filter(|ranked| ranked.action.continuation_policy == ContinuationPolicy::CursorBounded)
            .map(|ranked| ranked.action.action_id.clone())
            .collect::<Vec<_>>();
        let freshness_constraints = scoped_graph.freshness_constraints.clone();

        visible_version_keys.sort();
        let visible_digest = visible_snapshot_digest(
            &visible_version_keys,
            &request.access.purpose,
            request.access.disclosure_budget.include_payloads,
            request.access.disclosure_budget.include_evidence_refs,
        );
        let public_ref = if let Some(public_ref) = requested_public_ref {
            public_ref
        } else {
            engine.get_or_create_scoped_revision_alias(
                &scope_binding,
                &visible_digest,
                &internal_ref,
            )?
        };

        let mut limiting_reasons = Vec::new();
        let coverage = if budget_limited {
            limiting_reasons.push("disclosure-budget-exhausted".into());
            CoverageState::Partial
        } else {
            request.access.coverage_evidence
        };
        Ok(ScopedDataViewProjectionOutcome::Projected(Box::new(
            ScopedDataView {
                data_view: public_ref,
                projection_mode,
                visible_results,
                visible_relationships,
                scoped_discovery_graph: scoped_graph,
                discovery_frontier: frontier,
                coverage,
                limiting_reasons,
                visible_continuation_action_ids,
                freshness_constraints,
            },
        )))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn issue_action_context(
        &self,
        engine: &mut DataViewEngine,
        scoped_data_view: &ScopedDataView,
        access: &ScopedProjectionAccess,
        action_id: &str,
        policy_context_digest: impl Into<String>,
        grant_epoch: u64,
        issued_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<ActionExecutionContext, DataViewContractError> {
        if scoped_data_view.projection_mode != ProjectionMode::CurrentActionable {
            return Err(DataViewContractError::HistoricalReplayIsReadOnly);
        }
        validate_projection_access(access)?;
        if !scoped_data_view
            .discovery_frontier
            .ranked_actions
            .iter()
            .any(|ranked| ranked.action.action_id == action_id)
        {
            return Err(DataViewContractError::NotFound("visible action"));
        }
        let internal_ref = engine
            .resolve_scoped_revision_alias(
                &scoped_data_view.data_view,
                &projection_scope_binding(access),
            )
            .ok_or(DataViewContractError::NotFound("scoped revision"))?;
        engine.issue_action_context(
            action_id,
            internal_ref,
            ProjectionMode::CurrentActionable,
            policy_context_digest,
            grant_epoch,
            issued_at_ms,
            expires_at_ms,
        )
    }
}

fn validate_projection_access(
    access: &ScopedProjectionAccess,
) -> Result<(), DataViewContractError> {
    if access.subject_scope_id.trim().is_empty()
        || access.tenant_id.trim().is_empty()
        || access.principal_id.trim().is_empty()
        || access.purpose.trim().is_empty()
        || access.disclosure_budget.max_results == 0
        || access.disclosure_budget.max_relationships == 0
    {
        return Err(DataViewContractError::InvalidContract(
            "scoped projection access",
        ));
    }
    Ok(())
}

fn result_is_visible(
    version: &data_view::PayloadVersionRecord,
    access: &ScopedProjectionAccess,
) -> bool {
    access
        .permitted_source_authority_refs
        .contains(&version.source_authority_ref)
        && version.grant_epoch == access.current_grant_epoch
        && version
            .policy_labels
            .iter()
            .all(|label| access.visible_policy_labels.contains(label))
        && match version.freshness {
            FreshnessState::Fresh => true,
            FreshnessState::StaleButPlannable => access.allow_stale_but_plannable,
            FreshnessState::RequiresRevalidation | FreshnessState::Invalidated => false,
        }
}

fn projection_scope_binding(access: &ScopedProjectionAccess) -> String {
    let mut material = String::new();
    for component in [
        access.subject_scope_id.as_str(),
        access.tenant_id.as_str(),
        access.principal_id.as_str(),
        access.purpose.as_str(),
    ] {
        material.push_str(component);
        material.push('\0');
    }
    material.push_str(&access.current_grant_epoch.to_string());
    material.push('\0');
    for authority in &access.permitted_source_authority_refs {
        material.push_str(authority.as_ref());
        material.push('\0');
    }
    material.push('\u{1}');
    for label in &access.visible_policy_labels {
        material.push_str(label);
        material.push('\0');
    }
    let hash = digest(&SHA256, material.as_bytes());
    let mut encoded = String::with_capacity(6 + hash.as_ref().len() * 2);
    encoded.push_str("scope:");
    for byte in hash.as_ref() {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn filter_graph(
    mut graph: ScopedDiscoveryGraph,
    permitted_authorities: &BTreeSet<data_view::SourceAuthorityRef>,
) -> ScopedDiscoveryGraph {
    let permitted = permitted_authorities
        .iter()
        .map(|authority| authority.0.as_str())
        .collect::<BTreeSet<_>>();
    graph
        .resource_nodes
        .retain(|node| permitted.contains(node.source_authority_ref.as_str()));
    graph
        .action_nodes
        .retain(|node| permitted.contains(node.source_authority_ref.as_str()));
    let resource_ids = graph
        .resource_nodes
        .iter()
        .map(|node| node.resource_id.as_str())
        .collect::<BTreeSet<_>>();
    let action_ids = graph
        .action_nodes
        .iter()
        .map(|node| node.action_id.as_str())
        .collect::<BTreeSet<_>>();
    graph.visibility_edges.retain(|edge| {
        resource_ids.contains(edge.to.as_str()) || resource_ids.contains(edge.from.as_str())
    });
    graph.executable_edges.retain(|edge| {
        action_ids.contains(edge.to.as_str()) || action_ids.contains(edge.from.as_str())
    });
    graph.expansion_edges.retain(|edge| {
        action_ids.contains(edge.to.as_str()) || action_ids.contains(edge.from.as_str())
    });
    graph
        .source_authority_refs
        .retain(|authority| permitted.contains(authority.as_str()));
    graph
        .freshness_constraints
        .retain(|guard| permitted.contains(guard.source_authority_ref.as_str()));
    graph
}

fn filter_frontier(
    mut frontier: DiscoveryFrontier,
    visible_action_ids: &BTreeSet<&str>,
    budget: &DiscoveryBudget,
) -> DiscoveryFrontier {
    let short_broad_scan = matches!(
        frontier.strategy,
        crate::DiscoveryFrontierStrategy::ShortBroadScan
    );
    frontier.ranked_actions.retain(|ranked| {
        visible_action_ids.contains(ranked.action.action_id.as_str())
            && ranked.action.max_latency_ms <= budget.remaining_time_ms
            && ranked.action.max_calls <= budget.remaining_calls
            && ranked.action.max_fanout <= budget.remaining_fanout
            && ranked.action.max_cost_units <= budget.remaining_cost_units
            && if short_broad_scan {
                !matches!(
                    ranked.action.operation,
                    data_view::DiscoveryOperation::LimitedSample
                        | data_view::DiscoveryOperation::AggregateProbe
                )
            } else {
                ranked.action.max_rows <= budget.remaining_rows
                    && ranked.action.max_bytes <= budget.remaining_bytes
            }
    });
    for (index, ranked) in frontier.ranked_actions.iter_mut().enumerate() {
        ranked.rank = index + 1;
    }
    frontier.skipped_branches.clear();
    frontier.explanation = "bounded visible frontier".into();
    frontier
}

fn visible_snapshot_digest(
    visible_version_keys: &[String],
    purpose: &str,
    include_payloads: bool,
    include_evidence_refs: bool,
) -> String {
    let mut material = visible_version_keys.join("\u{0}");
    material.push('\0');
    material.push_str(purpose);
    material.push('\0');
    material.push_str(if include_payloads {
        "payloads"
    } else {
        "metadata"
    });
    material.push('\0');
    material.push_str(if include_evidence_refs {
        "evidence"
    } else {
        "redacted-evidence"
    });
    let hash = digest(&SHA256, material.as_bytes());
    let mut encoded = String::with_capacity(hash.as_ref().len() * 2);
    for byte in hash.as_ref() {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}
