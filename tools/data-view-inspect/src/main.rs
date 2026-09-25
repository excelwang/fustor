use base64::{Engine, engine::general_purpose::STANDARD};
use capanix_app_sdk::{
    Event,
    runtime::{EventMetadata, NodeId},
};
use data_view::{
    ActionExecutionContext, DeclaredResultPolicy, DeclaredResultSubmission, DerivationEvidence,
    FileDataViewPersistence, FreshnessState, SchemaVersionRef, SourceAuthorityRef,
};
use data_view_normalizers::{SourceActionSubmissionContext, map_builtin_source_action_submission};
use private_discovery::ScopedDataViewProjectionOutcome;
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    io::{self, Read},
    path::PathBuf,
    sync::Arc,
};
use union_graph_runtime::{
    BuildScopedDataViewRequest, DataViewRuntimePayload, DataViewRuntimeRequest,
    DataViewRuntimeService,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    data_view_id: String,
    state_path: PathBuf,
    allowed_state_root: PathBuf,
    #[serde(default)]
    declared_policies: Vec<DeclaredResultPolicy>,
    #[serde(default)]
    source_policies: Vec<SourcePolicy>,
    scoped_request: BuildScopedDataViewRequest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePolicy {
    profile_id: String,
    source_kind: String,
    source_authority_ref: SourceAuthorityRef,
    authority_object_ref: String,
    producer_id: String,
    policy_context_digest: String,
    policy_labels: BTreeSet<String>,
    max_ttl_ms: u64,
}
impl SourcePolicy {
    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.profile_id.trim().is_empty()
            || self.producer_id.trim().is_empty()
            || self.source_authority_ref.0.trim().is_empty()
            || self.authority_object_ref.trim().is_empty()
            || self.policy_context_digest.trim().is_empty()
            || self.max_ttl_ms == 0
            || self.max_ttl_ms > 300_000
            || ![
                "jsonl", "csv", "local-fs", "s3", "mysql", "es", "pg", "graph",
            ]
            .contains(&self.source_kind.as_str())
        {
            return Err("invalid source admission policy".into());
        }
        Ok(())
    }
}
impl Config {
    fn source_policy(&self, id: &str) -> Result<&SourcePolicy, Box<dyn std::error::Error>> {
        self.source_policies
            .iter()
            .find(|policy| {
                policy.profile_id == id
                    && self
                        .scoped_request
                        .access
                        .permitted_source_authority_refs
                        .contains(&policy.source_authority_ref)
                    && policy
                        .policy_labels
                        .is_subset(&self.scoped_request.access.visible_policy_labels)
            })
            .ok_or_else(|| "source admission unavailable".into())
    }
}
fn bounded(reader: impl Read) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    reader.take(2 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 2 * 1024 * 1024 {
        return Err("input exceeds 2 MiB".into());
    }
    Ok(bytes)
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 3
        || args[1] != "--config"
        || ![
            "scoped",
            "admission-issue",
            "declared-submit",
            "declared-inspect",
            "declared-capability",
            "action-issue",
            "source-submit",
            "validate",
        ]
        .contains(&args[0].as_str())
    {
        return Err("usage: fustor-data-view-inspect scoped|admission-issue|declared-submit|declared-inspect|declared-capability|action-issue|source-submit|validate --config native.json".into());
    }
    let mut config: Config = serde_json::from_slice(&bounded(std::fs::File::open(&args[2])?)?)?;
    if !config.state_path.is_absolute()
        || config.declared_policies.len() > 20
        || config.source_policies.len() > 20
    {
        return Err("invalid native DataView config".into());
    }
    let root = config.allowed_state_root.canonicalize()?;
    let parent = config
        .state_path
        .parent()
        .ok_or("state parent missing")?
        .canonicalize()?;
    if !parent.starts_with(&root)
        || std::fs::symlink_metadata(&config.state_path).is_ok_and(|m| m.file_type().is_symlink())
    {
        return Err("state path outside authorized native root".into());
    }
    for policy in &config.declared_policies {
        policy.validate()?;
    }
    let mut ids = BTreeSet::new();
    for policy in &config.source_policies {
        policy.validate()?;
        if !ids.insert(&policy.profile_id) {
            return Err("duplicate source admission profile".into());
        }
    }
    if args[0] == "validate" {
        println!("{{\"valid\":true}}");
        return Ok(());
    }
    let now_ms = source_kit::source_now_ms();
    config.scoped_request.now_ms = now_ms;
    // Source actions are issued only from the owner's freshly projected scope.
    // No source driver or caller-authored graph assertions enter this process.
    let read_only = args[0] == "declared-inspect" || args[0] == "declared-capability";
    let persistence = if read_only {
        FileDataViewPersistence::new_read_only(config.state_path.clone())
    } else {
        FileDataViewPersistence::new_exclusive(config.state_path.clone())?
    };
    let service = DataViewRuntimeService::with_persistence(
        config.data_view_id.clone(),
        NodeId("dataview-inspect".into()),
        Arc::new(persistence),
    )?;
    for policy in &config.declared_policies {
        if args[0] == "declared-inspect" && (policy.grant_epoch != config.scoped_request.access.current_grant_epoch
            || !config.scoped_request.access.permitted_source_authority_refs.contains(&policy.result_authority_ref)
            || !policy.required_policy_labels.is_subset(&config.scoped_request.access.visible_policy_labels)) {
            continue;
        }
        service.register_declared_policy(policy.clone())?;
    }
    let request = match args[0].as_str() {
        "declared-capability" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input { profile_id: String, producer_id: String, schema: SchemaVersionRef }
            let input: Input = serde_json::from_slice(&bounded(io::stdin())?)?;
            DataViewRuntimeRequest::InspectDeclaredCapability { profile_id: input.profile_id, producer_id: input.producer_id, schema: input.schema }
        }
        "declared-inspect" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input { profile_id: String, idempotency_key: String }
            let input: Input = serde_json::from_slice(&bounded(io::stdin())?)?;
            DataViewRuntimeRequest::InspectDeclaredOutcome { profile_id: input.profile_id, idempotency_key: input.idempotency_key }
        }
        "scoped" => {
            // Scope, disclosure budgets, principal, grants and policy are owner configuration.
            // The browser cannot request an unrestricted head or override them.
            config.scoped_request.now_ms = now_ms;
            DataViewRuntimeRequest::BuildScopedDataView {
                request: config.scoped_request,
            }
        }
        "admission-issue" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input {
                profile_id: String,
                idempotency_key: String,
                ttl_ms: u64,
            }
            let input: Input = serde_json::from_slice(&bounded(io::stdin())?)?;
            DataViewRuntimeRequest::IssueDeclaredAdmission {
                profile_id: input.profile_id,
                idempotency_key: input.idempotency_key,
                now_ms,
                ttl_ms: input.ttl_ms,
            }
        }
        "declared-submit" => {
            let submission: DeclaredResultSubmission =
                serde_json::from_slice(&bounded(io::stdin())?)?;
            let policy = config
                .declared_policies
                .iter()
                .find(|p| p.profile_id == submission.admission.policy.profile_id)
                .ok_or("declared policy unavailable")?;
            DataViewRuntimeRequest::SubmitDeclaredResult {
                submission,
                now_ms,
                current_policy_context_digest: policy.policy_context_digest.clone(),
                current_grant_epoch: policy.grant_epoch,
            }
        }
        "action-issue" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input {
                profile_id: String,
                action_id: String,
                ttl_ms: u64,
            }
            let input: Input = serde_json::from_slice(&bounded(io::stdin())?)?;
            let policy = config.source_policy(&input.profile_id)?;
            if input.ttl_ms == 0 || input.ttl_ms > policy.max_ttl_ms {
                return Err("action context lifetime exceeds policy".into());
            }
            let projected = invoke(
                &service,
                DataViewRuntimeRequest::BuildScopedDataView {
                    request: config.scoped_request.clone(),
                },
                now_ms,
            )?;
            let visible = match projected {
                DataViewRuntimePayload::ScopedDataView(
                    ScopedDataViewProjectionOutcome::Projected(view),
                ) => view.discovery_frontier.ranked_actions.iter().any(|ranked| {
                    ranked.action.action_id == input.action_id
                        && ranked.action.source_authority_ref == policy.source_authority_ref
                }),
                _ => false,
            };
            if !visible {
                return Err("source admission unavailable".into());
            }
            DataViewRuntimeRequest::IssueVisibleActionContext {
                request: config.scoped_request.clone(),
                action_id: input.action_id,
                policy_context_digest: policy.policy_context_digest.clone(),
                grant_epoch: config.scoped_request.access.current_grant_epoch,
                issued_at_ms: now_ms,
                expires_at_ms: now_ms + input.ttl_ms,
            }
        }
        "source-submit" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Input {
                profile_id: String,
                submission_id: String,
                execution: ActionExecutionContext,
                response_base64: String,
            }
            let input: Input = serde_json::from_slice(&bounded(io::stdin())?)?;
            let policy = config.source_policy(&input.profile_id)?;
            if input.execution.grant_epoch != config.scoped_request.access.current_grant_epoch
                || input.execution.policy_context_digest != policy.policy_context_digest
            {
                return Err("source admission unavailable".into());
            }
            let submission = map_builtin_source_action_submission(
                SourceActionSubmissionContext {
                    submission_id: input.submission_id,
                    execution: input.execution,
                    source_authority_ref: policy.source_authority_ref.clone(),
                    producer_id: policy.producer_id.clone(),
                    schema: SchemaVersionRef {
                        schema_ref: format!("fustor.{}.source-payload", policy.source_kind),
                        schema_version: "1".into(),
                    },
                    policy_labels: policy.policy_labels.clone(),
                    freshness: FreshnessState::Fresh,
                    derivation: DerivationEvidence {
                        derivation_kind: "bounded-source-observation".into(),
                        rule_ref: None,
                        dependency_refs: vec![],
                    },
                },
                STANDARD.decode(input.response_base64)?,
            )?;
            if submission.authority_object_ref != policy.authority_object_ref {
                return Err("source admission unavailable".into());
            }
            DataViewRuntimeRequest::SubmitActionResult {
                submission,
                now_ms,
                current_policy_context_digest: policy.policy_context_digest.clone(),
            }
        }
        _ => unreachable!(),
    };
    let payload = invoke(&service, request, now_ms)?;
    println!(
        "{}",
        serde_json::json!({"adapter":"fustor.native.dataview.v1", "wireEncoding":"msgpack", "response":payload})
    );
    Ok(())
}
fn invoke(
    service: &DataViewRuntimeService,
    request: DataViewRuntimeRequest,
    now_ms: u64,
) -> Result<DataViewRuntimePayload, Box<dyn std::error::Error>> {
    let event = Event::new(
        EventMetadata {
            origin_id: NodeId("configured-caller".into()),
            timestamp_us: now_ms * 1000,
            logical_ts: None,
            correlation_id: None,
            ingress_auth: None,
            trace: None,
        },
        source_kit::encode_msgpack(&request)?,
    );
    let reply = if request.is_query_operation() {
        service.handle_query_events(vec![event]).remove(0)
    } else {
        service.handle_command_events(vec![event]).remove(0)
    };
    Ok(source_kit::decode_msgpack(reply.payload_bytes())?)
}
fn main() {
    if let Err(error) = run() {
        eprintln!(
            "{}",
            serde_json::json!({"errorCode":"dataview-inspection-failed", "message":error.to_string()})
        );
        std::process::exit(2);
    }
}
