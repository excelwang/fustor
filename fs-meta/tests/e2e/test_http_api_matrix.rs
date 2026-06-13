#![cfg(target_os = "linux")]

use crate::support::api_client::{ApiResponse, FsMetaApiClient, OperatorSession};
use crate::support::cluster5::Cluster5;
use crate::support::full_demo_roots::{self, FullDemoRoot};
use crate::support::nfs_lab::NfsLab;
use crate::support::oracle::FsTreeOracle;
use crate::support::{reserve_http_addrs, skip_unless_real_nfs_enabled, wait_until};
use fs_meta::{RootSelector, RootSpec};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatrixMode {
    Full,
    QueryBaselineOnly,
    LiveOnlyOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DemoEvidenceEnvironment {
    Full5Node5Nfs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DemoEvidencePolicy {
    QueryBaseline,
    EnvironmentBaseline,
    FullAcceptance,
}

impl DemoEvidencePolicy {
    fn as_str(self) -> &'static str {
        match self {
            Self::QueryBaseline => "query-baseline",
            Self::EnvironmentBaseline => "environment-baseline",
            Self::FullAcceptance => "full-acceptance",
        }
    }
}

impl DemoEvidenceEnvironment {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full5Node5Nfs => "full-5node-5nfs-demo",
        }
    }
}

impl MatrixMode {
    fn app_prefix(self) -> &'static str {
        match self {
            Self::Full => "fs-meta-api-matrix",
            Self::QueryBaselineOnly => "fs-meta-api-matrix-baseline",
            Self::LiveOnlyOnly => "fs-meta-api-matrix-live-only",
        }
    }
}

struct MatrixHarness {
    cluster: Cluster5,
    lab: NfsLab,
    client: FsMetaApiClient,
    candidate_base_urls: Vec<String>,
    facade_count: usize,
    roots: Vec<RootSpec>,
    root_sources: BTreeMap<String, String>,
    uses_full_demo_roots: bool,
}

const MINI_EXPORTS: [&str; 5] = [
    "mini_nfs1",
    "mini_nfs2",
    "mini_nfs3",
    "mini_nfs4",
    "mini_nfs5",
];

const FULL_EXPORTS: [&str; 5] = ["nfs1", "nfs2", "nfs3", "nfs4", "nfs5"];
const ACTIVE_THREE_EXPORTS: [&str; 3] = ["nfs-144", "nfs-145", "nfs-146"];
const ACTIVE_THREE_MOUNTS: [(&str, &str); 3] = [
    ("node-a", "nfs-144"),
    ("node-b", "nfs-145"),
    ("node-c", "nfs-146"),
];
const ACTIVE_THREE_NODES: [&str; 3] = ["node-a", "node-b", "node-c"];
const ACTIVE_THREE_FILES_PER_ROOT_ENV: &str = "FSMETA_ACTIVE_THREE_FILES_PER_ROOT";
const ACTIVE_THREE_DIRS_PER_ROOT_ENV: &str = "FSMETA_ACTIVE_THREE_DIRS_PER_ROOT";
const ACTIVE_THREE_MATERIALIZATION_TIMEOUT: Duration = Duration::from_secs(240);

const FULL_MOUNTS: [(&str, &str); 15] = [
    ("node-a", "nfs1"),
    ("node-b", "nfs1"),
    ("node-c", "nfs1"),
    ("node-b", "nfs2"),
    ("node-c", "nfs2"),
    ("node-d", "nfs2"),
    ("node-c", "nfs3"),
    ("node-d", "nfs3"),
    ("node-e", "nfs3"),
    ("node-d", "nfs4"),
    ("node-e", "nfs4"),
    ("node-a", "nfs4"),
    ("node-e", "nfs5"),
    ("node-a", "nfs5"),
    ("node-b", "nfs5"),
];

const MINI_MOUNTS: [(&str, &str); 15] = [
    ("node-a", "mini_nfs1"),
    ("node-b", "mini_nfs1"),
    ("node-c", "mini_nfs1"),
    ("node-b", "mini_nfs2"),
    ("node-c", "mini_nfs2"),
    ("node-d", "mini_nfs2"),
    ("node-c", "mini_nfs3"),
    ("node-d", "mini_nfs3"),
    ("node-e", "mini_nfs3"),
    ("node-d", "mini_nfs4"),
    ("node-e", "mini_nfs4"),
    ("node-a", "mini_nfs4"),
    ("node-e", "mini_nfs5"),
    ("node-a", "mini_nfs5"),
    ("node-b", "mini_nfs5"),
];

pub fn run() -> Result<(), String> {
    run_mode(MatrixMode::Full)
}

pub fn run_query_baseline_only() -> Result<(), String> {
    run_mode(MatrixMode::QueryBaselineOnly)
}

pub fn run_live_only_rescan_only() -> Result<(), String> {
    run_mode(MatrixMode::LiveOnlyOnly)
}

pub fn run_mini_real_nfs_smoke() -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-matrix-mini] skipped: {reason}");
        return Ok(());
    }

    let harness = build_mini_matrix_harness()?;
    eprintln!(
        "[fs-meta-api-matrix-mini] candidates={:?}",
        harness.candidate_base_urls
    );

    eprintln!("[fs-meta-api-matrix-mini] step=login-matrix");
    run_login_matrix(&harness.client)?;
    eprintln!("[fs-meta-api-matrix-mini] step=operator-login-many");
    let mut session = OperatorSession::login_many(
        harness.candidate_base_urls.clone(),
        "operator",
        "operator123",
    )?;
    eprintln!("[fs-meta-api-matrix-mini] step=initial-rescan");
    wait_for_rescan_accepted(
        &mut session,
        CURRENT_ROOTS_RESCAN_ACCEPTED_TIMEOUT,
        "mini initial source repair rescan accepted",
    )?;

    eprintln!("[fs-meta-api-matrix-mini] step=status-and-grants");
    run_mini_status_and_grants_checks(&harness.client, &mut session, &harness.lab, 1)?;
    eprintln!("[fs-meta-api-matrix-mini] step=management-roots");
    run_mini_roots_management_smoke(&mut session, &harness.lab)?;
    eprintln!("[fs-meta-api-matrix-mini] step=query-and-key-api");
    run_mini_query_and_key_smoke(&mut session, &harness.lab)?;
    Ok(())
}

pub fn run_active_three_external_worker_replay() -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-active3-replay] skipped: {reason}");
        return Ok(());
    }
    std::env::set_var("DATANIX_KEEP_E2E_ARTIFACTS", "1");

    eprintln!("[fs-meta-active3-replay] step=start-lab");
    let mut lab = NfsLab::start_with_exports(ACTIVE_THREE_EXPORTS)?;
    seed_active_three_content(&lab)?;
    eprintln!("[fs-meta-active3-replay] step=start-cluster");
    let cluster = Cluster5::start_with_node_names(&ACTIVE_THREE_NODES)?;
    let app_id = format!("fs-meta-active3-replay-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(1)?;

    eprintln!("[fs-meta-active3-replay] step=announce-active3-resources");
    install_active_three_resources(&cluster, &mut lab, &facade_resource_id, &facade_addrs)?;

    eprintln!("[fs-meta-active3-replay] step=deploy-empty-roots");
    let release = cluster.build_fs_meta_release_for_node_names(
        &app_id,
        &facade_resource_id,
        Vec::new(),
        1,
        true,
        &ACTIVE_THREE_NODES,
    )?;
    cluster.apply_release("node-b", release)?;

    let candidate_base_urls = facade_addrs
        .iter()
        .map(|addr| format!("http://{addr}"))
        .collect::<Vec<_>>();
    let base_url = cluster.wait_http_login_ready(
        &candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(120),
    )?;
    eprintln!("[fs-meta-active3-replay] facade={base_url}");
    let mut session = OperatorSession::login_many(candidate_base_urls, "operator", "operator123")?;

    eprintln!("[fs-meta-active3-replay] step=roots-apply-active3");
    let roots = active_three_roots(&lab);
    let roots_payload = roots_payload(&roots);
    let mut last_apply = None::<ApiResponse>;
    for attempt in 1..=4 {
        let response = session.update_roots_raw(&roots_payload)?;
        eprintln!(
            "[fs-meta-active3-replay] roots_apply_attempt={} status={} body={}",
            attempt, response.status, response.body
        );
        let ok = (200..300).contains(&response.status)
            && response.body.get("roots_count").and_then(Value::as_u64)
                == Some(ACTIVE_THREE_EXPORTS.len() as u64);
        last_apply = Some(response);
        if ok {
            break;
        }
        thread::sleep(Duration::from_secs(5));
    }

    let mut evidence = collect_active_three_evidence(&cluster, &app_id)?;
    eprintln!(
        "[fs-meta-active3-replay] evidence_after_roots_apply={}",
        serde_json::to_string_pretty(&evidence).unwrap_or_else(|_| evidence.to_string())
    );
    let apply_ok = last_apply.as_ref().is_some_and(|response| {
        (200..300).contains(&response.status)
            && response.body.get("roots_count").and_then(Value::as_u64)
                == Some(ACTIVE_THREE_EXPORTS.len() as u64)
    });
    if !apply_ok {
        return Err(format!(
            "active-three roots apply failed before convergence; last_apply={:?}; evidence={}",
            last_apply.as_ref().map(|response| json!({
                "status": response.status,
                "body": response.body,
            })),
            evidence
        ));
    }

    eprintln!("[fs-meta-active3-replay] step=source-repair-rescan");
    wait_for_rescan_accepted(
        &mut session,
        CURRENT_ROOTS_RESCAN_ACCEPTED_TIMEOUT,
        "active-three source repair rescan accepted",
    )?;

    eprintln!("[fs-meta-active3-replay] step=status-poll");
    let mut final_status = Value::Null;
    let expected_owner_by_group = active_three_expected_owner_by_group(&cluster)?;
    wait_until(
        Duration::from_secs(120),
        "active-three status has expected ownership",
        || {
            let status = session.status()?;
            validate_active_three_status_ownership(&status, &expected_owner_by_group)?;
            final_status = status;
            Ok(true)
        },
    )?;
    eprintln!("[fs-meta-active3-replay] step=materialization-poll");
    wait_until(
        ACTIVE_THREE_MATERIALIZATION_TIMEOUT,
        "active-three source/sink materialization has transport evidence",
        || {
            let status = session.status()?;
            validate_active_three_status_ownership(&status, &expected_owner_by_group)?;
            validate_active_three_status_materialization(&status, &expected_owner_by_group)?;
            final_status = status;
            Ok(true)
        },
    )?;
    eprintln!(
        "[fs-meta-active3-replay] final_status={}",
        serde_json::to_string_pretty(&final_status).unwrap_or_else(|_| final_status.to_string())
    );

    evidence = collect_active_three_evidence(&cluster, &app_id)?;
    if let Some(evidence) = evidence.as_object_mut() {
        evidence.insert("status".to_string(), final_status);
    }
    eprintln!(
        "[fs-meta-active3-replay] evidence_after_status={}",
        serde_json::to_string_pretty(&evidence).unwrap_or_else(|_| evidence.to_string())
    );
    reject_active_three_blocker_evidence(&evidence)?;
    Ok(())
}

fn run_mode(mode: MatrixMode) -> Result<(), String> {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-matrix] skipped: {reason}");
        return Ok(());
    }

    let harness = build_matrix_harness(mode.app_prefix())?;
    eprintln!(
        "[fs-meta-api-matrix] candidates={:?}",
        harness.candidate_base_urls
    );

    eprintln!("[fs-meta-api-matrix] step=login-matrix");
    run_login_matrix(&harness.client)?;
    eprintln!("[fs-meta-api-matrix] step=operator-login-many");
    let mut session = OperatorSession::login_many(
        harness.candidate_base_urls.clone(),
        "operator",
        "operator123",
    )?;
    eprintln!("[fs-meta-api-matrix] step=initial-rescan");
    wait_for_rescan_accepted(
        &mut session,
        CURRENT_ROOTS_RESCAN_ACCEPTED_TIMEOUT,
        "initial source repair rescan accepted",
    )?;

    match mode {
        MatrixMode::Full => {
            eprintln!("[fs-meta-api-matrix] step=status-and-grants");
            run_status_and_grants_checks(
                &harness.client,
                &mut session,
                &harness.root_sources,
                harness.facade_count,
                false,
                false,
            )?;
            eprintln!("[fs-meta-api-matrix] step=roots-matrix");
            run_roots_matrix(&mut session, &harness.roots)?;
            eprintln!("[fs-meta-api-matrix] step=query-matrix");
            run_query_matrix(&harness.cluster, &mut session, &harness.lab)?;
            let metrics = session.bound_route_metrics()?;
            for key in [
                "call_timeout_total",
                "correlation_mismatch_total",
                "uncorrelated_reply_total",
                "recv_loop_iterations",
                "pending_calls",
            ] {
                if !metrics.get(key).is_some_and(Value::is_number) {
                    return Err(format!(
                        "bound-route-metrics missing numeric key {key}: {metrics}"
                    ));
                }
            }
        }
        MatrixMode::QueryBaselineOnly => {
            eprintln!("[fs-meta-api-matrix] step=status-and-grants");
            run_status_and_grants_checks(
                &harness.client,
                &mut session,
                &harness.root_sources,
                harness.facade_count,
                false,
                false,
            )?;
            eprintln!("[fs-meta-api-matrix] step=roots-matrix");
            run_roots_matrix(&mut session, &harness.roots)?;
            eprintln!("[fs-meta-api-matrix] step=query-matrix");
            run_full_query_capacity_baseline_phase(&mut session)?;
            let metrics = session.bound_route_metrics()?;
            for key in [
                "call_timeout_total",
                "correlation_mismatch_total",
                "uncorrelated_reply_total",
                "recv_loop_iterations",
                "pending_calls",
            ] {
                if !metrics.get(key).is_some_and(Value::is_number) {
                    return Err(format!(
                        "bound-route-metrics missing numeric key {key}: {metrics}"
                    ));
                }
            }
        }
        MatrixMode::LiveOnlyOnly => {
            eprintln!("[fs-meta-api-matrix] step=status-and-grants");
            run_status_and_grants_checks(
                &harness.client,
                &mut session,
                &harness.root_sources,
                harness.facade_count,
                false,
                false,
            )?;
        }
    }

    if matches!(mode, MatrixMode::LiveOnlyOnly) {
        if harness.uses_full_demo_roots {
            return Err(
                "live-only mutation phase requires the local NFS lab; run with FSMETA_FULL_NFS_ROOTS_FILE unset"
                    .to_string(),
            );
        }
        eprintln!("[fs-meta-api-matrix] step=query-live-only");
        run_query_live_only_rescan_phase(&harness.cluster, &mut session, &harness.lab)?;
    }

    let evidence_policy = demo_evidence_policy_for_mode(mode);
    emit_demo_evidence_result(
        &mut session,
        DemoEvidenceEnvironment::Full5Node5Nfs,
        evidence_policy,
    )?;

    Ok(())
}

fn demo_evidence_policy_for_mode(mode: MatrixMode) -> DemoEvidencePolicy {
    match mode {
        MatrixMode::QueryBaselineOnly => DemoEvidencePolicy::QueryBaseline,
        MatrixMode::Full => DemoEvidencePolicy::EnvironmentBaseline,
        MatrixMode::LiveOnlyOnly => DemoEvidencePolicy::EnvironmentBaseline,
    }
}

fn build_matrix_harness(app_prefix: &str) -> Result<MatrixHarness, String> {
    let mut lab = NfsLab::start_with_exports(FULL_EXPORTS)?;
    let full_demo_roots = full_demo_roots::logical_roots_from_env(&FULL_EXPORTS)?;
    if full_demo_roots.is_none() {
        seed_baseline_content(&lab)?;
    }
    let cluster = Cluster5::start()?;
    let app_id = format!("{app_prefix}-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(1)?;
    if let Some(roots) = full_demo_roots.as_ref() {
        install_full_demo_resources(&cluster, roots, &facade_resource_id, &facade_addrs)?;
    } else {
        install_baseline_resources(&cluster, &mut lab, &facade_resource_id, &facade_addrs)?;
    }

    let roots = full_demo_roots
        .as_ref()
        .map(|roots| roots.iter().map(FullDemoRoot::root_spec).collect())
        .unwrap_or_else(|| baseline_roots(&lab));
    let root_sources: BTreeMap<String, String> = full_demo_roots
        .as_ref()
        .map(|roots| {
            roots
                .iter()
                .map(|root| (root.id.clone(), root.source.clone()))
                .collect()
        })
        .unwrap_or_else(|| {
            FULL_EXPORTS
                .iter()
                .map(|export_name| ((*export_name).to_string(), lab.export_source(export_name)))
                .collect()
        });
    let release =
        cluster.build_fs_meta_release(&app_id, &facade_resource_id, roots.clone(), 1, true)?;
    cluster.apply_release("node-a", release)?;

    let candidate_base_urls = facade_addrs
        .iter()
        .map(|addr| format!("http://{addr}"))
        .collect::<Vec<_>>();
    let base_url = cluster.wait_http_login_ready(
        &candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(120),
    )?;
    let client = FsMetaApiClient::new(base_url)?;

    Ok(MatrixHarness {
        cluster,
        lab,
        client,
        candidate_base_urls,
        facade_count: facade_addrs.len(),
        roots,
        root_sources,
        uses_full_demo_roots: full_demo_roots.is_some(),
    })
}

fn build_mini_matrix_harness() -> Result<MatrixHarness, String> {
    let mut lab = NfsLab::start_with_exports(MINI_EXPORTS)?;
    seed_mini_content(&lab)?;
    let cluster = Cluster5::start()?;
    let app_id = format!("fs-meta-api-matrix-mini-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(1)?;
    install_mini_resources(&cluster, &mut lab, &facade_resource_id, &facade_addrs)?;

    let roots = mini_roots(&lab);
    let release =
        cluster.build_fs_meta_release(&app_id, &facade_resource_id, roots.clone(), 1, true)?;
    cluster.apply_release("node-a", release)?;

    let candidate_base_urls = facade_addrs
        .iter()
        .map(|addr| format!("http://{addr}"))
        .collect::<Vec<_>>();
    let base_url = cluster.wait_http_login_ready(
        &candidate_base_urls,
        "operator",
        "operator123",
        Duration::from_secs(120),
    )?;
    let client = FsMetaApiClient::new(base_url)?;
    let root_sources: BTreeMap<String, String> = MINI_EXPORTS
        .iter()
        .map(|export_name| ((*export_name).to_string(), lab.export_source(export_name)))
        .collect();

    Ok(MatrixHarness {
        cluster,
        lab,
        client,
        candidate_base_urls,
        facade_count: facade_addrs.len(),
        roots,
        root_sources,
        uses_full_demo_roots: false,
    })
}

fn run_query_matrix(
    _cluster: &Cluster5,
    session: &mut OperatorSession,
    _lab: &NfsLab,
) -> Result<(), String> {
    eprintln!("[fs-meta-api-matrix] substep=query-core-readiness-gate");
    assert_trusted_tree_matches_readiness(session, "query core readiness gate")?;
    assert_error(
        session.force_find_raw(&[
            ("path", "/".to_string()),
            ("recursive", "true".to_string()),
            ("read_class", "trusted-materialized".to_string()),
        ])?,
        400,
        "read_class must be fresh on /on-demand-force-find",
    )?;

    eprintln!("[fs-meta-api-matrix] substep=query-api-key-management");
    let keys_before = session.list_query_api_keys()?;
    let created = session.create_query_api_key("full-core-extra-key")?;
    let api_key = created
        .get("api_key")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("query api key response missing api_key: {created}"))?;
    let key_id = created
        .get("key")
        .and_then(|key| key.get("key_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| format!("query api key response missing key.key_id: {created}"))?;
    let keys_after_create = session.list_query_api_keys()?;
    if !query_api_key_list_contains(&keys_after_create, key_id) {
        return Err(format!(
            "created query api key {key_id} not visible: before={keys_before}; after={keys_after_create}"
        ));
    }
    let revoked = session.revoke_query_api_key(key_id)?;
    if revoked.get("revoked").and_then(Value::as_bool) != Some(true) {
        return Err(format!(
            "query api key revoke did not report true: {revoked}"
        ));
    }
    assert_error(
        session.tree_raw_with_query_api_key(api_key, &[("path", "/".to_string())])?,
        401,
        "invalid query api key",
    )?;

    Ok(())
}

fn run_full_query_capacity_baseline_phase(session: &mut OperatorSession) -> Result<(), String> {
    eprintln!("[fs-meta-api-matrix] substep=full-capacity-trusted-materialized-gate");
    wait_for_rescan_accepted(
        session,
        CURRENT_ROOTS_RESCAN_ACCEPTED_TIMEOUT,
        "mini roots source repair rescan accepted",
    )?;
    assert_trusted_tree_matches_readiness(session, "full capacity trusted materialized gate")?;
    assert_error(
        session.force_find_raw(&[
            ("path", "/".to_string()),
            ("recursive", "true".to_string()),
            ("read_class", "trusted-materialized".to_string()),
        ])?,
        400,
        "read_class must be fresh on /on-demand-force-find",
    )?;
    Ok(())
}

fn emit_demo_evidence_result(
    session: &mut OperatorSession,
    environment: DemoEvidenceEnvironment,
    policy: DemoEvidencePolicy,
) -> Result<(), String> {
    let mut last_report = None::<DemoEvidenceReport>;
    wait_until(
        Duration::from_secs(120),
        "full demo evidence reaches accepted state",
        || {
            let status = match session.status() {
                Ok(status) => status,
                Err(err)
                    if policy != DemoEvidencePolicy::FullAcceptance
                        && is_trusted_materialized_status_unavailable(&err) =>
                {
                    last_report = Some(bounded_materialization_unready_demo_evidence(
                        environment,
                        policy,
                        &err,
                    ));
                    return Ok(true);
                }
                Err(err) => return Err(err),
            };
            let report = build_demo_evidence_report(&status, environment)?;
            let accepted = report.accepted_for(policy);
            last_report = Some(report);
            Ok(accepted)
        },
    )
    .map_err(|err| {
        let evidence = last_report
            .as_ref()
            .map(|report| report.evidence.to_string())
            .unwrap_or_else(|| "<none>".to_string());
        format!("{err}: last_demo_evidence={evidence}")
    })?;
    let report = last_report
        .ok_or_else(|| "demo evidence wait completed without status report".to_string())?;
    let mut evidence = report.evidence;
    if let Some(object) = evidence.as_object_mut() {
        object.insert("policy".to_string(), json!(policy.as_str()));
        object.insert("policy_result".to_string(), json!("accepted"));
        object.insert("policy_accepted".to_string(), json!(true));
    }
    eprintln!("[fs-meta-api-matrix] demo_evidence_result={evidence}");
    Ok(())
}

fn is_trusted_materialized_status_unavailable(err: &str) -> bool {
    err.contains("trusted-materialized reads remain unavailable")
        || err.contains("package-local materialized observation evidence is trusted")
}

fn bounded_materialization_unready_demo_evidence(
    environment: DemoEvidenceEnvironment,
    policy: DemoEvidencePolicy,
    err: &str,
) -> DemoEvidenceReport {
    DemoEvidenceReport {
        evidence: json!({
            "environment": environment.as_str(),
            "status": "bounded_degraded",
            "reason": "sink_materialization_unready",
            "status_error": err,
            "policy": policy.as_str(),
        }),
        unacceptable_reasons: vec!["sink_materialization_unready".to_string()],
    }
}

struct DemoEvidenceReport {
    evidence: Value,
    unacceptable_reasons: Vec<String>,
}

impl DemoEvidenceReport {
    fn accepted_for(&self, policy: DemoEvidencePolicy) -> bool {
        match policy {
            DemoEvidencePolicy::FullAcceptance => self.unacceptable_reasons.is_empty(),
            DemoEvidencePolicy::EnvironmentBaseline => self
                .unacceptable_reasons
                .iter()
                .all(|reason| reason == "sink_materialization_unready"),
            DemoEvidencePolicy::QueryBaseline => true,
        }
    }
}

fn build_demo_evidence_report(
    status: &Value,
    environment: DemoEvidenceEnvironment,
) -> Result<DemoEvidenceReport, String> {
    let source = status
        .get("source")
        .ok_or_else(|| format!("status missing source for demo evidence: {status}"))?;
    let roots = source
        .get("logical_roots")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            format!("status.source missing logical_roots for demo evidence: {status}")
        })?;
    let degraded_roots = source
        .get("degraded_roots")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let root_ids = roots
        .iter()
        .filter_map(|root| root.get("root_id").and_then(Value::as_str))
        .collect::<Vec<_>>();
    let source_debug = source.get("debug").cloned().unwrap_or_else(|| json!({}));
    let artifact_verified = status.get("runtime_artifact").is_some();
    let mut unacceptable_reasons = Vec::<String>::new();
    if !artifact_verified {
        unacceptable_reasons.push("runtime_artifact_missing".to_string());
    }
    if roots.len() != FULL_EXPORTS.len() {
        unacceptable_reasons.push(format!("full_demo_root_count_{}", roots.len()));
    }
    if !degraded_roots.is_empty() {
        unacceptable_reasons.push("source_degraded_roots_present".to_string());
    }

    let mut coverage_modes = BTreeMap::<String, usize>::new();
    for root in roots {
        let mode = root
            .get("coverage_mode")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        *coverage_modes.entry(mode.to_string()).or_insert(0) += 1;
        if mode == "watch_degraded" {
            unacceptable_reasons.push("watch_degraded".to_string());
        }
        if mode == "audit_without_file_metadata" {
            unacceptable_reasons.push("audit_without_file_metadata".to_string());
        }
        let file_metadata_coverage = root
            .get("coverage_capabilities")
            .and_then(|value| value.get("file_metadata_coverage"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !file_metadata_coverage {
            unacceptable_reasons.push("file_metadata_coverage_missing".to_string());
        }
    }

    let mut sink_unready = Vec::<String>::new();
    let sink_groups = status
        .get("sink")
        .and_then(|sink| sink.get("groups"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("status.sink.groups missing for demo evidence: {status}"))?;
    if sink_groups.len() != FULL_EXPORTS.len() {
        unacceptable_reasons.push(format!("full_demo_sink_group_count_{}", sink_groups.len()));
    }
    for group in sink_groups {
        let group_id = group
            .get("group_id")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        let readiness = group
            .get("materialization_readiness")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        let initial_audit_completed = group
            .get("initial_audit_completed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if readiness != "ready" || !initial_audit_completed {
            sink_unready.push(format!("{group_id}:{readiness}"));
        }
    }
    if !sink_unready.is_empty() {
        unacceptable_reasons.push("sink_materialization_unready".to_string());
    }

    unacceptable_reasons.sort();
    unacceptable_reasons.dedup();
    let result = if unacceptable_reasons.is_empty() {
        "passed"
    } else {
        "failed"
    };
    let evidence = json!({
        "environment": environment.as_str(),
        "artifact_verified": artifact_verified,
        "expected_root_count": FULL_EXPORTS.len(),
        "root_count": roots.len(),
        "root_ids": root_ids,
        "source_debug": source_debug,
        "coverage_modes": coverage_modes,
        "degraded_roots": degraded_roots,
        "expected_sink_group_count": FULL_EXPORTS.len(),
        "sink_group_count": sink_groups.len(),
        "sink_unready": sink_unready,
        "unacceptable_reasons": unacceptable_reasons.clone(),
        "result": result,
    });
    Ok(DemoEvidenceReport {
        evidence,
        unacceptable_reasons,
    })
}

fn run_query_live_only_rescan_phase(
    _cluster: &Cluster5,
    session: &mut OperatorSession,
    lab: &NfsLab,
) -> Result<(), String> {
    let grants = session.runtime_grants()?;
    let all_mounts = group_mount_pairs_for_roots(
        &grants,
        &[
            ("nfs1", &lab.export_source("nfs1")),
            ("nfs2", &lab.export_source("nfs2")),
            ("nfs3", &lab.export_source("nfs3")),
        ],
    )?;

    let live_probe_roots = json!([
        root_payload_flags("nfs1", &lab.export_source("nfs1"), "/", false, true),
        root_payload_flags("nfs2", &lab.export_source("nfs2"), "/", true, true),
        root_payload_flags("nfs3", &lab.export_source("nfs3"), "/", true, true),
    ]);
    session.update_roots(&live_probe_roots)?;
    wait_until(
        Duration::from_secs(30),
        "nfs1 live-probe roots applied",
        || {
            let current = session.monitoring_roots()?;
            Ok(current.to_string().contains("\"id\":\"nfs1\"")
                && current.to_string().contains("\"watch\":false"))
        },
    )?;

    lab.mkdir("nfs1", "live-only")?;
    lab.write_file("nfs1", "live-only/fresh.txt", "live-before-materialized\n")?;

    eprintln!("[fs-meta-api-matrix] substep=query-live-force-find");
    let live_force_find = session.force_find(&[
        ("path", "/live-only".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let expected_live_force_find = FsTreeOracle::grouped_force_find_response(
        &all_mounts,
        "/live-only",
        true,
        None,
        1_000,
        "group-key",
        64,
    )?;
    assert_json_eq(
        "live force-find observes fresh path before materialized convergence",
        &live_force_find,
        &expected_live_force_find,
    )?;

    wait_for_rescan_accepted(
        session,
        CURRENT_ROOTS_RESCAN_ACCEPTED_TIMEOUT,
        "mini roots source repair rescan accepted",
    )?;
    match session.status() {
        Ok(post_rescan_status) => validate_status_monitoring_shape(&post_rescan_status, true)?,
        Err(err) if is_trusted_materialized_status_unavailable(&err) => {
            eprintln!(
                "[fs-meta-api-matrix] live-only post-rescan status remains bounded-degraded: {err}"
            );
        }
        Err(err) => return Err(err),
    }

    session.update_roots(&roots_payload(&baseline_roots(lab)))?;
    session.rescan()?;
    wait_until(
        Duration::from_secs(30),
        "restore baseline watch=true roots",
        || {
            let current = session.monitoring_roots()?;
            Ok(current.to_string().contains("\"id\":\"nfs1\"")
                && current.to_string().contains("\"watch\":true"))
        },
    )?;

    Ok(())
}

fn run_login_matrix(client: &FsMetaApiClient) -> Result<(), String> {
    assert_error(
        client.login_raw("", "")?,
        400,
        "username/password must not be empty",
    )?;
    assert_error(client.login_raw("operator", "wrong-pass")?, 401, "invalid")?;
    let login = client.login_raw("operator", "operator123")?;
    assert_status(login.status, 200, "valid login")?;
    if login.body.get("token").and_then(Value::as_str).is_none() {
        return Err(format!("login response missing token: {}", login.body));
    }
    Ok(())
}

fn run_status_and_grants_checks(
    client: &FsMetaApiClient,
    session: &mut OperatorSession,
    root_sources: &BTreeMap<String, String>,
    facade_count: usize,
    require_sink_groups: bool,
    require_sink_live_nodes: bool,
) -> Result<(), String> {
    assert_error(
        client.get_json_raw("/status", "bad-token")?,
        401,
        "invalid session token",
    )?;

    eprintln!("[fs-meta-api-matrix] substep=status");
    wait_until(
        Duration::from_secs(120),
        "status sink groups become available",
        || {
            let status = session.status()?;
            let source = match status.get("source") {
                Some(source) => source,
                None => return Ok(false),
            };
            let sink = match status.get("sink") {
                Some(sink) => sink,
                None => return Ok(false),
            };
            if source
                .get("grants_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                < FULL_EXPORTS.len() as u64
            {
                return Ok(false);
            }
            if require_sink_live_nodes
                && sink.get("live_nodes").and_then(Value::as_u64).unwrap_or(0) == 0
            {
                return Ok(false);
            }
            let Some(groups) = sink.get("groups").and_then(Value::as_array) else {
                return Ok(!require_sink_groups);
            };
            if groups.is_empty() && require_sink_groups {
                return Ok(false);
            }
            Ok(validate_status_monitoring_shape(&status, require_sink_groups).is_ok())
        },
    )
    .map_err(|err| {
        let status = session
            .status()
            .map(|status| status.to_string())
            .unwrap_or_else(|status_err| format!("<status unavailable: {status_err}>"));
        format!("{err}: last_status={status}")
    })?;
    let metrics_before = session.bound_route_metrics()?;
    let status = session.status()?;
    let metrics_after = session.bound_route_metrics()?;
    let source = status
        .get("source")
        .ok_or_else(|| format!("status missing source: {status}"))?;
    let sink = status
        .get("sink")
        .ok_or_else(|| format!("status missing sink: {status}"))?;
    if source
        .get("grants_count")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        < FULL_EXPORTS.len() as u64
    {
        return Err(format!(
            "expected at least {} grants in status: {status}",
            FULL_EXPORTS.len()
        ));
    }
    if require_sink_live_nodes && sink.get("live_nodes").and_then(Value::as_u64).unwrap_or(0) == 0 {
        return Err(format!("expected live sink nodes in status: {status}"));
    }
    validate_status_monitoring_shape(&status, require_sink_groups)?;
    let timeouts_before = metrics_before
        .get("call_timeout_total")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            format!("bound-route-metrics missing call_timeout_total: {metrics_before}")
        })?;
    let timeouts_after = metrics_after
        .get("call_timeout_total")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            format!("bound-route-metrics missing call_timeout_total: {metrics_after}")
        })?;
    if timeouts_after != timeouts_before {
        return Err(format!(
            "status should not increase bound-route call_timeout_total: before={timeouts_before} after={timeouts_after} status={status} metrics_before={metrics_before} metrics_after={metrics_after}"
        ));
    }

    eprintln!("[fs-meta-api-matrix] substep=runtime-grants");
    let grants = session.runtime_grants()?;
    let rows = grants
        .get("grants")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("runtime grants missing array: {grants}"))?;
    let nfs_rows = rows
        .iter()
        .filter(|row| row.get("fs_type").and_then(Value::as_str) == Some("nfs"))
        .count();
    let listener_rows = rows
        .iter()
        .filter(|row| row.get("fs_type").and_then(Value::as_str) == Some("tcp_listener"))
        .count();
    let expected_total = FULL_MOUNTS.len() + facade_count;
    if nfs_rows != FULL_MOUNTS.len()
        || listener_rows != facade_count
        || rows.len() != expected_total
    {
        return Err(format!(
            "expected {expected_total} runtime grants rows ({} nfs + {facade_count} tcp_listener), got {} total / {} nfs / {} listeners: {grants}",
            FULL_MOUNTS.len(),
            rows.len(),
            nfs_rows,
            listener_rows
        ));
    }
    for export_name in FULL_EXPORTS {
        let expected_source = root_sources
            .get(export_name)
            .ok_or_else(|| format!("missing expected source for export {export_name}"))?;
        if !rows.iter().any(|row| {
            row.get("fs_source").and_then(Value::as_str) == Some(expected_source.as_str())
        }) {
            return Err(format!(
                "runtime grants missing export {export_name}: {grants}"
            ));
        }
    }
    Ok(())
}

fn run_mini_status_and_grants_checks(
    client: &FsMetaApiClient,
    session: &mut OperatorSession,
    lab: &NfsLab,
    facade_count: usize,
) -> Result<(), String> {
    assert_error(
        client.get_json_raw("/status", "bad-token")?,
        401,
        "invalid session token",
    )?;

    wait_until(
        Duration::from_secs(120),
        "mini status evidence becomes ready",
        || {
            let status = session.status()?;
            let source_grants = status
                .get("source")
                .and_then(|source| source.get("grants_count"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let sink_groups = status
                .get("sink")
                .and_then(|sink| sink.get("groups"))
                .and_then(Value::as_array)
                .map(|groups| groups.len())
                .unwrap_or(0);
            if source_grants < MINI_EXPORTS.len() as u64 || sink_groups < MINI_EXPORTS.len() {
                return Err(format!(
                "source_grants={source_grants} expected_at_least={} sink_groups={sink_groups} expected_at_least={} status={status}",
                MINI_EXPORTS.len(),
                MINI_EXPORTS.len()
            ));
            }
            validate_status_monitoring_shape(&status, true)?;
            validate_status_top_level_evidence(&status)?;
            Ok(true)
        },
    )?;

    let grants = session.runtime_grants()?;
    let rows = grants
        .get("grants")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("runtime grants missing array: {grants}"))?;
    let nfs_rows = rows
        .iter()
        .filter(|row| row.get("fs_type").and_then(Value::as_str) == Some("nfs"))
        .count();
    let listener_rows = rows
        .iter()
        .filter(|row| row.get("fs_type").and_then(Value::as_str) == Some("tcp_listener"))
        .count();
    let expected_total = MINI_MOUNTS.len() + facade_count;
    if nfs_rows != MINI_MOUNTS.len()
        || listener_rows != facade_count
        || rows.len() != expected_total
    {
        return Err(format!(
            "expected {expected_total} runtime grants rows ({} nfs + {facade_count} tcp_listener), got {} total / {} nfs / {} listeners: {grants}",
            MINI_MOUNTS.len(),
            rows.len(),
            nfs_rows,
            listener_rows
        ));
    }
    for export_name in MINI_EXPORTS {
        if !rows.iter().any(|row| {
            row.get("fs_source").and_then(Value::as_str)
                == Some(lab.export_source(export_name).as_str())
        }) {
            return Err(format!(
                "runtime grants missing mini export {export_name}: {grants}"
            ));
        }
    }
    Ok(())
}

fn validate_status_top_level_evidence(status: &Value) -> Result<(), String> {
    let runtime_artifact = status
        .get("runtime_artifact")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("status.runtime_artifact missing object: {status}"))?;
    if !runtime_artifact
        .get("available")
        .is_some_and(Value::is_boolean)
    {
        return Err(format!(
            "status.runtime_artifact.available missing boolean value: {status}"
        ));
    }
    let authority_epoch = status
        .get("authority_epoch")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("status.authority_epoch missing object: {status}"))?;
    for key in [
        "roots_signature",
        "grants_signature",
        "sink_materialization_generation",
        "facade_runtime_generation",
    ] {
        if !authority_epoch.get(key).is_some_and(Value::is_string) {
            return Err(format!(
                "status.authority_epoch.{key} missing string value: {status}"
            ));
        }
    }
    let readiness_planes = status
        .get("readiness_planes")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("status.readiness_planes missing object: {status}"))?;
    for key in [
        "api_facade_liveness",
        "management_write_readiness",
        "source_repair_readiness",
        "trusted_observation_readiness",
    ] {
        if !readiness_planes.get(key).is_some_and(Value::is_boolean) {
            return Err(format!(
                "status.readiness_planes.{key} missing boolean value: {status}"
            ));
        }
    }
    if readiness_planes
        .get("api_facade_liveness")
        .and_then(Value::as_bool)
        != Some(true)
        || readiness_planes
            .get("management_write_readiness")
            .and_then(Value::as_bool)
            != Some(true)
        || readiness_planes
            .get("source_repair_readiness")
            .and_then(Value::as_bool)
            != Some(true)
        || readiness_planes
            .get("trusted_observation_readiness")
            .and_then(Value::as_bool)
            != Some(true)
    {
        return Err(format!(
            "mini smoke requires all readiness planes ready: {status}"
        ));
    }
    if let Some(repair_lanes) = status.get("repair_lanes").and_then(Value::as_array) {
        for lane in repair_lanes {
            let state = lane
                .get("state")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("status.repair_lanes[].state missing string: {status}"))?;
            if !matches!(
                state,
                "idle"
                    | "scheduled"
                    | "inflight"
                    | "blocked"
                    | "completed"
                    | "failed"
                    | "timed-out"
                    | "skipped"
            ) {
                return Err(format!(
                    "status.repair_lanes[].state must use RecoveryLaneState, got {state}: {status}"
                ));
            }
        }
    }
    Ok(())
}

fn validate_status_monitoring_shape(
    status: &Value,
    require_sink_groups: bool,
) -> Result<(), String> {
    let source = status
        .get("source")
        .ok_or_else(|| format!("status missing source: {status}"))?;
    validate_status_source_shape(source, status)?;
    let sink = status
        .get("sink")
        .ok_or_else(|| format!("status missing sink: {status}"))?;
    validate_status_sink_shape(sink, status, require_sink_groups)?;
    let facade = status
        .get("facade")
        .ok_or_else(|| format!("status missing facade: {status}"))?;
    validate_status_facade_shape(facade, status)?;
    Ok(())
}

fn validate_status_source_shape(source: &Value, status: &Value) -> Result<(), String> {
    if !source.get("grants_count").is_some_and(Value::is_u64) {
        return Err(format!(
            "status.source.grants_count missing numeric value: {status}"
        ));
    }
    if !source.get("roots_count").is_some_and(Value::is_u64) {
        return Err(format!(
            "status.source.roots_count missing numeric value: {status}"
        ));
    }
    let logical_roots = source
        .get("logical_roots")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("status.source.logical_roots missing array: {status}"))?;
    if logical_roots.is_empty() {
        return Err(format!(
            "status.source.logical_roots unexpectedly empty: {status}"
        ));
    }
    for root in logical_roots {
        let group_state = root
            .get("service_state")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!("status.source.logical_roots[].service_state missing: {status}")
            })?;
        if !matches!(
            group_state,
            "not-selected"
                | "selected-pending"
                | "serving-trusted"
                | "serving-degraded"
                | "retiring"
                | "retired"
        ) {
            return Err(format!(
                "status.source.logical_roots has invalid service_state={group_state}: {root}"
            ));
        }
        if !root.get("root_id").is_some_and(Value::is_string) {
            return Err(format!(
                "status.source.logical_roots[].root_id missing string value: {root}"
            ));
        }
        if !root.get("matched_grants").is_some_and(Value::is_u64) {
            return Err(format!(
                "status.source.logical_roots[].matched_grants missing numeric value: {root}"
            ));
        }
        if !root.get("active_members").is_some_and(Value::is_u64) {
            return Err(format!(
                "status.source.logical_roots[].active_members missing numeric value: {root}"
            ));
        }
        validate_status_coverage_mode(root.get("coverage_mode"), root)?;
    }

    let concrete_roots = source
        .get("concrete_roots")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("status.source.concrete_roots missing array: {status}"))?;
    if concrete_roots.is_empty() {
        return Err(format!(
            "status.source.concrete_roots unexpectedly empty: {status}"
        ));
    }
    for root in concrete_roots {
        if !root.get("root_key").is_some_and(Value::is_string) {
            return Err(format!(
                "status.source.concrete_roots[].root_key missing string value: {root}"
            ));
        }
        if !root.get("logical_root_id").is_some_and(Value::is_string) {
            return Err(format!(
                "status.source.concrete_roots[].logical_root_id missing string value: {root}"
            ));
        }
        if !root.get("object_ref").is_some_and(Value::is_string) {
            return Err(format!(
                "status.source.concrete_roots[].object_ref missing string value: {root}"
            ));
        }
        let participation_state = root
            .get("participation_state")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!(
                    "status.source.concrete_roots[].participation_state missing string value: {root}"
                )
            })?;
        if !matches!(
            participation_state,
            "absent" | "joining" | "serving" | "degraded" | "retiring" | "retired"
        ) {
            return Err(format!(
                "status.source.concrete_roots has invalid participation_state={participation_state}: {root}"
            ));
        }
        validate_status_coverage_mode(root.get("coverage_mode"), root)?;
        for key in [
            "watch_enabled",
            "scan_enabled",
            "is_group_primary",
            "active",
            "overflow_pending",
            "rescan_pending",
        ] {
            if !root.get(key).is_some_and(Value::is_boolean) {
                return Err(format!(
                    "status.source.concrete_roots[].{key} missing boolean value: {root}"
                ));
            }
        }
        for key in ["watch_lru_capacity", "audit_interval_ms", "overflow_count"] {
            if !root.get(key).is_some_and(Value::is_u64) {
                return Err(format!(
                    "status.source.concrete_roots[].{key} missing numeric value: {root}"
                ));
            }
        }
        for key in ["last_rescan_reason", "last_error"] {
            if !root
                .get(key)
                .is_some_and(|value| value.is_null() || value.is_string())
            {
                return Err(format!(
                    "status.source.concrete_roots[].{key} must be null|string: {root}"
                ));
            }
        }
        for key in [
            "last_audit_started_at_us",
            "last_audit_completed_at_us",
            "last_audit_duration_ms",
        ] {
            if !root
                .get(key)
                .is_some_and(|value| value.is_null() || value.is_u64())
            {
                return Err(format!(
                    "status.source.concrete_roots[].{key} must be null|u64: {root}"
                ));
            }
        }
    }
    Ok(())
}

fn validate_status_sink_shape(
    sink: &Value,
    status: &Value,
    require_sink_groups: bool,
) -> Result<(), String> {
    for key in [
        "live_nodes",
        "tombstoned_count",
        "attested_count",
        "suspect_count",
        "blind_spot_count",
        "shadow_time_us",
        "estimated_heap_bytes",
    ] {
        if !sink.get(key).is_some_and(Value::is_u64) {
            return Err(format!("status.sink.{key} missing numeric value: {status}"));
        }
    }
    if !sink
        .get("primary_host_ref_by_group")
        .is_some_and(Value::is_object)
    {
        return Err(format!(
            "status.sink.primary_host_ref_by_group missing object value: {status}"
        ));
    }
    let groups = sink
        .get("groups")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("status.sink.groups missing array: {status}"))?;
    if require_sink_groups && groups.is_empty() {
        return Err(format!("status.sink.groups unexpectedly empty: {status}"));
    }
    for group in groups {
        if !group.get("group_id").is_some_and(Value::is_string) {
            return Err(format!(
                "status.sink.groups[].group_id missing string value: {group}"
            ));
        }
        let service_state = group
            .get("service_state")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!("status.sink.groups[].service_state missing string value: {group}")
            })?;
        if !matches!(
            service_state,
            "not-selected"
                | "selected-pending"
                | "serving-trusted"
                | "serving-degraded"
                | "retiring"
                | "retired"
        ) {
            return Err(format!(
                "status.sink.groups has invalid service_state={service_state}: {group}"
            ));
        }
        if !group
            .get("primary_object_ref")
            .is_some_and(Value::is_string)
        {
            return Err(format!(
                "status.sink.groups[].primary_object_ref missing string value: {group}"
            ));
        }
        for key in [
            "total_nodes",
            "live_nodes",
            "tombstoned_count",
            "attested_count",
            "suspect_count",
            "blind_spot_count",
            "shadow_time_us",
            "shadow_lag_us",
            "estimated_heap_bytes",
        ] {
            if !group.get(key).is_some_and(Value::is_u64) {
                return Err(format!(
                    "status.sink.groups[].{key} missing numeric value: {group}"
                ));
            }
        }
        for key in [
            "overflow_pending_materialization",
            "initial_audit_completed",
        ] {
            if !group.get(key).is_some_and(Value::is_boolean) {
                return Err(format!(
                    "status.sink.groups[].{key} missing boolean value: {group}"
                ));
            }
        }
        let readiness = group
            .get("materialization_readiness")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!(
                    "status.sink.groups[].materialization_readiness missing string value: {group}"
                )
            })?;
        if !matches!(
            readiness,
            "pending-materialization" | "waiting-for-materialized-root" | "ready"
        ) {
            return Err(format!(
                "status.sink.groups has invalid materialization_readiness={readiness}: {group}"
            ));
        }
        let initial_audit_completed = group
            .get("initial_audit_completed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if initial_audit_completed != (readiness == "ready") {
            return Err(format!(
                "status.sink.groups initial_audit_completed/readiness mismatch: readiness={readiness} group={group}"
            ));
        }
    }
    Ok(())
}

fn validate_status_facade_shape(facade: &Value, status: &Value) -> Result<(), String> {
    let state = facade
        .get("state")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("status.facade.state missing string value: {status}"))?;
    if !matches!(state, "unavailable" | "pending" | "serving" | "degraded") {
        return Err(format!("status.facade has invalid state={state}: {status}"));
    }
    if let Some(pending) = facade.get("pending") {
        if !pending.is_object() {
            return Err(format!("status.facade.pending must be object: {status}"));
        }
        if !pending.get("route_key").is_some_and(Value::is_string) {
            return Err(format!(
                "status.facade.pending.route_key missing string value: {pending}"
            ));
        }
        if !pending.get("generation").is_some_and(Value::is_u64) {
            return Err(format!(
                "status.facade.pending.generation missing numeric value: {pending}"
            ));
        }
        if !pending
            .get("resource_ids")
            .and_then(Value::as_array)
            .is_some_and(|items| items.iter().all(Value::is_string))
        {
            return Err(format!(
                "status.facade.pending.resource_ids missing string array: {pending}"
            ));
        }
        for key in ["runtime_managed", "runtime_exposure_confirmed"] {
            if !pending.get(key).is_some_and(Value::is_boolean) {
                return Err(format!(
                    "status.facade.pending.{key} missing boolean value: {pending}"
                ));
            }
        }
        let reason = pending
            .get("reason")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!("status.facade.pending.reason missing string value: {pending}")
            })?;
        if !matches!(
            reason,
            "awaiting_runtime_exposure"
                | "awaiting_observation_eligibility"
                | "retrying_after_error"
        ) {
            return Err(format!(
                "status.facade.pending has invalid reason={reason}: {pending}"
            ));
        }
        for key in ["retry_attempts", "pending_since_us"] {
            if !pending.get(key).is_some_and(Value::is_u64) {
                return Err(format!(
                    "status.facade.pending.{key} missing numeric value: {pending}"
                ));
            }
        }
        if !pending
            .get("last_error")
            .is_none_or(|value| value.is_string())
        {
            return Err(format!(
                "status.facade.pending.last_error must be omitted|string: {pending}"
            ));
        }
        for key in [
            "last_attempt_at_us",
            "last_error_at_us",
            "retry_backoff_ms",
            "next_retry_at_us",
        ] {
            if !pending.get(key).is_none_or(Value::is_u64) {
                return Err(format!(
                    "status.facade.pending.{key} must be omitted|u64: {pending}"
                ));
            }
        }
    }
    Ok(())
}

fn validate_status_coverage_mode(value: Option<&Value>, root: &Value) -> Result<(), String> {
    let coverage_mode = value
        .and_then(Value::as_str)
        .ok_or_else(|| format!("status source root missing string coverage_mode: {root}"))?;
    if !matches!(
        coverage_mode,
        "realtime_hotset_plus_audit"
            | "audit_only"
            | "audit_with_metadata"
            | "audit_without_file_metadata"
            | "watch_degraded"
    ) {
        return Err(format!(
            "status source root has invalid coverage_mode={coverage_mode}: {root}"
        ));
    }
    validate_status_coverage_capabilities(root.get("coverage_capabilities"), root)?;
    Ok(())
}

fn validate_status_coverage_capabilities(
    value: Option<&Value>,
    root: &Value,
) -> Result<(), String> {
    let capabilities = value
        .and_then(Value::as_object)
        .ok_or_else(|| format!("status source root missing coverage_capabilities: {root}"))?;
    for key in [
        "exists_coverage",
        "file_count_coverage",
        "file_metadata_coverage",
        "mtime_size_coverage",
        "watch_freshness_coverage",
    ] {
        if !capabilities.get(key).is_some_and(Value::is_boolean) {
            return Err(format!(
                "status source root coverage_capabilities.{key} missing boolean value: {root}"
            ));
        }
    }
    Ok(())
}

fn run_roots_matrix(
    session: &mut OperatorSession,
    baseline_roots: &[RootSpec],
) -> Result<(), String> {
    eprintln!("[fs-meta-api-matrix] substep=monitoring-roots-current");
    let current = session.monitoring_roots()?;
    let current_rows = current
        .get("roots")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("monitoring_roots missing roots array: {current}"))?;
    if current_rows.len() != FULL_EXPORTS.len() {
        return Err(format!(
            "expected {} roots from full demo release doc, got {current}",
            FULL_EXPORTS.len()
        ));
    }

    let empty_preview = session.preview_roots_raw(&json!([]))?;
    assert_status(empty_preview.status, 200, "empty roots preview")?;
    if empty_preview
        .body
        .get("preview")
        .and_then(Value::as_array)
        .map(|items| !items.is_empty())
        .unwrap_or(true)
    {
        return Err(format!(
            "empty roots preview should return no preview rows: {}",
            empty_preview.body
        ));
    }

    let roots = baseline_roots.to_vec();
    let selector = selector_payload(
        roots
            .first()
            .ok_or_else(|| "baseline roots must not be empty".to_string())?,
    );
    let second_selector = selector_payload(
        roots
            .get(1)
            .ok_or_else(|| "baseline roots must contain at least two roots".to_string())?,
    );
    let preview = session.preview_roots_raw(&roots_payload(&roots))?;
    assert_status(preview.status, 200, "baseline roots preview")?;
    if preview
        .body
        .get("preview")
        .and_then(Value::as_array)
        .map(|items| items.len())
        != Some(FULL_EXPORTS.len())
    {
        return Err(format!(
            "expected {} preview rows: {}",
            FULL_EXPORTS.len(),
            preview.body
        ));
    }
    if preview
        .body
        .get("unmatched_roots")
        .and_then(Value::as_array)
        .map(|items| !items.is_empty())
        .unwrap_or(true)
    {
        return Err(format!(
            "baseline preview unexpectedly has unmatched roots: {}",
            preview.body
        ));
    }

    assert_error(
        session.preview_roots_raw(&json!([{
            "id": "",
            "selector": selector.clone(),
            "subpath_scope": "/",
            "watch": true,
            "scan": true
        }]))?,
        400,
        "roots[].id must not be empty",
    )?;
    assert_error(
        session.preview_roots_raw(&json!([{
            "id": "bad-subpath",
            "selector": selector.clone(),
            "subpath_scope": "relative",
            "watch": true,
            "scan": true
        }]))?,
        400,
        "subpath_scope",
    )?;
    assert_error(
        session.preview_roots_raw(&json!([{
            "id": "missing-selector",
            "selector": {},
            "subpath_scope": "/",
            "watch": true,
            "scan": true
        }]))?,
        400,
        "selector",
    )?;
    assert_error(
        session.preview_roots_raw(&json!([{
            "id": "no-watch-scan",
            "selector": selector.clone(),
            "subpath_scope": "/",
            "watch": false,
            "scan": false
        }]))?,
        400,
        "watch",
    )?;
    assert_error(
        session.preview_roots_raw(&json!([{
            "id": "legacy-path",
            "path": "/legacy",
            "selector": selector.clone(),
            "subpath_scope": "/",
            "watch": true,
            "scan": true
        }]))?,
        400,
        "roots[].path is forbidden",
    )?;
    assert_error(
        session.preview_roots_raw(&json!([{
            "id": "legacy-source-locator",
            "source_locator": "legacy://nfs1",
            "selector": selector.clone(),
            "subpath_scope": "/",
            "watch": true,
            "scan": true
        }]))?,
        400,
        "roots[].source_locator is forbidden",
    )?;
    assert_error(
        session.update_roots_raw(&json!([
            {
                "id": "dup",
                "selector": selector.clone(),
                "subpath_scope": "/",
                "watch": true,
                "scan": true
            },
            {
                "id": "dup",
                "selector": second_selector,
                "subpath_scope": "/",
                "watch": true,
                "scan": true
            }
        ]))?,
        400,
        "duplicate",
    )?;

    eprintln!("[fs-meta-api-matrix] substep=single-root-apply");
    let single_root = vec![roots[0].clone()];
    let put = session.update_roots_raw(&roots_payload(&single_root))?;
    assert_status(put.status, 200, "single-root apply")?;
    if put.body.get("roots_count").and_then(Value::as_u64) != Some(1) {
        return Err(format!("single-root apply did not converge: {}", put.body));
    }
    wait_for_rescan_accepted(
        session,
        CURRENT_ROOTS_RESCAN_ACCEPTED_TIMEOUT,
        "single-root source repair rescan accepted",
    )?;
    wait_until(Duration::from_secs(30), "single root visible", || {
        let roots = session.monitoring_roots()?;
        Ok(roots
            .get("roots")
            .and_then(Value::as_array)
            .map(|items| {
                items.len() == 1 && items[0].get("id").and_then(Value::as_str) == Some("nfs1")
            })
            .unwrap_or(false))
    })?;

    eprintln!("[fs-meta-api-matrix] substep=restore-roots");
    let restore = session.update_roots_raw(&roots_payload(&roots))?;
    assert_response_status(&restore, 200, "restore roots")?;
    wait_for_rescan_accepted(
        session,
        CURRENT_ROOTS_RESCAN_ACCEPTED_TIMEOUT,
        "restored-roots source repair rescan accepted",
    )?;
    wait_until(Duration::from_secs(30), "restore baseline roots", || {
        let current_roots = session.monitoring_roots()?;
        Ok(current_roots
            .get("roots")
            .and_then(Value::as_array)
            .map(|items| items.len() == roots.len())
            .unwrap_or(false))
    })?;
    Ok(())
}

const CURRENT_ROOTS_RESCAN_ACCEPTED_TIMEOUT: Duration = Duration::from_secs(180);
fn wait_for_rescan_accepted(
    session: &mut OperatorSession,
    timeout: Duration,
    label: &str,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last_retryable_err = Some("no retryable source-repair response observed".to_string());
    loop {
        if Instant::now() > deadline {
            let last_retryable_err = last_retryable_err
                .expect("rescan retry timeout message should always be initialized");
            return Err(format!("timeout waiting for {label}: {last_retryable_err}"));
        }
        match session.rescan() {
            Ok(_) => return Ok(()),
            Err(err) if is_retryable_source_repair_not_ready(&err) => {
                last_retryable_err = Some(err);
            }
            Err(err) => return Err(err),
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn is_retryable_source_repair_not_ready(err: &str) -> bool {
    err.contains("http 503 failed")
        && (err.contains("manual rescan current roots runtime-scope readiness failed")
            || err.contains("manual rescan scoped source route pending")
            || err.contains("manual rescan source-status target proof incomplete")
            || err.contains("manual rescan source repair readiness pending")
            || err.contains("manual rescan response budget exhausted"))
}

#[test]
fn source_repair_retry_classifier_treats_target_proof_as_transient_readiness() {
    let err = "http 503 failed: {\"error\":\"manual rescan source-status target proof incomplete: expected_roots={\\\"nfs1\\\"}\"}";
    assert!(is_retryable_source_repair_not_ready(err));
}

#[test]
fn l4_demo_evidence_accepts_bounded_materialization_unready() {
    let err = "http 503 failed: {\"error\":\"trusted-materialized reads remain unavailable until package-local materialized observation evidence is trusted: initial audit incomplete for groups [nfs2]\"}";
    assert!(is_trusted_materialized_status_unavailable(err));
    let report = bounded_materialization_unready_demo_evidence(
        DemoEvidenceEnvironment::Full5Node5Nfs,
        DemoEvidencePolicy::EnvironmentBaseline,
        err,
    );
    assert!(report.accepted_for(DemoEvidencePolicy::EnvironmentBaseline));
    assert!(!report.accepted_for(DemoEvidencePolicy::FullAcceptance));
}

#[test]
fn l4_live_only_mode_uses_environment_baseline_evidence_policy() {
    assert_eq!(
        demo_evidence_policy_for_mode(MatrixMode::LiveOnlyOnly),
        DemoEvidencePolicy::EnvironmentBaseline,
        "live-only is part of L4 environment validation; L5 full acceptance remains a separate real-cluster gate"
    );
}

fn run_mini_roots_management_smoke(
    session: &mut OperatorSession,
    lab: &NfsLab,
) -> Result<(), String> {
    let current = session.monitoring_roots()?;
    let current_rows = current
        .get("roots")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("monitoring_roots missing roots array: {current}"))?;
    if current_rows.len() != MINI_EXPORTS.len() {
        return Err(format!(
            "expected {} mini roots from release doc, got {current}",
            MINI_EXPORTS.len()
        ));
    }

    let roots = mini_roots(lab);
    let preview = session.preview_roots_raw(&roots_payload(&roots))?;
    assert_status(preview.status, 200, "mini roots preview")?;
    if preview
        .body
        .get("preview")
        .and_then(Value::as_array)
        .map(|items| items.len())
        != Some(MINI_EXPORTS.len())
    {
        return Err(format!("expected mini preview rows: {}", preview.body));
    }
    if preview
        .body
        .get("unmatched_roots")
        .and_then(Value::as_array)
        .map(|items| !items.is_empty())
        .unwrap_or(true)
    {
        return Err(format!(
            "mini preview unexpectedly has unmatched roots: {}",
            preview.body
        ));
    }

    let put = session.update_roots_raw(&roots_payload(&roots))?;
    assert_status(put.status, 200, "mini roots apply")?;
    if put.body.get("roots_count").and_then(Value::as_u64) != Some(MINI_EXPORTS.len() as u64) {
        return Err(format!("mini roots apply did not converge: {}", put.body));
    }
    session.rescan()?;
    wait_until(Duration::from_secs(60), "mini roots remain visible", || {
        let roots = session.monitoring_roots()?;
        Ok(roots
            .get("roots")
            .and_then(Value::as_array)
            .map(|items| items.len() == MINI_EXPORTS.len())
            .unwrap_or(false))
    })?;
    Ok(())
}

fn run_mini_query_and_key_smoke(session: &mut OperatorSession, lab: &NfsLab) -> Result<(), String> {
    session.rescan()?;
    wait_until(
        Duration::from_secs(180),
        "mini materialized tree covers all roots",
        || {
            let tree = session.tree(&[
                ("path", "/".to_string()),
                ("recursive", "true".to_string()),
                ("read_class", "materialized".to_string()),
            ])?;
            let ready = MINI_EXPORTS
                .iter()
                .all(|root| group_total_nodes(&tree, root) >= 11);
            if ready {
                Ok(true)
            } else {
                let status = session
                    .status()
                    .unwrap_or_else(|status_err| json!({ "status_error": status_err }));
                Err(format!("tree={tree}; status={status}"))
            }
        },
    )?;

    let tree = session.tree(&[
        ("path", "/".to_string()),
        ("recursive", "true".to_string()),
        ("read_class", "materialized".to_string()),
    ])?;
    if tree.get("read_class").and_then(Value::as_str) != Some("materialized") {
        return Err(format!(
            "mini materialized tree returned wrong read_class: {tree}"
        ));
    }
    let observation_state = tree
        .get("observation_status")
        .and_then(|status| status.get("state"))
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    if !matches!(
        observation_state,
        "materialized-untrusted" | "trusted-materialized"
    ) {
        return Err(format!(
            "mini materialized tree returned unexpected observation state {observation_state}: {tree}"
        ));
    }
    for root in MINI_EXPORTS {
        if group_total_nodes(&tree, root) < 11 {
            return Err(format!(
                "mini materialized tree missing nodes for root {root}: {tree}"
            ));
        }
    }

    let grants = session.runtime_grants()?;
    let roots = MINI_EXPORTS
        .iter()
        .map(|root| (*root, lab.export_source(root)))
        .collect::<Vec<_>>();
    let root_refs = roots
        .iter()
        .map(|(root, source)| (*root, source.as_str()))
        .collect::<Vec<_>>();
    let all_mounts = group_mount_pairs_for_roots(&grants, &root_refs)?;

    let force_find =
        session.force_find(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    let expected_force_find = FsTreeOracle::grouped_force_find_response(
        &all_mounts,
        "/",
        true,
        None,
        1_000,
        "group-key",
        64,
    )?;
    assert_json_eq(
        "mini all groups force-find",
        &force_find,
        &expected_force_find,
    )?;

    let stats = session.stats(&[
        ("path", "/".to_string()),
        ("recursive", "true".to_string()),
        ("read_class", "materialized".to_string()),
    ])?;
    if stats.get("read_class").and_then(Value::as_str) != Some("materialized") {
        return Err(format!(
            "mini materialized stats returned wrong read_class: {stats}"
        ));
    }
    for root in MINI_EXPORTS {
        let total_nodes = stats
            .get("groups")
            .and_then(Value::as_object)
            .and_then(|groups| groups.get(root))
            .and_then(|group| group.get("data"))
            .and_then(|data| data.get("total_nodes"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if total_nodes < 10 {
            return Err(format!(
                "mini materialized stats missing nodes for root {root}: {stats}"
            ));
        }
    }

    let keys_before = session.client().list_query_api_keys(session.token())?;
    let created = session
        .client()
        .create_query_api_key(session.token(), "mini-smoke-extra-key")?;
    let api_key = created
        .get("api_key")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("query api key response missing api_key: {created}"))?;
    let key_id = created
        .get("key")
        .and_then(|key| key.get("key_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| format!("query api key response missing key.key_id: {created}"))?;
    let _ = session.client().tree(
        api_key,
        &[
            ("path", "/".to_string()),
            ("recursive", "false".to_string()),
            ("read_class", "materialized".to_string()),
        ],
    )?;
    let keys_after_create = session.client().list_query_api_keys(session.token())?;
    if !query_api_key_list_contains(&keys_after_create, key_id) {
        return Err(format!(
            "created query api key {key_id} not visible: before={keys_before}; after={keys_after_create}"
        ));
    }
    let revoked = session
        .client()
        .revoke_query_api_key(session.token(), key_id)?;
    if revoked.get("revoked").and_then(Value::as_bool) != Some(true) {
        return Err(format!(
            "query api key revoke did not report true: {revoked}"
        ));
    }
    assert_error(
        session
            .client()
            .tree_raw(api_key, &[("path", "/".to_string())])?,
        401,
        "invalid query api key",
    )?;
    Ok(())
}

fn query_api_key_list_contains(keys: &Value, key_id: &str) -> bool {
    keys.get("keys")
        .and_then(Value::as_array)
        .is_some_and(|rows| {
            rows.iter()
                .any(|row| row.get("key_id").and_then(Value::as_str) == Some(key_id))
        })
}

fn seed_baseline_content(lab: &NfsLab) -> Result<(), String> {
    lab.mkdir("nfs1", "nested/child")?;
    lab.write_file("nfs1", "nested/child/deep.txt", "deep-nfs1\n")?;
    lab.write_file("nfs1", "extra-count-1.txt", "count-1\n")?;
    lab.write_file("nfs1", "extra-count-2.txt", "count-2\n")?;
    thread::sleep(Duration::from_millis(5));
    lab.write_file("nfs2", "latest-age.txt", "latest-nfs2\n")?;
    lab.mkdir("nfs2", "nested")?;
    lab.write_file("nfs2", "nested/peer.txt", "peer\n")?;
    thread::sleep(Duration::from_millis(5));
    lab.write_file("nfs3", "misc.txt", "misc\n")?;
    lab.write_file("nfs4", "demo/full-node-d.txt", "full-nfs4\n")?;
    lab.write_file("nfs5", "demo/full-node-e.txt", "full-nfs5\n")?;
    Ok(())
}

fn seed_mini_content(lab: &NfsLab) -> Result<(), String> {
    for export_name in MINI_EXPORTS {
        for index in 1..=7 {
            lab.write_file(
                export_name,
                &format!("data/file-{index:02}.txt"),
                &format!("{export_name}-file-{index:02}\n"),
            )?;
        }
    }
    Ok(())
}

fn seed_active_three_content(lab: &NfsLab) -> Result<(), String> {
    let files_per_root = active_three_seed_env_usize(ACTIVE_THREE_FILES_PER_ROOT_ENV, 9)?;
    let dirs_per_root = active_three_seed_env_usize(ACTIVE_THREE_DIRS_PER_ROOT_ENV, 1)?.max(1);
    eprintln!(
        "[fs-meta-active3-replay] seed files_per_root={files_per_root} dirs_per_root={dirs_per_root}"
    );
    for export_name in ACTIVE_THREE_EXPORTS {
        if files_per_root == 9 && dirs_per_root == 1 {
            for index in 1..=9 {
                lab.write_file(
                    export_name,
                    &format!("active3/file-{index:02}.txt"),
                    &format!("{export_name}-file-{index:02}\n"),
                )?;
            }
            continue;
        }
        for index in 0..files_per_root {
            let dir_index = index % dirs_per_root;
            lab.write_file(
                export_name,
                &format!("active3/dir-{dir_index:04}/file-{index:08}.txt"),
                &format!("{export_name}-file-{index:08}\n"),
            )?;
        }
    }
    Ok(())
}

fn active_three_seed_env_usize(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<usize>()
            .map_err(|err| format!("parse {name}={raw:?} as usize failed: {err}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(format!("read {name} failed: {err}")),
    }
}

fn install_baseline_resources(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    facade_resource_id: &str,
    facade_addrs: &[String],
) -> Result<(), String> {
    for (node_name, export_name) in FULL_MOUNTS {
        let mount_path = lab.mount_export(node_name, export_name)?;
        let node_id = cluster.node_id(node_name)?;
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": export_name,
            "node_id": node_id,
            "resource_kind": "nfs",
            "source": lab.export_source(export_name),
            "mount_hint": mount_path.display().to_string(),
        })])?;
    }
    for (node_name, bind_addr) in ["node-d", "node-e"].into_iter().zip(facade_addrs.iter()) {
        let node_id = cluster.node_id(node_name)?;
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": facade_resource_id,
            "node_id": node_id,
            "resource_kind": "tcp_listener",
            "source": format!("http://{node_name}/listener/{facade_resource_id}"),
            "bind_addr": bind_addr,
        })])?;
    }
    Ok(())
}

fn install_full_demo_resources(
    cluster: &Cluster5,
    roots: &[FullDemoRoot],
    facade_resource_id: &str,
    facade_addrs: &[String],
) -> Result<(), String> {
    for (node_name, export_name) in FULL_MOUNTS {
        let root = roots
            .iter()
            .find(|root| root.id == export_name)
            .ok_or_else(|| format!("full demo root {export_name} not mapped"))?;
        let node_id = cluster.node_id(node_name)?;
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": export_name,
            "node_id": node_id,
            "resource_kind": "nfs",
            "source": root.source.clone(),
            "host_ip": root.host_ip.clone(),
            "mount_hint": root.mount_point.display().to_string(),
        })])?;
    }
    for (node_name, bind_addr) in ["node-d", "node-e"].into_iter().zip(facade_addrs.iter()) {
        let node_id = cluster.node_id(node_name)?;
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": facade_resource_id,
            "node_id": node_id,
            "resource_kind": "tcp_listener",
            "source": format!("http://{node_name}/listener/{facade_resource_id}"),
            "bind_addr": bind_addr,
        })])?;
    }
    Ok(())
}

fn install_mini_resources(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    facade_resource_id: &str,
    facade_addrs: &[String],
) -> Result<(), String> {
    for (node_name, export_name) in MINI_MOUNTS {
        let mount_path = lab.mount_export(node_name, export_name)?;
        let node_id = cluster.node_id(node_name)?;
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": export_name,
            "node_id": node_id,
            "resource_kind": "nfs",
            "source": lab.export_source(export_name),
            "mount_hint": mount_path.display().to_string(),
        })])?;
    }
    for (node_name, bind_addr) in ["node-d", "node-e"].into_iter().zip(facade_addrs.iter()) {
        let node_id = cluster.node_id(node_name)?;
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": facade_resource_id,
            "node_id": node_id,
            "resource_kind": "tcp_listener",
            "source": format!("http://{node_name}/listener/{facade_resource_id}"),
            "bind_addr": bind_addr,
        })])?;
    }
    Ok(())
}

fn install_active_three_resources(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    facade_resource_id: &str,
    facade_addrs: &[String],
) -> Result<(), String> {
    for (node_name, export_name) in ACTIVE_THREE_MOUNTS {
        let mount_path = lab.mount_export(node_name, export_name)?;
        let node_id = cluster.node_id(node_name)?;
        cluster.announce_resources_clusterwide(vec![json!({
            "resource_id": export_name,
            "node_id": node_id,
            "resource_kind": "nfs",
            "source": lab.export_source(export_name),
            "mount_hint": mount_path.display().to_string(),
        })])?;
    }
    let bind_addr = facade_addrs
        .first()
        .ok_or_else(|| "active-three replay requires one facade bind addr".to_string())?;
    let node_id = cluster.node_id("node-b")?;
    cluster.announce_resources_clusterwide(vec![json!({
        "resource_id": facade_resource_id,
        "node_id": node_id,
        "resource_kind": "tcp_listener",
        "source": format!("http://node-b/listener/{facade_resource_id}"),
        "bind_addr": bind_addr,
    })])?;
    Ok(())
}

fn assert_baseline_mounts_live(lab: &NfsLab) {
    for (node_name, export_name) in FULL_MOUNTS {
        let mount_path = lab
            .mount_path(node_name, export_name)
            .unwrap_or_else(|| panic!("missing mount path for {node_name}/{export_name}"));
        assert!(
            mount_path.exists(),
            "mount path vanished for {node_name}/{export_name}: {}",
            mount_path.display()
        );
        let status = Command::new("mountpoint")
            .arg("-q")
            .arg(&mount_path)
            .status()
            .expect("run mountpoint");
        assert!(
            status.success(),
            "mount path is no longer an active mount for {node_name}/{export_name}: {}",
            mount_path.display()
        );
        assert!(
            mount_path.join("root.txt").exists(),
            "mounted export content is not visible for {node_name}/{export_name}: {}",
            mount_path.display()
        );
    }
}

#[test]
fn install_baseline_resources_keeps_real_nfs_mounts_live_through_initial_release() {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-matrix] skipped: {reason}");
        return;
    }

    let mut lab = NfsLab::start_with_exports(FULL_EXPORTS).expect("start NFS lab");
    seed_baseline_content(&lab).expect("seed baseline content");
    let cluster = Cluster5::start().expect("start cluster");
    let app_id = format!("fs-meta-api-matrix-mount-liveness-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(1).expect("reserve facade addrs");
    install_baseline_resources(&cluster, &mut lab, &facade_resource_id, &facade_addrs)
        .expect("install baseline resources");

    let roots = baseline_roots(&lab);
    let release = cluster
        .build_fs_meta_release(&app_id, &facade_resource_id, roots, 1, true)
        .expect("build release");
    cluster
        .apply_release("node-a", release)
        .expect("apply release");

    assert_baseline_mounts_live(&lab);
}

#[test]
fn matrix_harness_keeps_real_nfs_mounts_live_through_initial_rescan() {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-matrix] skipped: {reason}");
        return;
    }

    let harness = build_matrix_harness("fs-meta-api-matrix-mount-rescan");
    let harness = harness.expect("build matrix harness");
    let mut session = OperatorSession::login_many(
        harness.candidate_base_urls.clone(),
        "operator",
        "operator123",
    )
    .expect("login many");
    session.rescan().expect("initial rescan");

    assert_baseline_mounts_live(&harness.lab);
}

#[test]
fn baseline_tree_materialization_wait_keeps_real_nfs_mounts_live() {
    if let Some(reason) = skip_unless_real_nfs_enabled() {
        eprintln!("[fs-meta-api-matrix] skipped: {reason}");
        return;
    }

    let harness = build_matrix_harness("fs-meta-api-matrix-wait-liveness");
    let harness = harness.expect("build matrix harness");
    let mut session = OperatorSession::login_many(
        harness.candidate_base_urls.clone(),
        "operator",
        "operator123",
    )
    .expect("login many");
    session.rescan().expect("initial rescan");

    let mut last_rescan_at = std::time::Instant::now();
    wait_until(
        Duration::from_secs(90),
        "baseline tree materializes without losing active mounts",
        || {
            if last_rescan_at.elapsed() >= Duration::from_secs(10) {
                session.rescan()?;
                last_rescan_at = std::time::Instant::now();
            }

            assert_baseline_mounts_live(&harness.lab);

            let tree = session
                .tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])
                .unwrap_or_else(|err| json!({ "tree_error": err }));
            let ready = tree
                .get("groups")
                .and_then(Value::as_array)
                .map(|groups| {
                    groups.len() >= FULL_EXPORTS.len()
                        && group_total_nodes(&tree, "nfs1") > 0
                        && group_total_nodes(&tree, "nfs2") > 0
                        && group_total_nodes(&tree, "nfs3") > 0
                        && group_total_nodes(&tree, "nfs4") > 0
                        && group_total_nodes(&tree, "nfs5") > 0
                })
                .unwrap_or(false);
            if ready {
                Ok(true)
            } else {
                let status = session
                    .status()
                    .unwrap_or_else(|status_err| json!({ "status_error": status_err }));
                Err(format!("tree={tree}; status={status}"))
            }
        },
    )
    .expect("baseline tree wait should preserve active mounts");
}

fn baseline_roots(lab: &NfsLab) -> Vec<RootSpec> {
    FULL_EXPORTS
        .iter()
        .map(|export_name| root_spec(export_name, &lab.export_source(export_name)))
        .collect()
}

fn mini_roots(lab: &NfsLab) -> Vec<RootSpec> {
    MINI_EXPORTS
        .iter()
        .map(|export_name| root_spec(export_name, &lab.export_source(export_name)))
        .collect()
}

fn active_three_roots(lab: &NfsLab) -> Vec<RootSpec> {
    ACTIVE_THREE_EXPORTS
        .iter()
        .map(|export_name| root_spec(export_name, &lab.export_source(export_name)))
        .collect()
}

fn root_spec(id: &str, fs_source: &str) -> RootSpec {
    RootSpec {
        id: id.to_string(),
        selector: RootSelector {
            mount_point: None,
            fs_source: Some(fs_source.to_string()),
            fs_type: None,
            host_ip: None,
            host_ref: None,
        },
        subpath_scope: PathBuf::from("/"),
        watch: true,
        scan: true,
        audit_interval_ms: None,
    }
}

fn selector_payload(root: &RootSpec) -> Value {
    json!({
        "fs_source": root.selector.fs_source.clone(),
        "mount_point": root.selector.mount_point.as_ref().map(|path| path.display().to_string()),
        "fs_type": root.selector.fs_type.clone(),
        "host_ip": root.selector.host_ip.clone(),
        "host_ref": root.selector.host_ref.clone(),
    })
}

fn roots_payload(roots: &[RootSpec]) -> Value {
    Value::Array(
        roots
            .iter()
            .map(|root| {
                json!({
                    "id": root.id,
                    "selector": selector_payload(root),
                    "subpath_scope": root.subpath_scope.display().to_string(),
                    "watch": root.watch,
                    "scan": root.scan,
                })
            })
            .collect(),
    )
}

fn root_payload_flags(
    id: &str,
    fs_source: &str,
    subpath_scope: &str,
    watch: bool,
    scan: bool,
) -> Value {
    json!({
        "id": id,
        "selector": { "fs_source": fs_source },
        "subpath_scope": subpath_scope,
        "watch": watch,
        "scan": scan,
    })
}

fn assert_status(actual: u16, expected: u16, context: &str) -> Result<(), String> {
    if actual != expected {
        return Err(format!(
            "{context}: expected status {expected}, got {actual}"
        ));
    }
    Ok(())
}

fn assert_response_status(
    response: &ApiResponse,
    expected: u16,
    context: &str,
) -> Result<(), String> {
    if response.status != expected {
        return Err(format!(
            "{context}: expected status {expected}, got {}; body={}",
            response.status, response.body
        ));
    }
    Ok(())
}

fn assert_error(response: ApiResponse, expected_status: u16, needle: &str) -> Result<(), String> {
    if response.status != expected_status {
        return Err(format!(
            "error response: expected status {expected_status}, got {}; body={}",
            response.status, response.body
        ));
    }
    let body = response.body.to_string();
    if !body.contains(needle) {
        return Err(format!(
            "expected response body to contain '{needle}', got {}",
            response.body
        ));
    }
    Ok(())
}

fn assert_trusted_materialized_not_ready(
    response: ApiResponse,
    context: &str,
) -> Result<(), String> {
    assert_status(response.status, 503, context)?;
    if response.body.get("code").and_then(Value::as_str) != Some("NOT_READY") {
        return Err(format!(
            "{context}: expected NOT_READY code for closed trusted-materialized readiness, got {}",
            response.body
        ));
    }
    let error = response
        .body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !error.contains("trusted-materialized") {
        return Err(format!(
            "{context}: expected trusted-materialized readiness explanation, got {}",
            response.body
        ));
    }
    Ok(())
}

fn assert_trusted_tree_matches_readiness(
    session: &mut OperatorSession,
    context: &str,
) -> Result<(), String> {
    let status = trusted_observation_status_gate(session, context)?;
    let trusted_ready = status
        .pointer("/readiness_planes/trusted_observation_readiness")
        .and_then(Value::as_bool)
        == Some(true);
    let response =
        session.tree_raw(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;

    if trusted_ready {
        if response.status != 200 {
            let status_after_tree = session.status()?;
            return Err(format!(
                "{context}: status reported trusted observation ready but /tree did not return 200; before={status}; after={status_after_tree}; response_status={}; response_body={}",
                response.status, response.body
            ));
        }
        assert_trusted_tree_response(response, context)?;
    } else {
        if response.status == 200 {
            let status_after_tree = trusted_observation_status_gate(session, context)?;
            if status_after_tree
                .pointer("/readiness_planes/trusted_observation_readiness")
                .and_then(Value::as_bool)
                != Some(true)
            {
                return Err(format!(
                    "{context}: /tree returned trusted-materialized success while trusted observation readiness remained false; before={status}; after={status_after_tree}; body={}",
                    response.body
                ));
            }
            assert_trusted_tree_response(response, context)?;
        } else {
            assert_trusted_materialized_not_ready(response, context)?;
        }
    }

    Ok(())
}

fn trusted_observation_status_gate(
    session: &mut OperatorSession,
    context: &str,
) -> Result<Value, String> {
    let response = session.get_json_raw("/status")?;
    match response.status {
        200 => Ok(response.body),
        503 => {
            let body = response.body.to_string();
            if body.contains("trusted-materialized reads remain unavailable")
                || body.contains("package-local materialized observation evidence is trusted")
            {
                Ok(json!({
                    "readiness_planes": {
                        "trusted_observation_readiness": false
                    },
                    "status_not_ready": response.body,
                }))
            } else {
                Err(format!(
                    "{context}: status failed with non-materialized 503: {}",
                    response.body
                ))
            }
        }
        status => Err(format!(
            "{context}: status expected 200 or trusted-materialized 503, got {status}: {}",
            response.body
        )),
    }
}

fn assert_trusted_tree_response(response: ApiResponse, context: &str) -> Result<(), String> {
    if response.body.get("read_class").and_then(Value::as_str) != Some("trusted-materialized") {
        return Err(format!(
            "{context}: /tree success did not return trusted-materialized evidence: body={}",
            response.body
        ));
    }
    if !response.body.get("groups").is_some_and(Value::is_array) {
        return Err(format!(
            "{context}: /tree success missing groups array: body={}",
            response.body
        ));
    }
    Ok(())
}

fn group_total_nodes(payload: &Value, group_key: &str) -> u64 {
    let Some(group) = payload
        .get("groups")
        .and_then(Value::as_array)
        .and_then(|groups| {
            groups
                .iter()
                .find(|group| group.get("group").and_then(Value::as_str) == Some(group_key))
        })
    else {
        return 0;
    };
    let root_exists = group
        .get("root")
        .and_then(|root| root.get("exists"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let entries = group
        .get("entries")
        .and_then(Value::as_array)
        .map(|rows| rows.len() as u64)
        .unwrap_or(0);
    entries + if root_exists { 1 } else { 0 }
}

fn mounts_by_fs_source(grants: &Value) -> Result<BTreeMap<String, Vec<PathBuf>>, String> {
    let mut by_fs_source = BTreeMap::<String, Vec<PathBuf>>::new();
    let rows = grants
        .get("grants")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("runtime grants missing rows: {grants}"))?;
    for row in rows {
        let fs_source = row
            .get("fs_source")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("grant row missing fs_source: {row}"))?;
        let mount_point = row
            .get("mount_point")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("grant row missing mount_point: {row}"))?;
        by_fs_source
            .entry(fs_source.to_string())
            .or_default()
            .push(PathBuf::from(mount_point));
    }
    Ok(by_fs_source)
}

fn normalize_mount_candidate_for_group(group_id: &str, path: &std::path::Path) -> PathBuf {
    path.ancestors()
        .find(|ancestor| {
            ancestor.exists()
                && ancestor
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name == group_id)
        })
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| path.to_path_buf())
}

fn representative_mount_for_group(group_id: &str, paths: &[PathBuf]) -> Option<PathBuf> {
    let mut candidates = paths
        .iter()
        .map(|path| normalize_mount_candidate_for_group(group_id, path))
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| {
        (!a.exists())
            .cmp(&(!b.exists()))
            .then_with(|| a.components().count().cmp(&b.components().count()))
            .then_with(|| a.cmp(b))
    });
    candidates.into_iter().next()
}

fn path_has_visible_entries(path: &std::path::Path) -> bool {
    std::fs::read_dir(path)
        .ok()
        .and_then(|mut entries| entries.next())
        .is_some()
}

fn local_existing_path_for_fs_source(fs_source: &str) -> Option<PathBuf> {
    let (_, path) = fs_source.split_once(':')?;
    if !path.starts_with('/') {
        return None;
    }
    let path = PathBuf::from(path);
    path.exists().then_some(path)
}

fn group_mount_pairs_for_roots(
    grants: &Value,
    roots: &[(&str, &str)],
) -> Result<Vec<(String, PathBuf)>, String> {
    let mount_map = mounts_by_fs_source(grants)?;
    roots
        .iter()
        .map(|(group_id, fs_source)| {
            let local_existing = local_existing_path_for_fs_source(fs_source);
            let candidate = mount_map
                .get(*fs_source)
                .and_then(|paths| representative_mount_for_group(group_id, paths))
                .or_else(|| local_existing.clone())
                .ok_or_else(|| format!("missing representative mount for fs_source {fs_source}"))?;
            let chosen = match local_existing {
                Some(local)
                    if local.exists()
                        && local.is_dir()
                        && path_has_visible_entries(&local)
                        && (!candidate.exists()
                            || !candidate.is_dir()
                            || !path_has_visible_entries(&candidate)) =>
                {
                    local
                }
                _ => candidate,
            };
            Ok((group_id.to_string(), chosen))
        })
        .collect()
}

#[test]
fn group_mount_pairs_for_roots_prefers_existing_root_mount_over_nested_child_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root_mount = temp.path().join("mounts/node-b/nfs3");
    std::fs::create_dir_all(&root_mount).expect("create root mount");
    let nested_child = root_mount.join("baseline");
    let grants = json!({
        "grants": [
            {
                "fs_source": "nfs3-source",
                "mount_point": nested_child.display().to_string()
            },
            {
                "fs_source": "nfs3-source",
                "mount_point": root_mount.display().to_string()
            }
        ]
    });

    let pairs = group_mount_pairs_for_roots(&grants, &[("nfs3", "nfs3-source")]).expect("pairs");

    assert_eq!(pairs, vec![("nfs3".to_string(), root_mount)]);
}

#[test]
fn group_mount_pairs_for_roots_recovers_existing_group_root_from_missing_nested_child_mount() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root_mount = temp.path().join("mounts/node-b/nfs3");
    std::fs::create_dir_all(&root_mount).expect("create root mount");
    let missing_nested_child = root_mount.join("baseline");
    let grants = json!({
        "grants": [
            {
                "fs_source": "nfs3-source",
                "mount_point": missing_nested_child.display().to_string()
            }
        ]
    });

    let pairs = group_mount_pairs_for_roots(&grants, &[("nfs3", "nfs3-source")]).expect("pairs");

    assert_eq!(pairs, vec![("nfs3".to_string(), root_mount)]);
}

#[test]
fn group_mount_pairs_for_roots_recovers_existing_group_root_from_existing_nested_child_mount() {
    let temp = tempfile::tempdir().expect("tempdir");
    let root_mount = temp.path().join("mounts/node-a/nfs1");
    let nested_child = root_mount.join("nested/child");
    std::fs::create_dir_all(&nested_child).expect("create nested child");
    let grants = json!({
        "grants": [
            {
                "fs_source": "nfs1-source",
                "mount_point": nested_child.display().to_string()
            }
        ]
    });

    let pairs = group_mount_pairs_for_roots(&grants, &[("nfs1", "nfs1-source")]).expect("pairs");

    assert_eq!(pairs, vec![("nfs1".to_string(), root_mount)]);
}

#[test]
fn group_mount_pairs_for_roots_prefers_runtime_mount_over_local_export_source_when_both_exist() {
    let temp = tempfile::tempdir().expect("tempdir");
    let export_root = temp.path().join("exports/nfs1");
    let mount_root = temp.path().join("mounts/node-a/nfs1");
    std::fs::create_dir_all(&export_root).expect("create export root");
    std::fs::create_dir_all(&mount_root).expect("create mount root");
    std::fs::write(export_root.join("root.txt"), "hello\n").expect("write export file");
    std::fs::write(mount_root.join("root.txt"), "mounted\n").expect("write mount file");
    let fs_source = format!("127.0.0.1:{}", export_root.display());
    let grants = json!({
        "grants": [
            {
                "fs_source": fs_source,
                "mount_point": mount_root.display().to_string()
            }
        ]
    });

    let pairs =
        group_mount_pairs_for_roots(&grants, &[("nfs1", fs_source.as_str())]).expect("pairs");

    assert_eq!(pairs, vec![("nfs1".to_string(), mount_root)]);
}

#[test]
fn group_mount_pairs_for_roots_prefers_local_export_source_over_empty_runtime_mount() {
    let temp = tempfile::tempdir().expect("tempdir");
    let export_root = temp.path().join("exports/nfs1");
    let mount_root = temp.path().join("mounts/node-a/nfs1");
    std::fs::create_dir_all(&export_root).expect("create export root");
    std::fs::create_dir_all(&mount_root).expect("create mount root");
    std::fs::write(export_root.join("root.txt"), "hello\n").expect("write export file");
    let fs_source = format!("127.0.0.1:{}", export_root.display());
    let grants = json!({
        "grants": [
            {
                "fs_source": fs_source,
                "mount_point": mount_root.display().to_string()
            }
        ]
    });

    let pairs =
        group_mount_pairs_for_roots(&grants, &[("nfs1", fs_source.as_str())]).expect("pairs");

    assert_eq!(pairs, vec![("nfs1".to_string(), export_root)]);
}

#[test]
fn group_mount_pairs_for_roots_prefers_local_export_source_over_missing_runtime_mount() {
    let temp = tempfile::tempdir().expect("tempdir");
    let export_root = temp.path().join("exports/nfs1");
    let missing_mount_root = temp.path().join("mounts/node-a/nfs1");
    std::fs::create_dir_all(&export_root).expect("create export root");
    std::fs::write(export_root.join("root.txt"), "hello\n").expect("write export file");
    let fs_source = format!("127.0.0.1:{}", export_root.display());
    let grants = json!({
        "grants": [
            {
                "fs_source": fs_source,
                "mount_point": missing_mount_root.display().to_string()
            }
        ]
    });

    let pairs =
        group_mount_pairs_for_roots(&grants, &[("nfs1", fs_source.as_str())]).expect("pairs");

    assert_eq!(pairs, vec![("nfs1".to_string(), export_root)]);
}

fn normalize_tree_like_json(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let is_dir = map.get("is_dir").and_then(Value::as_bool).unwrap_or(false);
            if is_dir {
                map.insert("modified_time_us".into(), Value::Number(0u64.into()));
            }
            if map
                .get("next_cursor")
                .is_some_and(|value| matches!(value, Value::String(_)))
            {
                map.insert("next_cursor".into(), Value::Null);
            }
            if map
                .get("next_entry_after")
                .is_some_and(|value| matches!(value, Value::String(_)))
            {
                map.insert("next_entry_after".into(), Value::Null);
            }
            if map.contains_key("id") && map.contains_key("expires_at_ms") {
                map.insert("id".into(), Value::String("oracle-pit".into()));
                map.insert("expires_at_ms".into(), Value::Number(0u64.into()));
            }
            if is_materialized_tree_group_envelope(map) {
                map.insert("reliable".into(), Value::String("oracle-dynamic".into()));
                map.insert(
                    "unreliable_reason".into(),
                    Value::String("oracle-dynamic".into()),
                );
                map.insert(
                    "stability".into(),
                    json!({
                        "mode": "oracle-dynamic",
                        "state": "oracle-dynamic",
                        "quiet_window_ms": "oracle-dynamic",
                        "observed_quiet_for_ms": "oracle-dynamic",
                        "remaining_ms": "oracle-dynamic",
                        "blocked_reasons": ["oracle-dynamic"],
                    }),
                );
            }
            for child in map.values_mut() {
                normalize_tree_like_json(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_tree_like_json(item);
            }
        }
        _ => {}
    }
}

fn is_materialized_tree_group_envelope(map: &serde_json::Map<String, Value>) -> bool {
    map.contains_key("group")
        && map.contains_key("reliable")
        && map.contains_key("unreliable_reason")
        && map.contains_key("stability")
        && map
            .get("meta")
            .and_then(Value::as_object)
            .and_then(|meta| meta.get("read_class"))
            .and_then(Value::as_str)
            == Some("trusted-materialized")
}

fn validate_materialized_tree_group_dynamic_fields(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(map) => {
            if is_materialized_tree_group_envelope(map) {
                let group = map
                    .get("group")
                    .and_then(Value::as_str)
                    .unwrap_or("<unknown>");
                if map.get("reliable").and_then(Value::as_bool).is_none() {
                    return Err(format!(
                        "trusted-materialized group {group} missing boolean reliable: {value}"
                    ));
                }
                match map.get("unreliable_reason") {
                    Some(Value::Null) => {}
                    Some(Value::String(reason))
                        if matches!(
                            reason.as_str(),
                            "Unattested"
                                | "SuspectNodes"
                                | "BlindSpotsDetected"
                                | "WatchOverflowPendingMaterialization"
                        ) => {}
                    Some(other) => {
                        return Err(format!(
                            "trusted-materialized group {group} has invalid unreliable_reason {other}"
                        ));
                    }
                    None => {
                        return Err(format!(
                            "trusted-materialized group {group} missing unreliable_reason: {value}"
                        ));
                    }
                }
                let stability =
                    map.get("stability")
                        .and_then(Value::as_object)
                        .ok_or_else(|| {
                            format!(
                            "trusted-materialized group {group} missing stability object: {value}"
                        )
                        })?;
                let mode = stability
                    .get("mode")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        format!(
                            "trusted-materialized group {group} missing stability.mode: {value}"
                        )
                    })?;
                if !matches!(mode, "none" | "quiet-window") {
                    return Err(format!(
                        "trusted-materialized group {group} has invalid stability.mode={mode}: {value}"
                    ));
                }
                let state = stability
                    .get("state")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        format!(
                            "trusted-materialized group {group} missing stability.state: {value}"
                        )
                    })?;
                if !matches!(
                    state,
                    "not-evaluated" | "stable" | "unstable" | "unknown" | "degraded"
                ) {
                    return Err(format!(
                        "trusted-materialized group {group} has invalid stability.state={state}: {value}"
                    ));
                }
                for key in ["quiet_window_ms", "observed_quiet_for_ms", "remaining_ms"] {
                    if !stability
                        .get(key)
                        .is_some_and(|field| field.is_null() || field.is_u64())
                    {
                        return Err(format!(
                            "trusted-materialized group {group} has invalid stability.{key}: {value}"
                        ));
                    }
                }
                if !stability
                    .get("blocked_reasons")
                    .and_then(Value::as_array)
                    .is_some_and(|reasons| reasons.iter().all(Value::is_string))
                {
                    return Err(format!(
                        "trusted-materialized group {group} has invalid stability.blocked_reasons: {value}"
                    ));
                }
            }
            for child in map.values() {
                validate_materialized_tree_group_dynamic_fields(child)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                validate_materialized_tree_group_dynamic_fields(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn assert_json_eq(label: &str, actual: &Value, expected: &Value) -> Result<(), String> {
    validate_materialized_tree_group_dynamic_fields(actual)?;
    let mut actual_normalized = actual.clone();
    let mut expected_normalized = expected.clone();
    normalize_tree_like_json(&mut actual_normalized);
    normalize_tree_like_json(&mut expected_normalized);
    if actual_normalized != expected_normalized {
        return Err(format!(
            "{label} mismatch\nactual={}\nexpected={}",
            serde_json::to_string_pretty(&actual_normalized)
                .unwrap_or_else(|_| actual_normalized.to_string()),
            serde_json::to_string_pretty(&expected_normalized)
                .unwrap_or_else(|_| expected_normalized.to_string())
        ));
    }
    Ok(())
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn active_three_status_covers_roots(status: &Value) -> bool {
    let expected = ACTIVE_THREE_EXPORTS.len() as u64;
    let roots_count = status
        .get("source")
        .and_then(|source| source.get("roots_count"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let sink_groups = status
        .get("sink")
        .and_then(|sink| sink.get("groups"))
        .and_then(Value::as_array)
        .map(|groups| groups.len())
        .unwrap_or(0);
    roots_count >= expected && sink_groups >= ACTIVE_THREE_EXPORTS.len()
}

fn validate_active_three_status_ownership(
    status: &Value,
    expected: &BTreeMap<String, String>,
) -> Result<(), String> {
    let mut blockers = Vec::new();
    if !active_three_status_covers_roots(status) {
        blockers.push(format!(
            "status does not yet cover all active-three roots: {status}"
        ));
    }

    let source_debug = status
        .get("source")
        .and_then(|source| source.get("debug"))
        .ok_or_else(|| format!("status.source.debug missing: {status}"))?;
    let sink = status
        .get("sink")
        .ok_or_else(|| format!("status.sink missing: {status}"))?;
    let sink_debug = sink
        .get("debug")
        .ok_or_else(|| format!("status.sink.debug missing: {status}"))?;

    let active_nodes = expected.values().cloned().collect::<BTreeSet<_>>();

    validate_primary_owner_map(
        "source.debug.source_primary_by_group",
        &status_string_map(source_debug, "source_primary_by_group", status)?,
        expected,
        &mut blockers,
    );
    validate_primary_owner_map(
        "sink.primary_host_ref_by_group",
        &status_string_map(sink, "primary_host_ref_by_group", status)?,
        expected,
        &mut blockers,
    );
    validate_scheduled_owner_map(
        "source.debug.scheduled_source_groups_by_node",
        &status_group_map(source_debug, "scheduled_source_groups_by_node", status)?,
        expected,
        &active_nodes,
        &mut blockers,
    );
    validate_scheduled_owner_map(
        "source.debug.scheduled_scan_groups_by_node",
        &status_group_map(source_debug, "scheduled_scan_groups_by_node", status)?,
        expected,
        &active_nodes,
        &mut blockers,
    );
    validate_scheduled_owner_map(
        "sink.debug.scheduled_groups_by_node",
        &status_group_map(sink_debug, "scheduled_groups_by_node", status)?,
        expected,
        &active_nodes,
        &mut blockers,
    );

    if blockers.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "active-three status ownership not converged: {}; status={}",
            blockers.join("; "),
            status
        ))
    }
}

fn validate_active_three_status_materialization(
    status: &Value,
    expected: &BTreeMap<String, String>,
) -> Result<(), String> {
    let mut blockers = Vec::new();
    let active_nodes = expected.values().cloned().collect::<BTreeSet<_>>();

    let readiness_planes = status
        .get("readiness_planes")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("status.readiness_planes missing object: {status}"))?;
    if readiness_planes
        .get("trusted_observation_readiness")
        .and_then(Value::as_bool)
        != Some(true)
    {
        blockers.push("trusted_observation_readiness is not true".to_string());
    }

    validate_active_three_source_audit_complete(status, expected, &mut blockers)?;
    validate_active_three_sink_materialized(status, expected, &mut blockers)?;
    validate_active_three_publish_receive_transport(
        status,
        expected,
        &active_nodes,
        &mut blockers,
    )?;

    if blockers.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "active-three materialization not converged: {}; summary={}",
            blockers.join("; "),
            active_three_status_debug_summary(status)
        ))
    }
}

fn validate_active_three_source_audit_complete(
    status: &Value,
    expected: &BTreeMap<String, String>,
    blockers: &mut Vec<String>,
) -> Result<(), String> {
    let roots = status
        .get("source")
        .and_then(|source| source.get("concrete_roots"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("status.source.concrete_roots missing array: {status}"))?;
    let mut primaries_by_group = BTreeMap::<String, Vec<&Value>>::new();
    for root in roots {
        let Some(group_id) = root.get("logical_root_id").and_then(Value::as_str) else {
            continue;
        };
        if !expected.contains_key(group_id) {
            continue;
        }
        if root
            .get("is_group_primary")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            primaries_by_group
                .entry(group_id.to_string())
                .or_default()
                .push(root);
        }
    }

    for (group, expected_node) in expected {
        let Some(roots) = primaries_by_group.get(group) else {
            blockers.push(format!("source concrete root {group} missing primary"));
            continue;
        };
        if roots.len() != 1 {
            blockers.push(format!(
                "source concrete root {group} expected one primary, got {}",
                roots.len()
            ));
            continue;
        }
        let root = roots[0];
        let object_ref = root
            .get("object_ref")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        if !host_ref_matches_expected_node(object_ref, expected_node) {
            blockers.push(format!(
                "source concrete root {group} expected owner {expected_node}, got {object_ref}"
            ));
        }
        if root.get("active").and_then(Value::as_bool) != Some(true) {
            blockers.push(format!("source concrete root {group} is not active"));
        }
        if root.get("scan_enabled").and_then(Value::as_bool) != Some(true) {
            blockers.push(format!("source concrete root {group} scan is not enabled"));
        }
        if root.get("participation_state").and_then(Value::as_str) != Some("serving") {
            blockers.push(format!(
                "source concrete root {group} participation_state is {:?}",
                root.get("participation_state")
            ));
        }
        let started = root.get("last_audit_started_at_us").and_then(Value::as_u64);
        let completed = root
            .get("last_audit_completed_at_us")
            .and_then(Value::as_u64);
        match (started, completed) {
            (Some(started), Some(completed)) if completed >= started => {}
            (Some(started), Some(completed)) => blockers.push(format!(
                "source concrete root {group} audit completion {completed} is before start {started}"
            )),
            (started, completed) => blockers.push(format!(
                "source concrete root {group} audit incomplete started={started:?} completed={completed:?}"
            )),
        }
    }

    Ok(())
}

fn validate_active_three_sink_materialized(
    status: &Value,
    expected: &BTreeMap<String, String>,
    blockers: &mut Vec<String>,
) -> Result<(), String> {
    let groups = status
        .get("sink")
        .and_then(|sink| sink.get("groups"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("status.sink.groups missing array: {status}"))?;
    let groups_by_id = groups
        .iter()
        .filter_map(|group| {
            group
                .get("group_id")
                .and_then(Value::as_str)
                .map(|group_id| (group_id.to_string(), group))
        })
        .collect::<BTreeMap<_, _>>();

    for (group_id, expected_node) in expected {
        let Some(group) = groups_by_id.get(group_id) else {
            blockers.push(format!("sink group {group_id} missing"));
            continue;
        };
        let primary = group
            .get("primary_object_ref")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        if !host_ref_matches_expected_node(primary, expected_node) {
            blockers.push(format!(
                "sink group {group_id} expected primary owner {expected_node}, got {primary}"
            ));
        }
        if group
            .get("initial_audit_completed")
            .and_then(Value::as_bool)
            != Some(true)
        {
            blockers.push(format!(
                "sink group {group_id} initial audit is not complete"
            ));
        }
        if group
            .get("materialization_readiness")
            .and_then(Value::as_str)
            != Some("ready")
        {
            blockers.push(format!(
                "sink group {group_id} materialization_readiness is {:?}",
                group.get("materialization_readiness")
            ));
        }
        if group.get("live_nodes").and_then(Value::as_u64).unwrap_or(0) == 0 {
            blockers.push(format!("sink group {group_id} has zero live_nodes"));
        }
    }

    Ok(())
}

fn validate_active_three_publish_receive_transport(
    status: &Value,
    expected: &BTreeMap<String, String>,
    active_nodes: &BTreeSet<String>,
    blockers: &mut Vec<String>,
) -> Result<(), String> {
    let source_debug = status
        .get("source")
        .and_then(|source| source.get("debug"))
        .ok_or_else(|| format!("status.source.debug missing: {status}"))?;
    let sink_debug = status
        .get("sink")
        .and_then(|sink| sink.get("debug"))
        .ok_or_else(|| format!("status.sink.debug missing: {status}"))?;

    let published_events = status_count_map(source_debug, "published_events_by_node", status)?;
    let published_batches = status_count_map(source_debug, "published_batches_by_node", status)?;
    let received_events = status_count_map(sink_debug, "received_events_by_node", status)?;
    let received_batches = status_count_map(sink_debug, "received_batches_by_node", status)?;

    let total_published_events = published_events.values().sum::<u64>();
    let total_received_events = received_events.values().sum::<u64>();
    if total_published_events > 0 && total_received_events == 0 {
        blockers.push(format!(
            "source published {total_published_events} events but sink received zero events"
        ));
    }

    for node in active_nodes {
        let source_events = published_events.get(node).copied().unwrap_or(0);
        let source_batches = published_batches.get(node).copied().unwrap_or(0);
        if source_events == 0 || source_batches == 0 {
            blockers.push(format!(
                "source node {node} has insufficient published transport evidence events={source_events} batches={source_batches}"
            ));
        }
        let sink_events = received_events.get(node).copied().unwrap_or(0);
        let sink_batches = received_batches.get(node).copied().unwrap_or(0);
        if sink_events == 0 || sink_batches == 0 {
            blockers.push(format!(
                "sink node {node} has insufficient received transport evidence events={sink_events} batches={sink_batches}"
            ));
        }
    }

    let published_nodes = published_events
        .iter()
        .filter(|(node, count)| active_nodes.contains(*node) && **count > 0)
        .count();
    let received_nodes = received_events
        .iter()
        .filter(|(node, count)| active_nodes.contains(*node) && **count > 0)
        .count();
    if published_nodes != expected.len() || received_nodes != expected.len() {
        blockers.push(format!(
            "transport node coverage incomplete published_nodes={published_nodes} received_nodes={received_nodes} expected={}",
            expected.len()
        ));
    }

    Ok(())
}

fn active_three_status_debug_summary(status: &Value) -> Value {
    json!({
        "trusted_observation_readiness": status
            .pointer("/readiness_planes/trusted_observation_readiness")
            .cloned()
            .unwrap_or(Value::Null),
        "source_roots": status
            .pointer("/source/concrete_roots")
            .and_then(Value::as_array)
            .map(|roots| {
                roots
                    .iter()
                    .filter_map(|root| {
                        let group = root.get("logical_root_id").and_then(Value::as_str)?;
                        if !ACTIVE_THREE_EXPORTS.contains(&group) {
                            return None;
                        }
                        Some(json!({
                            "group": group,
                            "object_ref": root.get("object_ref").cloned().unwrap_or(Value::Null),
                            "active": root.get("active").cloned().unwrap_or(Value::Null),
                            "scan_enabled": root.get("scan_enabled").cloned().unwrap_or(Value::Null),
                            "is_group_primary": root.get("is_group_primary").cloned().unwrap_or(Value::Null),
                            "last_audit_started_at_us": root.get("last_audit_started_at_us").cloned().unwrap_or(Value::Null),
                            "last_audit_completed_at_us": root.get("last_audit_completed_at_us").cloned().unwrap_or(Value::Null),
                            "emitted_event_count": root.get("emitted_event_count").cloned().unwrap_or(Value::Null),
                            "forwarded_event_count": root.get("forwarded_event_count").cloned().unwrap_or(Value::Null),
                        }))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        "sink_groups": status
            .pointer("/sink/groups")
            .and_then(Value::as_array)
            .map(|groups| {
                groups
                    .iter()
                    .filter_map(|group| {
                        let group_id = group.get("group_id").and_then(Value::as_str)?;
                        if !ACTIVE_THREE_EXPORTS.contains(&group_id) {
                            return None;
                        }
                        Some(json!({
                            "group": group_id,
                            "primary_object_ref": group.get("primary_object_ref").cloned().unwrap_or(Value::Null),
                            "live_nodes": group.get("live_nodes").cloned().unwrap_or(Value::Null),
                            "total_nodes": group.get("total_nodes").cloned().unwrap_or(Value::Null),
                            "initial_audit_completed": group.get("initial_audit_completed").cloned().unwrap_or(Value::Null),
                            "materialization_readiness": group.get("materialization_readiness").cloned().unwrap_or(Value::Null),
                        }))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        "source_published_events_by_node": status
            .pointer("/source/debug/published_events_by_node")
            .cloned()
            .unwrap_or(Value::Null),
        "sink_received_events_by_node": status
            .pointer("/sink/debug/received_events_by_node")
            .cloned()
            .unwrap_or(Value::Null),
    })
}

fn active_three_expected_owner_by_group(
    cluster: &Cluster5,
) -> Result<BTreeMap<String, String>, String> {
    ACTIVE_THREE_MOUNTS
        .iter()
        .map(|(node, group)| {
            cluster
                .node_id(node)
                .map(|node_id| ((*group).to_string(), node_id))
        })
        .collect()
}

fn status_string_map(
    section: &Value,
    key: &str,
    status: &Value,
) -> Result<BTreeMap<String, String>, String> {
    let object = section
        .get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("status field {key} missing object value: {status}"))?;
    let mut out = BTreeMap::new();
    for (entry_key, value) in object {
        let Some(value) = value.as_str() else {
            return Err(format!(
                "status field {key}.{entry_key} missing string value: {status}"
            ));
        };
        out.insert(entry_key.clone(), value.to_string());
    }
    Ok(out)
}

fn status_group_map(
    section: &Value,
    key: &str,
    status: &Value,
) -> Result<BTreeMap<String, Vec<String>>, String> {
    let object = section
        .get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("status field {key} missing object value: {status}"))?;
    let mut out = BTreeMap::new();
    for (node, groups) in object {
        let Some(groups) = groups.as_array() else {
            return Err(format!(
                "status field {key}.{node} missing array value: {status}"
            ));
        };
        let mut parsed = Vec::with_capacity(groups.len());
        for group in groups {
            let Some(group) = group.as_str() else {
                return Err(format!(
                    "status field {key}.{node} contains non-string group: {status}"
                ));
            };
            parsed.push(group.to_string());
        }
        parsed.sort();
        parsed.dedup();
        out.insert(node.clone(), parsed);
    }
    Ok(out)
}

fn status_count_map(
    section: &Value,
    key: &str,
    status: &Value,
) -> Result<BTreeMap<String, u64>, String> {
    let object = section
        .get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("status field {key} missing object value: {status}"))?;
    let mut out = BTreeMap::new();
    for (entry_key, value) in object {
        let Some(value) = value.as_u64() else {
            return Err(format!(
                "status field {key}.{entry_key} missing u64 value: {status}"
            ));
        };
        out.insert(entry_key.clone(), value);
    }
    Ok(out)
}

fn validate_primary_owner_map(
    label: &str,
    actual: &BTreeMap<String, String>,
    expected: &BTreeMap<String, String>,
    blockers: &mut Vec<String>,
) {
    for (group, expected_node) in expected {
        match actual.get(group) {
            Some(owner) if host_ref_matches_expected_node(owner, expected_node) => {}
            Some(owner) => blockers.push(format!(
                "{label}.{group} expected owner {expected_node}, got {owner}"
            )),
            None => blockers.push(format!("{label}.{group} missing owner")),
        }
    }
}

fn validate_scheduled_owner_map(
    label: &str,
    actual: &BTreeMap<String, Vec<String>>,
    expected: &BTreeMap<String, String>,
    active_nodes: &BTreeSet<String>,
    blockers: &mut Vec<String>,
) {
    let mut owners_by_group = BTreeMap::<String, Vec<String>>::new();
    for (node, groups) in actual {
        let active_groups = groups
            .iter()
            .filter(|group| expected.contains_key(*group))
            .cloned()
            .collect::<Vec<_>>();
        if active_groups.is_empty() {
            continue;
        }
        if !active_nodes.contains(node) {
            blockers.push(format!(
                "{label}.{node} is inactive but claims active groups {active_groups:?}"
            ));
        }
        if active_groups.len() == expected.len() && expected.len() > 1 {
            blockers.push(format!(
                "{label}.{node} claims all active groups {active_groups:?}"
            ));
        }
        for group in active_groups {
            if expected.get(&group) != Some(node) {
                let expected_node = expected.get(&group).map(String::as_str).unwrap_or("<none>");
                blockers.push(format!(
                    "{label}.{group} scheduled on {node}, expected {expected_node}"
                ));
            }
            owners_by_group.entry(group).or_default().push(node.clone());
        }
    }

    for (group, expected_node) in expected {
        match owners_by_group.get(group) {
            Some(nodes) if nodes.len() == 1 && nodes.first() == Some(expected_node) => {}
            Some(nodes) => blockers.push(format!(
                "{label}.{group} expected unique owner {expected_node}, got {nodes:?}"
            )),
            None => blockers.push(format!("{label}.{group} missing scheduled owner")),
        }
    }
}

fn host_ref_matches_expected_node(owner: &str, expected_node: &str) -> bool {
    owner == expected_node
        || owner
            .strip_prefix(expected_node)
            .is_some_and(|suffix| suffix.starts_with("::"))
}

fn collect_active_three_evidence(cluster: &Cluster5, app_id: &str) -> Result<Value, String> {
    let mut nodes = Vec::new();
    for node in &cluster.nodes {
        let registry = read_json_or_empty(&node.home_dir.join("registry.json"));
        let current_entries = registry_entry_count(&registry, "current_entries", app_id);
        let draining_entries = registry_entry_count(&registry, "draining_entries", app_id);
        let descendants = cluster
            .daemon_host_descendant_pids(&node.name)
            .map(|pids| pids.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let mut worker_host_pids = Vec::new();
        let mut fs_meta_cmd_pids = Vec::new();
        for pid in descendants {
            let cmdline = proc_cmdline(pid);
            if cmdline.contains("capanix_worker_host") {
                worker_host_pids.push(pid);
            }
            if cmdline.contains("libfs_meta_runtime") || cmdline.contains("fs-meta-runtime") {
                fs_meta_cmd_pids.push(pid);
            }
        }
        nodes.push(json!({
            "node": node.name,
            "home_dir": node.home_dir.display().to_string(),
            "registry_current_entries": current_entries,
            "registry_draining_entries": draining_entries,
            "worker_host_pids": worker_host_pids,
            "fs_meta_cmd_pids": fs_meta_cmd_pids,
            "log_counts": count_log_patterns(&node.home_dir),
        }));
    }
    Ok(json!({
        "app_id": app_id,
        "nodes": nodes,
    }))
}

fn read_json_or_empty(path: &std::path::Path) -> Value {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .unwrap_or_else(|| json!({}))
}

fn registry_entry_count(registry: &Value, field: &str, app_id: &str) -> usize {
    registry
        .get(field)
        .and_then(Value::as_object)
        .map(|entries| entries.keys().filter(|key| key.as_str() == app_id).count())
        .unwrap_or(0)
}

fn proc_cmdline(pid: u32) -> String {
    fs::read(format!("/proc/{pid}/cmdline"))
        .map(|raw| {
            raw.split(|byte| *byte == 0)
                .filter(|part| !part.is_empty())
                .map(|part| String::from_utf8_lossy(part).to_string())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default()
}

fn count_log_patterns(root: &std::path::Path) -> Value {
    let patterns = [
        "operation timed out",
        "worker sidecar bind failed",
        "statecell_read failed",
        "drained/fenced",
        "route activation backpressure",
        "sidecar_control_bridge",
        "ipc_read_len: early eof",
    ];
    let mut counts = serde_json::Map::new();
    for pattern in patterns {
        counts.insert(pattern.to_string(), json!(0_u64));
    }
    count_log_patterns_recursive(root, &patterns, &mut counts);
    Value::Object(counts)
}

fn count_log_patterns_recursive(
    root: &std::path::Path,
    patterns: &[&str],
    counts: &mut serde_json::Map<String, Value>,
) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            count_log_patterns_recursive(&path, patterns, counts);
            continue;
        }
        let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
            continue;
        };
        if !matches!(ext, "log" | "out" | "err") {
            continue;
        }
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
        };
        for pattern in patterns {
            let add = raw.matches(pattern).count() as u64;
            if add == 0 {
                continue;
            }
            let current = counts.get(*pattern).and_then(Value::as_u64).unwrap_or(0);
            counts.insert((*pattern).to_string(), json!(current + add));
        }
    }
}

fn reject_active_three_blocker_evidence(evidence: &Value) -> Result<(), String> {
    let mut blockers = Vec::new();
    let Some(nodes) = evidence.get("nodes").and_then(Value::as_array) else {
        return Err(format!("active-three evidence missing nodes: {evidence}"));
    };
    for node in nodes {
        let name = node
            .get("node")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        let current = node
            .get("registry_current_entries")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let draining = node
            .get("registry_draining_entries")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let worker_hosts = node
            .get("worker_host_pids")
            .and_then(Value::as_array)
            .map(|pids| pids.len())
            .unwrap_or(0);
        let active = matches!(name, "node-a" | "node-b" | "node-c");
        if active && current != 1 {
            blockers.push(format!(
                "{name}: expected one current runtime, got {current}"
            ));
        }
        if !active && current != 0 {
            blockers.push(format!(
                "{name}: inactive node unexpectedly has current runtime count {current}"
            ));
        }
        if draining > 0 {
            blockers.push(format!(
                "{name}: draining runtime entries present count={draining}"
            ));
        }
        if active && worker_hosts > 2 {
            blockers.push(format!(
                "{name}: expected at most source+sink worker hosts, got {worker_hosts}"
            ));
        }
        if let Some(log_counts) = node.get("log_counts").and_then(Value::as_object) {
            for pattern in [
                "operation timed out",
                "worker sidecar bind failed",
                "statecell_read failed",
                "route activation backpressure",
            ] {
                let count = log_counts.get(pattern).and_then(Value::as_u64).unwrap_or(0);
                if count > 0 {
                    blockers.push(format!("{name}: log pattern `{pattern}` count={count}"));
                }
            }
        }
    }
    if blockers.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "active-three replay saw blocker evidence: {}; evidence={}",
            blockers.join("; "),
            evidence
        ))
    }
}
