#![cfg(target_os = "linux")]

use crate::support::api_client::{ApiResponse, FsMetaApiClient, OperatorSession};
use crate::support::cluster5::Cluster5;
use crate::support::nfs_lab::NfsLab;
use crate::support::oracle::FsTreeOracle;
use crate::support::{reserve_http_addrs, skip_unless_real_nfs_enabled, wait_until};
use fs_meta::{RootSelector, RootSpec};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatrixMode {
    Full,
    QueryBaselineOnly,
    LiveOnlyOnly,
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
}

const MINI_EXPORTS: [&str; 5] = [
    "mini_nfs1",
    "mini_nfs2",
    "mini_nfs3",
    "mini_nfs4",
    "mini_nfs5",
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
    session.rescan()?;

    eprintln!("[fs-meta-api-matrix-mini] step=status-and-grants");
    run_mini_status_and_grants_checks(&harness.client, &mut session, &harness.lab, 1)?;
    eprintln!("[fs-meta-api-matrix-mini] step=management-roots");
    run_mini_roots_management_smoke(&mut session, &harness.lab)?;
    eprintln!("[fs-meta-api-matrix-mini] step=query-and-key-api");
    run_mini_query_and_key_smoke(&mut session, &harness.lab)?;
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
    session.rescan()?;

    match mode {
        MatrixMode::Full => {
            eprintln!("[fs-meta-api-matrix] step=status-and-grants");
            run_status_and_grants_checks(
                &harness.client,
                &mut session,
                &harness.lab,
                harness.facade_count,
                true,
                false,
            )?;
            eprintln!("[fs-meta-api-matrix] step=roots-matrix");
            run_roots_matrix(&mut session, &harness.lab)?;
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
                &harness.lab,
                harness.facade_count,
                true,
                false,
            )?;
            eprintln!("[fs-meta-api-matrix] step=roots-matrix");
            run_roots_matrix(&mut session, &harness.lab)?;
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
                &harness.lab,
                harness.facade_count,
                true,
                false,
            )?;
        }
    }

    if matches!(mode, MatrixMode::LiveOnlyOnly) {
        eprintln!("[fs-meta-api-matrix] step=query-live-only");
        run_query_live_only_rescan_phase(&harness.cluster, &mut session, &harness.lab)?;
    }

    Ok(())
}

fn build_matrix_harness(app_prefix: &str) -> Result<MatrixHarness, String> {
    let mut lab = NfsLab::start()?;
    seed_baseline_content(&lab)?;
    let cluster = Cluster5::start()?;
    let app_id = format!("{app_prefix}-{}", unique_suffix());
    let facade_resource_id = format!("fs-meta-tcp-listener-{app_id}");
    let facade_addrs = reserve_http_addrs(1)?;
    install_baseline_resources(&cluster, &mut lab, &facade_resource_id, &facade_addrs)?;

    let roots = baseline_roots(&lab);
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

    Ok(MatrixHarness {
        cluster,
        lab,
        client,
        candidate_base_urls,
        facade_count: facade_addrs.len(),
    })
}

fn run_query_matrix(
    _cluster: &Cluster5,
    session: &mut OperatorSession,
    _lab: &NfsLab,
) -> Result<(), String> {
    eprintln!("[fs-meta-api-matrix] substep=query-core-readiness-gate");
    assert_error(
        session.client().tree_raw(
            session.query_api_key(),
            &[("path", "/".to_string()), ("recursive", "true".to_string())],
        )?,
        503,
        "trusted-materialized reads remain unavailable",
    )?;
    assert_error(
        session.client().force_find_raw(
            session.query_api_key(),
            &[
                ("path", "/".to_string()),
                ("recursive", "true".to_string()),
                ("read_class", "trusted-materialized".to_string()),
            ],
        )?,
        400,
        "read_class must be fresh on /on-demand-force-find",
    )?;

    eprintln!("[fs-meta-api-matrix] substep=query-api-key-management");
    let keys_before = session.client().list_query_api_keys(session.token())?;
    let created = session
        .client()
        .create_query_api_key(session.token(), "full-core-extra-key")?;
    let api_key = created
        .get("api_key")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("query api key response missing api_key: {created}"))?;
    let key_id = created
        .get("key")
        .and_then(|key| key.get("key_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| format!("query api key response missing key.key_id: {created}"))?;
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

fn run_full_query_capacity_baseline_phase(session: &mut OperatorSession) -> Result<(), String> {
    eprintln!("[fs-meta-api-matrix] substep=full-capacity-trusted-materialized-gate");
    session.rescan()?;
    assert_error(
        session.client().tree_raw(
            session.query_api_key(),
            &[("path", "/".to_string()), ("recursive", "true".to_string())],
        )?,
        503,
        "trusted-materialized reads remain unavailable",
    )?;
    let status = session.status()?;
    if status
        .pointer("/readiness_planes/trusted_observation_readiness")
        .and_then(Value::as_bool)
        != Some(false)
    {
        return Err(format!(
            "full capacity baseline must not report trusted observation readiness before full audit completes: {status}"
        ));
    }
    assert_error(
        session.client().force_find_raw(
            session.query_api_key(),
            &[
                ("path", "/".to_string()),
                ("recursive", "true".to_string()),
                ("read_class", "trusted-materialized".to_string()),
            ],
        )?,
        400,
        "read_class must be fresh on /on-demand-force-find",
    )?;
    Ok(())
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

    session.rescan()?;
    let post_rescan_status = session.status()?;
    validate_status_monitoring_shape(&post_rescan_status, true)?;

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
    lab: &NfsLab,
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
                < 3
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
        < 3
    {
        return Err(format!("expected at least 3 grants in status: {status}"));
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
    let expected_total = 9 + facade_count;
    if nfs_rows != 9 || listener_rows != facade_count || rows.len() != expected_total {
        return Err(format!(
            "expected {expected_total} runtime grants rows (9 nfs + {facade_count} tcp_listener), got {} total / {} nfs / {} listeners: {grants}",
            rows.len(),
            nfs_rows,
            listener_rows
        ));
    }
    for export_name in ["nfs1", "nfs2", "nfs3"] {
        if !rows.iter().any(|row| {
            row.get("fs_source").and_then(Value::as_str)
                == Some(lab.export_source(export_name).as_str())
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
            .get("trusted_observation_readiness")
            .and_then(Value::as_bool)
            != Some(true)
    {
        return Err(format!(
            "mini smoke requires all readiness planes ready: {status}"
        ));
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

fn run_roots_matrix(session: &mut OperatorSession, lab: &NfsLab) -> Result<(), String> {
    eprintln!("[fs-meta-api-matrix] substep=monitoring-roots-current");
    let current = session.monitoring_roots()?;
    let current_rows = current
        .get("roots")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("monitoring_roots missing roots array: {current}"))?;
    if current_rows.len() != 3 {
        return Err(format!("expected 3 roots from release doc, got {current}"));
    }

    let empty_preview = session
        .client()
        .preview_roots_raw(session.token(), &json!([]))?;
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

    let roots = baseline_roots(lab);
    let preview = session
        .client()
        .preview_roots_raw(session.token(), &roots_payload(&roots))?;
    assert_status(preview.status, 200, "baseline roots preview")?;
    if preview
        .body
        .get("preview")
        .and_then(Value::as_array)
        .map(|items| items.len())
        != Some(3)
    {
        return Err(format!("expected 3 preview rows: {}", preview.body));
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
        session.client().preview_roots_raw(
            session.token(),
            &json!([{
                "id": "",
                "selector": { "fs_source": lab.export_source("nfs1") },
                "subpath_scope": "/",
                "watch": true,
                "scan": true
            }]),
        )?,
        400,
        "roots[].id must not be empty",
    )?;
    assert_error(
        session.client().preview_roots_raw(
            session.token(),
            &json!([{
                "id": "bad-subpath",
                "selector": { "fs_source": lab.export_source("nfs1") },
                "subpath_scope": "relative",
                "watch": true,
                "scan": true
            }]),
        )?,
        400,
        "subpath_scope",
    )?;
    assert_error(
        session.client().preview_roots_raw(
            session.token(),
            &json!([{
                "id": "missing-selector",
                "selector": {},
                "subpath_scope": "/",
                "watch": true,
                "scan": true
            }]),
        )?,
        400,
        "selector",
    )?;
    assert_error(
        session.client().preview_roots_raw(
            session.token(),
            &json!([{
                "id": "no-watch-scan",
                "selector": { "fs_source": lab.export_source("nfs1") },
                "subpath_scope": "/",
                "watch": false,
                "scan": false
            }]),
        )?,
        400,
        "watch",
    )?;
    assert_error(
        session.client().preview_roots_raw(
            session.token(),
            &json!([{
                "id": "legacy-path",
                "path": "/legacy",
                "selector": { "fs_source": lab.export_source("nfs1") },
                "subpath_scope": "/",
                "watch": true,
                "scan": true
            }]),
        )?,
        400,
        "roots[].path is forbidden",
    )?;
    assert_error(
        session.client().preview_roots_raw(
            session.token(),
            &json!([{
                "id": "legacy-source-locator",
                "source_locator": "legacy://nfs1",
                "selector": { "fs_source": lab.export_source("nfs1") },
                "subpath_scope": "/",
                "watch": true,
                "scan": true
            }]),
        )?,
        400,
        "roots[].source_locator is forbidden",
    )?;
    assert_error(
        session.client().update_roots_raw(
            session.token(),
            &json!([
                {
                    "id": "dup",
                    "selector": { "fs_source": lab.export_source("nfs1") },
                    "subpath_scope": "/",
                    "watch": true,
                    "scan": true
                },
                {
                    "id": "dup",
                    "selector": { "fs_source": lab.export_source("nfs2") },
                    "subpath_scope": "/",
                    "watch": true,
                    "scan": true
                }
            ]),
        )?,
        400,
        "duplicate",
    )?;

    eprintln!("[fs-meta-api-matrix] substep=single-root-apply");
    let single_root = vec![root_spec("nfs1", &lab.export_source("nfs1"))];
    let put = session
        .client()
        .update_roots_raw(session.token(), &roots_payload(&single_root))?;
    assert_status(put.status, 200, "single-root apply")?;
    if put.body.get("roots_count").and_then(Value::as_u64) != Some(1) {
        return Err(format!("single-root apply did not converge: {}", put.body));
    }
    session.rescan()?;
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
    let restore = session
        .client()
        .update_roots_raw(session.token(), &roots_payload(&roots))?;
    assert_status(restore.status, 200, "restore roots")?;
    session.rescan()?;
    wait_until(Duration::from_secs(30), "restore baseline roots", || {
        let roots = session.monitoring_roots()?;
        Ok(roots
            .get("roots")
            .and_then(Value::as_array)
            .map(|items| items.len() == 3)
            .unwrap_or(false))
    })?;
    Ok(())
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
    let preview = session
        .client()
        .preview_roots_raw(session.token(), &roots_payload(&roots))?;
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

    let put = session
        .client()
        .update_roots_raw(session.token(), &roots_payload(&roots))?;
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
        "mini tree materializes all roots",
        || {
            let tree =
                session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
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

    let tree = session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    let expected_tree =
        FsTreeOracle::grouped_tree_response(&all_mounts, "/", true, None, 1_000, "group-key", 64)?;
    assert_json_eq("mini all groups tree", &tree, &expected_tree)?;

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

    let stats = session.stats(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    let expected_stats = FsTreeOracle::stats_response(&all_mounts, "/", true, None)?;
    assert_json_eq("mini all groups stats", &stats, &expected_stats)?;

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

fn install_baseline_resources(
    cluster: &Cluster5,
    lab: &mut NfsLab,
    facade_resource_id: &str,
    facade_addrs: &[String],
) -> Result<(), String> {
    let exports = [
        ("node-a", "nfs1"),
        ("node-b", "nfs1"),
        ("node-c", "nfs1"),
        ("node-a", "nfs2"),
        ("node-c", "nfs2"),
        ("node-d", "nfs2"),
        ("node-b", "nfs3"),
        ("node-d", "nfs3"),
        ("node-e", "nfs3"),
    ];
    for (node_name, export_name) in exports {
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

fn assert_baseline_mounts_live(lab: &NfsLab) {
    for (node_name, export_name) in [
        ("node-a", "nfs1"),
        ("node-b", "nfs1"),
        ("node-c", "nfs1"),
        ("node-a", "nfs2"),
        ("node-c", "nfs2"),
        ("node-d", "nfs2"),
        ("node-b", "nfs3"),
        ("node-d", "nfs3"),
        ("node-e", "nfs3"),
    ] {
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

    let mut lab = NfsLab::start().expect("start NFS lab");
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
                    groups.len() >= 3
                        && group_total_nodes(&tree, "nfs1") > 0
                        && group_total_nodes(&tree, "nfs2") > 0
                        && group_total_nodes(&tree, "nfs3") > 0
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
    vec![
        root_spec("nfs1", &lab.export_source("nfs1")),
        root_spec("nfs2", &lab.export_source("nfs2")),
        root_spec("nfs3", &lab.export_source("nfs3")),
    ]
}

fn mini_roots(lab: &NfsLab) -> Vec<RootSpec> {
    MINI_EXPORTS
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

fn roots_payload(roots: &[RootSpec]) -> Value {
    Value::Array(
        roots
            .iter()
            .map(|root| {
                json!({
                    "id": root.id,
                    "selector": {
                        "fs_source": root.selector.fs_source,
                        "mount_point": root.selector.mount_point.as_ref().map(|p| p.display().to_string()),
                        "fs_type": root.selector.fs_type,
                        "host_ip": root.selector.host_ip,
                        "host_ref": root.selector.host_ref,
                    },
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

fn assert_error(response: ApiResponse, expected_status: u16, needle: &str) -> Result<(), String> {
    assert_status(response.status, expected_status, "error response")?;
    let body = response.body.to_string();
    if !body.contains(needle) {
        return Err(format!(
            "expected response body to contain '{needle}', got {}",
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
