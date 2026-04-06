#![cfg(target_os = "linux")]

use crate::support::api_client::{ApiResponse, FsMetaApiClient, OperatorSession};
use crate::support::cluster5::Cluster5;
use crate::support::nfs_lab::NfsLab;
use crate::support::oracle::FsTreeOracle;
use crate::support::{reserve_http_addrs, skip_unless_real_nfs_enabled, wait_until};
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use base64::Engine;
use fs_meta::{RootSelector, RootSpec};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
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

pub fn run() -> Result<(), String> {
    run_mode(MatrixMode::Full)
}

pub fn run_query_baseline_only() -> Result<(), String> {
    run_mode(MatrixMode::QueryBaselineOnly)
}

pub fn run_live_only_rescan_only() -> Result<(), String> {
    run_mode(MatrixMode::LiveOnlyOnly)
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
                false,
            )?;
            eprintln!("[fs-meta-api-matrix] step=roots-matrix");
            run_roots_matrix(&mut session, &harness.lab)?;
            eprintln!("[fs-meta-api-matrix] step=query-matrix");
            run_query_baseline_phase(&harness.cluster, &mut session, &harness.lab)?;
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

fn run_query_matrix(
    cluster: &Cluster5,
    session: &mut OperatorSession,
    lab: &NfsLab,
) -> Result<(), String> {
    run_query_baseline_phase(cluster, session, lab)?;
    run_query_live_only_rescan_phase(cluster, session, lab)?;
    Ok(())
}

fn run_query_baseline_phase(
    cluster: &Cluster5,
    session: &mut OperatorSession,
    lab: &NfsLab,
) -> Result<(), String> {
    eprintln!("[fs-meta-api-matrix] substep=query-matrix-rescan");
    session.rescan()?;
    let mut last_rescan_at = std::time::Instant::now();
    wait_until(
        Duration::from_secs(300),
        "baseline tree materializes root data",
        || {
            if last_rescan_at.elapsed() >= Duration::from_secs(15) {
                let _ = session.rescan();
                last_rescan_at = std::time::Instant::now();
            }
            let tree = match session
                .tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])
            {
                Ok(tree) => tree,
                Err(err) => {
                    let status = session
                        .status()
                        .unwrap_or_else(|status_err| json!({ "status_error": status_err }));
                    let roots = session
                        .monitoring_roots()
                        .unwrap_or_else(|roots_err| json!({ "roots_error": roots_err }));
                    return Err(format!(
                        "tree request failed: {err}; status={status}; roots={roots}"
                    ));
                }
            };
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
                let diagnostics =
                    baseline_cluster_diagnostics(cluster, session).unwrap_or_else(|diag_err| {
                        json!({ "diagnostics_error": diag_err }).to_string()
                    });
                Err(format!(
                    "tree={tree}; status={status}; diagnostics={diagnostics}"
                ))
            }
        },
    )?;

    let grants = session.runtime_grants()?;
    let all_mounts = group_mount_pairs_for_roots(
        &grants,
        &[
            ("nfs1", &lab.export_source("nfs1")),
            ("nfs2", &lab.export_source("nfs2")),
            ("nfs3", &lab.export_source("nfs3")),
        ],
    )?;

    eprintln!("[fs-meta-api-matrix] substep=query-all-tree");
    let all_tree = session.tree(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    let all_groups = all_tree
        .get("groups")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("tree response missing groups: {all_tree}"))?;
    if all_groups.len() < 3 {
        return Err(format!(
            "expected at least 3 groups in baseline tree: {all_tree}"
        ));
    }
    let expected_all_tree =
        FsTreeOracle::grouped_tree_response(&all_mounts, "/", true, None, 1_000, "group-key", 64)?;
    assert_json_eq("all groups tree", &all_tree, &expected_all_tree)?;

    eprintln!("[fs-meta-api-matrix] substep=query-non-recursive-tree");
    let tree_non_recursive = session.tree(&[
        ("path", "/data".to_string()),
        ("recursive", "false".to_string()),
    ])?;
    let expected_non_recursive = FsTreeOracle::grouped_tree_response(
        &all_mounts,
        "/data",
        false,
        None,
        1_000,
        "group-key",
        64,
    )?;
    assert_json_eq(
        "non-recursive tree",
        &tree_non_recursive,
        &expected_non_recursive,
    )?;

    eprintln!("[fs-meta-api-matrix] substep=query-max-depth-tree");
    let tree_max_depth = session.tree(&[
        ("path", "/nested".to_string()),
        ("recursive", "true".to_string()),
        ("max_depth", "1".to_string()),
    ])?;
    let expected_max_depth = FsTreeOracle::grouped_tree_response(
        &all_mounts,
        "/nested",
        true,
        Some(1),
        1_000,
        "group-key",
        64,
    )?;
    assert_json_eq("max_depth tree", &tree_max_depth, &expected_max_depth)?;

    eprintln!("[fs-meta-api-matrix] substep=query-force-find-all");
    let find_all =
        session.force_find(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    let expected_find_all = FsTreeOracle::grouped_force_find_response(
        &all_mounts,
        "/",
        true,
        None,
        1_000,
        "group-key",
        64,
    )?;
    assert_json_eq("all groups force-find", &find_all, &expected_find_all)?;

    let best_count = session.tree(&[
        ("path", "/".to_string()),
        ("group_order", "file-count".to_string()),
        ("group_page_size", "1".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let best_count_group = FsTreeOracle::best_group_by_count(&all_mounts, "/", true, None)?
        .ok_or_else(|| "group_order=file-count oracle returned no winner".to_string())?;
    let actual_best_count_group = first_group_key(&best_count)?;
    if actual_best_count_group != best_count_group {
        return Err(format!(
            "group_order=file-count winner mismatch: expected {best_count_group}, got {actual_best_count_group}; payload={best_count}"
        ));
    }

    let best_age = session.tree(&[
        ("path", "/".to_string()),
        ("group_order", "file-age".to_string()),
        ("group_page_size", "1".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let best_age_group = FsTreeOracle::best_group_by_age(&all_mounts, "/", true, None)?
        .ok_or_else(|| "group_order=file-age oracle returned no winner".to_string())?;
    let actual_best_age_group = first_group_key(&best_age)?;
    if actual_best_age_group != best_age_group {
        return Err(format!(
            "group_order=file-age winner mismatch: expected {best_age_group}, got {actual_best_age_group}; payload={best_age}"
        ));
    }

    eprintln!("[fs-meta-api-matrix] substep=query-stats");
    let stats = session.stats(&[("path", "/".to_string()), ("recursive", "true".to_string())])?;
    let expected_stats = FsTreeOracle::stats_response(&all_mounts, "/", true, None)?;
    assert_json_eq("nfs1 stats", &stats, &expected_stats)?;
    assert_stats_latest_file_mtime_present(&stats)?;

    let file_path = "/nested/child/deep.txt";
    eprintln!("[fs-meta-api-matrix] substep=query-file-tree");
    let file_tree = session.tree(&[
        ("path", file_path.to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let expected_file_tree = FsTreeOracle::grouped_tree_response(
        &all_mounts,
        file_path,
        true,
        None,
        1_000,
        "group-key",
        64,
    )?;
    assert_json_eq("file-path tree", &file_tree, &expected_file_tree)?;
    let file_tree_b64 = session.tree(&[
        ("path_b64", B64URL.encode(file_path.as_bytes())),
        ("recursive", "true".to_string()),
    ])?;
    assert_json_eq(
        "file-path tree via path_b64",
        &file_tree_b64,
        &expected_file_tree,
    )?;
    assert_error(
        session.client().tree_raw(
            session.query_api_key(),
            &[
                ("path", file_path.to_string()),
                ("path_b64", B64URL.encode(file_path.as_bytes())),
            ],
        )?,
        400,
        "path and path_b64 are mutually exclusive",
    )?;

    let file_find = session.force_find(&[
        ("path", file_path.to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let expected_file_find = FsTreeOracle::grouped_force_find_response(
        &all_mounts,
        file_path,
        true,
        None,
        1_000,
        "group-key",
        64,
    )?;
    assert_json_eq("file-path force-find", &file_find, &expected_file_find)?;

    let file_stats = session.stats(&[
        ("path", file_path.to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let expected_file_stats = FsTreeOracle::stats_response(&all_mounts, file_path, true, None)?;
    assert_json_eq("file-path stats", &file_stats, &expected_file_stats)?;
    assert_stats_latest_file_mtime_present(&file_stats)?;

    let invalid_path_tree = session.tree(&[
        ("path", "/missing-dir".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let expected_invalid_path_tree = FsTreeOracle::grouped_tree_response(
        &all_mounts,
        "/missing-dir",
        true,
        None,
        1_000,
        "group-key",
        64,
    )?;
    assert_json_eq(
        "invalid path tree",
        &invalid_path_tree,
        &expected_invalid_path_tree,
    )?;

    let invalid_path_stats = session.stats(&[
        ("path", "/missing-dir".to_string()),
        ("recursive", "true".to_string()),
    ])?;
    let expected_invalid_path_stats =
        FsTreeOracle::stats_response(&all_mounts, "/missing-dir", true, None)?;
    assert_json_eq(
        "invalid path stats",
        &invalid_path_stats,
        &expected_invalid_path_stats,
    )?;
    assert_stats_latest_file_mtime_present(&invalid_path_stats)?;

    let entry_one_tree = session.tree(&[
        ("path", "/".to_string()),
        ("recursive", "true".to_string()),
        ("entry_page_size", "1".to_string()),
    ])?;
    let expected_entry_one =
        FsTreeOracle::grouped_tree_response(&all_mounts, "/", true, None, 1, "group-key", 64)?;
    assert_json_eq(
        "entry_page_size=1 tree",
        &entry_one_tree,
        &expected_entry_one,
    )?;

    let entry_max_tree = session.tree(&[
        ("path", "/".to_string()),
        ("recursive", "true".to_string()),
        ("entry_page_size", "10000".to_string()),
    ])?;
    let expected_entry_max =
        FsTreeOracle::grouped_tree_response(&all_mounts, "/", true, None, 10_000, "group-key", 64)?;
    assert_json_eq(
        "entry_page_size=max tree",
        &entry_max_tree,
        &expected_entry_max,
    )?;

    assert_error(
        session.client().tree_raw(
            session.query_api_key(),
            &[
                ("path", "/".to_string()),
                ("entry_page_size", "10001".to_string()),
            ],
        )?,
        400,
        "entry_page_size",
    )?;
    let tree_paged = session.tree(&[
        ("path", "/".to_string()),
        ("recursive", "true".to_string()),
        ("group_page_size", "1".to_string()),
        ("entry_page_size", "1".to_string()),
    ])?;
    let tree_pit_id = pit_id(&tree_paged)?;
    let tree_next_group = group_page_next_cursor(&tree_paged)?;
    let tree_next_entry_after = group_page_next_entry_after(&tree_paged)?;
    let tree_first_group = first_group_key(&tree_paged)?;
    let expected_group_sequence = group_keys(&expected_all_tree)?;
    if expected_group_sequence.len() < 2 {
        return Err(format!(
            "expected oracle to expose at least two groups for PIT pagination: {expected_all_tree}"
        ));
    }
    if tree_first_group != expected_group_sequence[0] {
        return Err(format!(
            "tree PIT first page group mismatch: expected {}, got {}; payload={tree_paged}",
            expected_group_sequence[0], tree_first_group
        ));
    }
    let expected_tree_first_entries = entry_paths_for_group(&expected_all_tree, &tree_first_group)?;
    if expected_tree_first_entries.len() < 2 {
        return Err(format!(
            "expected oracle first group to have at least two entries for PIT entry pagination: {expected_all_tree}"
        ));
    }
    let tree_entry_page_one = first_entry_path_for_group(&tree_paged, &tree_first_group)?;
    if tree_entry_page_one != expected_tree_first_entries[0] {
        return Err(format!(
            "tree PIT first entry mismatch: expected {}, got {}; payload={tree_paged}",
            expected_tree_first_entries[0], tree_entry_page_one
        ));
    }
    let tree_group_page_two = session.tree(&[
        ("path", "/".to_string()),
        ("recursive", "true".to_string()),
        ("group_page_size", "1".to_string()),
        ("entry_page_size", "1".to_string()),
        ("pit_id", tree_pit_id.clone()),
        ("group_after", tree_next_group.clone()),
    ])?;
    let tree_second_group = first_group_key(&tree_group_page_two)?;
    if tree_second_group != expected_group_sequence[1] {
        return Err(format!(
            "tree PIT second page group mismatch: expected {}, got {}; payload={tree_group_page_two}",
            expected_group_sequence[1], tree_second_group
        ));
    }
    let tree_entry_page_two = session.tree(&[
        ("path", "/".to_string()),
        ("recursive", "true".to_string()),
        ("group_page_size", "1".to_string()),
        ("entry_page_size", "1".to_string()),
        ("pit_id", tree_pit_id.clone()),
        ("entry_after", tree_next_entry_after.clone()),
    ])?;
    let tree_entry_page_two_first =
        first_entry_path_for_group(&tree_entry_page_two, &tree_first_group)?;
    if tree_entry_page_two_first != expected_tree_first_entries[1] {
        return Err(format!(
            "tree PIT entry continuation mismatch: expected {}, got {}; payload={tree_entry_page_two}",
            expected_tree_first_entries[1], tree_entry_page_two_first
        ));
    }
    assert_error(
        session.client().tree_raw(
            session.query_api_key(),
            &[
                ("path", "/".to_string()),
                ("group_after", tree_next_group.clone()),
            ],
        )?,
        400,
        "pit_id is required when using group_after or entry_after",
    )?;
    assert_error(
        session.client().tree_raw(
            session.query_api_key(),
            &[
                ("path", "/".to_string()),
                ("entry_after", tree_next_entry_after.clone()),
            ],
        )?,
        400,
        "pit_id is required when using group_after or entry_after",
    )?;

    let force_find_paged = session.force_find(&[
        ("path", "/".to_string()),
        ("recursive", "true".to_string()),
        ("group_page_size", "1".to_string()),
        ("entry_page_size", "1".to_string()),
    ])?;
    let force_find_pit_id = pit_id(&force_find_paged)?;
    let force_find_next_group = group_page_next_cursor(&force_find_paged)?;
    let force_find_next_entry_after = group_page_next_entry_after_optional(&force_find_paged)?;
    let force_find_first_group = first_group_key(&force_find_paged)?;
    if force_find_first_group != expected_group_sequence[0] {
        return Err(format!(
            "force-find PIT first page group mismatch: expected {}, got {}; payload={force_find_paged}",
            expected_group_sequence[0], force_find_first_group
        ));
    }
    let force_find_group_page_two = session.force_find(&[
        ("path", "/".to_string()),
        ("recursive", "true".to_string()),
        ("group_page_size", "1".to_string()),
        ("entry_page_size", "1".to_string()),
        ("pit_id", force_find_pit_id.clone()),
        ("group_after", force_find_next_group.clone()),
    ])?;
    let force_find_second_group = first_group_key(&force_find_group_page_two)?;
    if force_find_second_group != expected_group_sequence[1] {
        return Err(format!(
            "force-find PIT second page group mismatch: expected {}, got {}; payload={force_find_group_page_two}",
            expected_group_sequence[1], force_find_second_group
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
    assert_error(
        session.client().force_find_raw(
            session.query_api_key(),
            &[
                ("path", "/".to_string()),
                ("recursive", "true".to_string()),
                ("read_class", "materialized".to_string()),
            ],
        )?,
        400,
        "read_class must be fresh on /on-demand-force-find",
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
    if let Some(force_find_next_entry_after) = force_find_next_entry_after {
        assert_error(
            session.client().force_find_raw(
                session.query_api_key(),
                &[
                    ("path", "/".to_string()),
                    ("entry_after", force_find_next_entry_after.clone()),
                ],
            )?,
            400,
            "pit_id is required when using group_after or entry_after",
        )?;
    }
    assert_error(
        session.client().tree_raw(
            session.query_api_key(),
            &[
                ("path", "/".to_string()),
                ("recursive", "true".to_string()),
                ("pit_id", tree_pit_id.clone()),
                ("group_after", force_find_next_group.clone()),
            ],
        )?,
        400,
        "group_after cursor does not match the requested tree scope",
    )?;
    assert_error(
        session.client().force_find_raw(
            session.query_api_key(),
            &[
                ("path", "/".to_string()),
                ("recursive", "true".to_string()),
                ("pit_id", force_find_pit_id.clone()),
                ("entry_after", tree_next_entry_after.clone()),
            ],
        )?,
        400,
        "entry_after cursor does not match the requested force-find scope",
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
            if sink.get("live_nodes").and_then(Value::as_u64).unwrap_or(0) == 0 {
                return Ok(false);
            }
            let Some(groups) = sink.get("groups").and_then(Value::as_array) else {
                return Ok(!require_sink_groups);
            };
            if groups.is_empty() && require_sink_groups {
                return Ok(false);
            }
            Ok(groups
                .iter()
                .all(|group| group.get("shadow_lag_us").is_some_and(Value::is_number)))
        },
    )?;
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
    if sink.get("live_nodes").and_then(Value::as_u64).unwrap_or(0) == 0 {
        return Err(format!("expected live sink nodes in status: {status}"));
    }
    if let Some(sink_groups) = sink.get("groups").and_then(Value::as_array) {
        if require_sink_groups && sink_groups.is_empty() {
            return Err(format!(
                "expected non-empty sink.groups in status: {status}"
            ));
        }
        for group in sink_groups {
            if !group.get("shadow_lag_us").is_some_and(Value::is_number) {
                return Err(format!(
                    "status sink group missing numeric shadow_lag_us: {group}"
                ));
            }
        }
    } else if require_sink_groups {
        return Err(format!("status missing sink.groups array: {status}"));
    }
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

fn baseline_cluster_diagnostics(
    cluster: &Cluster5,
    session: &mut OperatorSession,
) -> Result<String, String> {
    let status = session
        .status()
        .unwrap_or_else(|err| json!({ "error": err }));
    let grants = session
        .runtime_grants()
        .unwrap_or_else(|err| json!({ "error": err }));
    let roots = session
        .monitoring_roots()
        .unwrap_or_else(|err| json!({ "error": err }));
    let metrics = session
        .bound_route_metrics()
        .unwrap_or_else(|err| json!({ "error": err }));
    let mut cluster_nodes = Vec::new();
    for node_name in ["node-a", "node-b", "node-c", "node-d", "node-e"] {
        let status_value = cluster
            .status(node_name)
            .unwrap_or_else(|err| json!({ "error": err }));
        let cluster_status = cluster
            .cluster_status(node_name)
            .unwrap_or_else(|err| json!({ "error": err }));
        let process_status = cluster
            .node_process_status(node_name)
            .unwrap_or_else(|err| json!({ "error": err }));
        let logs = cluster
            .node_log_excerpt(node_name)
            .unwrap_or_else(|err| json!({ "error": err }));
        let target_state = cluster
            .runtime_target_state(node_name)
            .unwrap_or_else(|err| json!({ "error": err }));
        cluster_nodes.push(json!({
            "node": node_name,
            "status": status_value,
            "cluster_status": cluster_status,
            "process_status": process_status,
            "logs": logs,
            "runtime_target_state": target_state,
        }));
    }
    Ok(json!({
        "session_status": status,
        "runtime_grants": grants,
        "monitoring_roots": roots,
        "bound_route_metrics": metrics,
        "nodes": cluster_nodes,
    })
    .to_string())
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

fn baseline_roots(lab: &NfsLab) -> Vec<RootSpec> {
    vec![
        root_spec("nfs1", &lab.export_source("nfs1")),
        root_spec("nfs2", &lab.export_source("nfs2")),
        root_spec("nfs3", &lab.export_source("nfs3")),
    ]
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

fn assert_stats_latest_file_mtime_present(stats: &Value) -> Result<(), String> {
    let groups = stats
        .get("groups")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("stats response missing groups object: {stats}"))?;
    if groups.is_empty() {
        return Err(format!(
            "stats response should expose at least one group: {stats}"
        ));
    }
    for (group, envelope) in groups {
        let Some(status) = envelope.get("status").and_then(Value::as_str) else {
            return Err(format!(
                "stats group missing status for {group}: {envelope}"
            ));
        };
        if status != "ok" {
            continue;
        }
        let latest = envelope
            .get("data")
            .and_then(|data| data.get("latest_file_mtime_us"))
            .ok_or_else(|| {
                format!("stats group missing data.latest_file_mtime_us for {group}: {envelope}")
            })?;
        if !(latest.is_null() || latest.is_u64()) {
            return Err(format!(
                "stats group latest_file_mtime_us must be u64|null for {group}: {envelope}"
            ));
        }
    }
    Ok(())
}

fn first_group_key(payload: &Value) -> Result<String, String> {
    payload
        .get("groups")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("group"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("response missing group keys: {payload}"))
}

fn group_keys(payload: &Value) -> Result<Vec<String>, String> {
    payload
        .get("groups")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("response missing groups array: {payload}"))?
        .iter()
        .map(|row| {
            row.get("group")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .ok_or_else(|| format!("response group row missing key: {payload}"))
        })
        .collect()
}

fn pit_id(payload: &Value) -> Result<String, String> {
    payload
        .get("pit")
        .and_then(|pit| pit.get("id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("response missing pit.id: {payload}"))
}

fn group_page_next_cursor(payload: &Value) -> Result<String, String> {
    payload
        .get("group_page")
        .and_then(|page| page.get("next_cursor"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("response missing group_page.next_cursor: {payload}"))
}

fn group_page_next_entry_after(payload: &Value) -> Result<String, String> {
    group_page_next_entry_after_optional(payload)?
        .ok_or_else(|| format!("response missing group_page.next_entry_after: {payload}"))
}

fn group_page_next_entry_after_optional(payload: &Value) -> Result<Option<String>, String> {
    payload
        .get("group_page")
        .and_then(|page| page.get("next_entry_after"))
        .map(|value| match value {
            Value::Null => Ok(None),
            Value::String(value) => Ok(Some(value.clone())),
            other => Err(format!(
                "response contains invalid group_page.next_entry_after type: {other}"
            )),
        })
        .unwrap_or_else(|| {
            Err(format!(
                "response missing group_page.next_entry_after: {payload}"
            ))
        })
}

fn entry_paths_for_group(payload: &Value, group_key: &str) -> Result<Vec<String>, String> {
    let group = payload
        .get("groups")
        .and_then(Value::as_array)
        .and_then(|groups| {
            groups
                .iter()
                .find(|group| group.get("group").and_then(Value::as_str) == Some(group_key))
        })
        .ok_or_else(|| format!("response missing group '{group_key}': {payload}"))?;
    group
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("group '{group_key}' missing entries array: {group}"))?
        .iter()
        .map(|entry| {
            entry
                .get("path")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .ok_or_else(|| format!("group '{group_key}' entry missing path: {entry}"))
        })
        .collect()
}

fn first_entry_path_for_group(payload: &Value, group_key: &str) -> Result<String, String> {
    entry_paths_for_group(payload, group_key)?
        .into_iter()
        .next()
        .ok_or_else(|| format!("group '{group_key}' has no entries: {payload}"))
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
    if path.exists() {
        return path.to_path_buf();
    }
    path.ancestors()
        .skip(1)
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

fn group_mount_pairs_for_roots(
    grants: &Value,
    roots: &[(&str, &str)],
) -> Result<Vec<(String, PathBuf)>, String> {
    let mount_map = mounts_by_fs_source(grants)?;
    roots
        .iter()
        .map(|(group_id, fs_source)| {
            mount_map
                .get(*fs_source)
                .and_then(|paths| representative_mount_for_group(group_id, paths))
                .map(|path| (group_id.to_string(), path))
                .ok_or_else(|| format!("missing representative mount for fs_source {fs_source}"))
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
                                | "WatchOverflowPendingAudit"
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
